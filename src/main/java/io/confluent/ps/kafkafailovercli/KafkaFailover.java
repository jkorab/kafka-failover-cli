package io.confluent.ps.kafkafailovercli;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Command line tool for changing min.insync.replicas for a set of topics.
 *
 * @author jkorab
 */
@CommandLine.Command(name = "kafka-failover", mixinStandardHelpOptions = true, version = "1.0",
        description = "Modifies the replication characteristics of a set of topics to enable failover in a Kafka cluster")
public class KafkaFailover implements Callable<Integer> {

    private static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";
    private Logger log = LoggerFactory.getLogger(this.getClass());

    @CommandLine.Option(names = {"-b"}, description = "Bootstrap servers", required = true)
    private String bootstrapServers;

    @CommandLine.Option(names = {"-c", "--current-isr"}, description = "Current min.insync.replicas", required = true)
    private Integer currentIsr;

    @CommandLine.Option(names = {"-f", "--future-isr"}, description = "Future min.insync.replicas", required = true)
    private Integer futureIsr;

    @CommandLine.Option(names = {"-t", "--topics"}, description = "Regex of topic names to match against")
    private String topicsMask;

    public static void main(String[] args) {
        System.exit(execute(args));
    }

    static int execute(String[] args) {
        return new CommandLine(new KafkaFailover()).execute(args);
    }

    @Override
    public Integer call() throws ExecutionException, InterruptedException {
        var adminClient = KafkaAdminClient.create(
                Collections.singletonMap("bootstrap.servers", bootstrapServers));
        log.info("Listing topics");
        var listTopicsResult = adminClient.listTopics();
        var topicListings = listTopicsResult.listings().get();
        var externalTopics = topicListings.stream()
                .filter(topicListing -> !topicListing.isInternal())
                .map(TopicListing::name)
                .collect(Collectors.toList());

        var topicConfigResources = externalTopics.stream()
                .map(topicName -> new ConfigResource(ConfigResource.Type.TOPIC, topicName))
                .collect(Collectors.toList());

        log.info("Describing topic configs");
        var configResourceConfigMap = adminClient.describeConfigs(topicConfigResources).all().get();
        List<ConfigResource> topicsMatchingMinIsr = new ArrayList<>();

        Pattern pattern = (topicsMask == null) ? null : Pattern.compile(topicsMask);

        configResourceConfigMap.forEach((configResource, config) -> {
            var topicName = configResource.name();
            boolean addToMatchingTopic = false;
            if (config.get(MIN_INSYNC_REPLICAS).value().equals(currentIsr.toString())) {
                if (pattern == null) {
                    addToMatchingTopic = true;
                } else {
                    if (pattern.matcher(topicName).matches()) {
                        addToMatchingTopic = true;
                    }
                }
            }

            if (addToMatchingTopic) {
                log.debug("Topic {} matched; {}}={}", topicName, MIN_INSYNC_REPLICAS, currentIsr);
                topicsMatchingMinIsr.add(configResource);
            } else {
                log.debug("Topic {} ignored; {}={}", topicName, MIN_INSYNC_REPLICAS, currentIsr);
            }
        });

        log.info("Updating {}={} for {} topics", MIN_INSYNC_REPLICAS, futureIsr, topicsMatchingMinIsr.size());

        var alterConfigOp = new AlterConfigOp(
                new ConfigEntry(MIN_INSYNC_REPLICAS, futureIsr.toString()),
                AlterConfigOp.OpType.SET);
        var alterConfigOps = Collections.singleton(alterConfigOp);

        // populate the options map to be sent to Kafka
        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
        topicsMatchingMinIsr.forEach(topic -> configs.put(topic, alterConfigOps));

        log.info("Performing incremental alter configs");
        adminClient.incrementalAlterConfigs(configs).all().get();
        log.info("Alter operation complete");

        return 0;
    }

}
