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
import java.util.stream.Collectors;

@CommandLine.Command(name="kafka-failover", mixinStandardHelpOptions = true, version = "1.0",
	description = "Modifies the replication characteristics of a set of topics to enable failover in a Kafka cluster")
class KafkaFailover implements Callable<Integer> {

	public static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";
	private Logger log = LoggerFactory.getLogger(this.getClass());

	@CommandLine.Option(names = {"-b"}, description = "Bootstrap servers", required = true)
	private String bootstrapServers;

	@CommandLine.Option(names = {"-c", "--current-isr"}, description = "Current min.insync.replicas", required = true)
	private Integer currentIsr;

	@CommandLine.Option(names = {"-f", "--future-isr"}, description = "Future min.insync.replicas", required = true)
	private Integer futureIsr;

	public static int main(String[] args) {
		return new CommandLine(new KafkaFailover()).execute(args);
	}

	@Override
	public Integer call() throws Exception {
		var adminClient = KafkaAdminClient.create(
				Collections.singletonMap("bootstrap.servers", bootstrapServers));
		log.info("Listing topics");
		var listTopicsResult = adminClient.listTopics();
		listTopicsResult.listings().whenComplete((topicListings, throwable) -> {
			var externalTopics = topicListings.stream()
					.filter(topicListing -> !topicListing.isInternal())
					.map(TopicListing::name)
					.collect(Collectors.toList());

			var topicConfigResources = externalTopics.stream()
					.map(topicName -> new ConfigResource(ConfigResource.Type.TOPIC, topicName))
					.collect(Collectors.toList());

			log.info("Describing topic configs");
			var describeConfigs = adminClient.describeConfigs(topicConfigResources).all();
			describeConfigs.whenComplete((configResourceConfigMap, throwable1) -> {
				List<ConfigResource> topicsMatchingMinIsr = new ArrayList<>();

				configResourceConfigMap.
						forEach((configResource, config) -> {
							var configEntry = config.get(MIN_INSYNC_REPLICAS);
							if (configEntry.value().equals(currentIsr.toString())) {
								log.info("Topic {} matched; {}}={}", configResource.name(), MIN_INSYNC_REPLICAS, currentIsr);
								topicsMatchingMinIsr.add(configResource);
							} else {
								log.info("Topic {} ignored; {}={}", configResource.name(), MIN_INSYNC_REPLICAS, currentIsr);
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

				adminClient.incrementalAlterConfigs(configs).all().whenComplete((aVoid, throwable2) ->
						log.info("Alter operation complete"));
			});

		});
		return 0;
	}

}
