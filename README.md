# Kafka Failover CLI

Command line tool for changing `min.insync.replicas` for a set of topics.

    Usage: kafka-failover [-hV] -b=<bootstrapServers> -c=<currentIsr>
                          -f=<futureIsr> [-t=<topicsMask>]
    Modifies the replication characteristics of a set of topics to enable failover
    in a Kafka cluster
      -b=<bootstrapServers>    Bootstrap servers
      -c, --current-isr=<currentIsr>
                               Current min.insync.replicas
      -f, --future-isr=<futureIsr>
                               Future min.insync.replicas
      -h, --help               Show this help message and exit.
      -t, --topics=<topicsMask>
                               Regex of topic names to match against
      -V, --version            Print version information and exit.

Sample usage:

    java -jar target/kafka-failover-cli-jar-with-dependencies.jar -b=localhost:9092 -c 2 -f 1 -t test-topic.*
