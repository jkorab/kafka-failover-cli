FROM openjdk:11
WORKDIR /
ADD target/kafka-failover-cli-jar-with-dependencies.jar kafka-failover-cli.jar
