package io.confluent.ps.kafkafailovercli;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaFailoverCliApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaFailoverCliApplication.class, args);
	}

}
