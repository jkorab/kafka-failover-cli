package io.confluent.ps.kafkafailovercli;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Failover test case")
class KafkaFailoverTest {

    @Test
    void testCliWorks() {
        assertNotEquals(0, KafkaFailover.execute(new String[0]));
    }
}