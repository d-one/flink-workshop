package org.done.flink.exercises.support;

import org.done.flink.exercises.data.airq.AirQSensorDataSampleBuilder;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class AirQSensorDataSampleBuilderTest {
    @Test
    public void testOk() {
        final var built = new AirQSensorDataSampleBuilder().setOk().build();
        assertEquals("\"OK\"", built.status);
        assertTrue(built.isOk());
    }

    @Test
    public void test02() {
        final var ok = new AirQSensorDataSampleBuilder().setOk().build();
        final var warmingUp = new AirQSensorDataSampleBuilder().setWarmingUp().build();

        assertNotEquals(ok, warmingUp);
        assertTrue(ok.isOk());
        assertEquals(ok.deviceId, warmingUp.deviceId);
        assertTrue(warmingUp.status.contains("warm up"));
        assertTrue(warmingUp.isWarmingUp());
    }

    @Test
    public void test03() {
        final var data = new AirQSensorDataSampleBuilder().setOk()
                .setDeviceTimestamp(Instant.ofEpochMilli(10))
                .setCo2(42.0)
                .build();

        assertTrue(data.isOk());
        assertEquals(Instant.ofEpochMilli(10), data.deviceTimestamp);
        assertEquals(42.0, data.co2);
    }

    @Test
    public void test04() {
        final var data = new AirQSensorDataSampleBuilder()
                .setWarmingUp(10, 42)
                .build();

        assertTrue(data.isWarmingUp());
        assertTrue(data.status.contains("warm up"));
    }
}