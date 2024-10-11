package org.done.flink.exercises.support;

import org.done.flink.exercises.data.airq.AirQSensorData;
import org.done.flink.exercises.data.airq.AirQSensorDataSampleBuilder;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class AirQSensorDataTest {
    @Test
    void loadFromJson() {
        final String jsonString = "{\"sound_max\":[80.80001,1.8], \"humidity_abs\":[7.607,0.42],\n" +
                " \"health\":1000,\n" +
                " \"sound\":[34.28,9.8],\n" +
                " \"timestamp\":1711542740000,\n" +
                " \"DeviceID\":\"339966b51f42510ca487d3f9609483b4\",\n" +
                " \"Status\":{\n" +
                " \"co\":\"co sensor still in warm up phase; waiting time = 1646 s\",\n" +
                " \"tvoc\":\"tvoc sensor still in warm up phase; waiting time = 86 s\"},\n" +
                " \"humidity\":[50.872,3.48],\n" +
                " \"performance\":757,\n" +
                " \"dewpt\":[7.378,0.81],\n" +
                " \"pressure\":[935.2,1.0],\n" +
                " \"temperature\":[17.485,0.55],\n" +
                " \"co2\":[712.8,71.4],\n" +
                " \"tvoc\":null,\n" +
                " \"co\":null}";

        final AirQSensorData data = AirQSensorData.fromJsonString(jsonString);

        assertNull(data.co);
        assertEquals(712.8, data.co2);
        assertEquals(9.8, data.soundError);
        assertNotNull(data.deviceId);
        assertEquals("339966b51f42510ca487d3f9609483b4", data.deviceId);
    }

    @Test
    void okSample() {
        final AirQSensorData data = new AirQSensorDataSampleBuilder().setOk().build();

        assertNotNull(data.co);
        assertNotNull(data.deviceId);
        assertEquals("\"OK\"", data.status);
        assertTrue(data.isOk());
    }

    @Test
    void stupidTimestampTest() {
        final var i = Instant.ofEpochMilli(1725541231000L);

        assertEquals("2024-09-05T13:00:31Z", i.toString());
    }
}