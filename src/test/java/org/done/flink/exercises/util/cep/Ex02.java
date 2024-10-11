package org.done.flink.exercises.util.cep;


import org.done.flink.exercises.data.airq.AirQSensorData;
import org.done.flink.exercises.data.airq.AirQSensorDataSampleBuilder;
import org.done.flink.exercises.data.airq.utils.Recordings;
import org.done.flink.exercises.data.events.Event;
import org.done.flink.exercises.data.events.EventType;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

/**
 * Exercise 01: Device Detection
 */
public class Ex02{
    private final static String deviceId1 = "339966b51f42510ca487d3f9609483b4";
    private final static Instant beginRoot = Instant.now();

    public static List<AirQSensorData> data() throws IOException {
        return Recordings.readSmall();
    }

    public static List<Event> getExpectedOutputPart1() {
        return List.of(
            new Event(deviceId1, EventType.CO2_WARNING, Instant.parse("2024-09-05T13:32:41Z")),
            new Event(deviceId1, EventType.CO2_DANGER, Instant.parse("2024-09-05T14:12:50Z"))
        );
    }


    public static List<AirQSensorData> dataPart2() {
        final var begin = beginRoot;

        return Arrays.asList(
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(0)).setOk().setCo2(700.0).build(), // Healthy
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(1)).setOk().setCo2(710.0).build(), // Healthy
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(3)).setOk().setCo2(730.0).build(), // Healthy
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(4)).setOk().setCo2(740.0).build(), // Healthy
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(7)).setOk().setCo2(1600.0).build(), // Dangerous
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(8)).setOk().setCo2(2000.0).build(), // Dangerous
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(12)).setOk().setCo2(1200.0).build(), // Warning
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(13)).setOk().setCo2(850.0).build(), // Healthy
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(14)).setOk().setCo2(860.0).build(), // Healthy
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(15)).setOk().setCo2(870.0).build(), // Healthy
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(16)).setOk().setCo2(885.0).build(), // Warning
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(17)).setOk().setCo2(840.0).build(), // Healthy
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(18)).setOk().setCo2(890.0).build(), // Warning
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(19)).setOk().setCo2(830.0).build(), // Healthy
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(20)).setOk().setCo2(895.0).build(), // Warning
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(23)).setOk().setCo2(1890.0).build(), // Warning
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(24)).setOk().setCo2(900.0).build(), // Warning
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(25)).setOk().setCo2(990.0).build(), // Warning

                // Late data for testing with earlier timestamps
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(21)).setOk().setCo2(890.0).build(), // Late Warning
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(22)).setOk().setCo2(1540.0).build(), // Late Warning
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(2)).setOk().setCo2(1600.0).build(), // Late Dangerous
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(9)).setOk().setCo2(1000.0).build(),  // Late Warning
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(10)).setOk().setCo2(1000.0).build(),  // Late Warning
                new AirQSensorDataSampleBuilder().setDeviceId("deviceId1").setDeviceTimestamp(begin.plusSeconds(11)).setOk().setCo2(1000.0).build()  // Late Warning
        );


    }

    public static List<Event> getExpectedOutputPart2() {

        return List.of(
                new Event("deviceId1", EventType.CO2_DANGER, beginRoot.plusSeconds(7)),
                new Event("deviceId1", EventType.CO2_WARNING, beginRoot.plusSeconds(20)),
                new Event("deviceId1", EventType.CO2_DANGER, beginRoot.plusSeconds(22))
        );
    }

}
