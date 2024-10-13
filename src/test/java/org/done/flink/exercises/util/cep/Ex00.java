package org.done.flink.exercises.util.cep;

import org.done.flink.exercises.data.airq.AirQSensorData;
import org.done.flink.exercises.data.airq.AirQSensorDataSampleBuilder;
import org.done.flink.exercises.data.events.Event;
import org.done.flink.exercises.data.events.EventType;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

/**
 * Exercise 00: Device Detection
 */
public class Ex00 {
    private final static String deviceId1 = "someId";
    private final static Duration timeBetweenMessages = Duration.ofSeconds(1);
    // 2024-01-01T00:00:00.000
    private final static Instant beginRoot = Instant.ofEpochSecond(1704067200);

    public static List<AirQSensorData> dataPart1() {
        final var begin = beginRoot;

        return Arrays.asList(
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin).setWarmingUp(1000, 10).setTemperature(22).setHumidity(23).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(1))).setOk().setTemperature(22).setHumidity(23).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(2))).setOk().setTemperature(23).setHumidity(23).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(3))).setOk().setTemperature(24).setHumidity(23).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(4))).setOk().setTemperature(25).setHumidity(23).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(5))).setOk().setTemperature(26).setHumidity(23).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(6))).setOk().setTemperature(27).setHumidity(23).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(7))).setOk().setTemperature(27).setHumidity(23).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(8))).setOk().setTemperature(27).setHumidity(23).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(9))).setOk().setTemperature(27).setHumidity(23).build()
        );
    }

    public static List<Event> getExpectedOutputPart1() {
        return List.of(
                new Event(deviceId1, EventType.OHSA_VIOLATION, beginRoot.plus(timeBetweenMessages.multipliedBy(4)))
        );
    }
}
