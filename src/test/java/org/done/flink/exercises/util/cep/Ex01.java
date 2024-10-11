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
 * Exercise 01: Device Detection
 */
public class Ex01 {
    private final static String deviceId1 = "someId";
    private final static String deviceId2 = "someOtherId";
    private final static Duration timeBetweenMessages = Duration.ofSeconds(1);
    // 2024-01-01T00:00:00.000
    private final static Instant beginRoot = Instant.ofEpochSecond(1704067200);

    public static List<AirQSensorData> dataPart1() {
        final var begin = beginRoot;

        return Arrays.asList(
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin).setWarmingUp(1000, 10).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages)).setWarmingUp(1000, 9).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(2))).setWarmingUp(1000, 8).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(3))).setWarmingUp(1000, 7).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(4))).setWarmingUp(1000, 5).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(5))).setWarmingUp(1000, 4).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(6))).setWarmingUp(1000, 3).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(7))).setWarmingUp(1000, 2).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(8))).setWarmingUp(1000, 1).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(9))).setWarmingUp(1000, 0).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(10))).setWarmingUp(1000, 0).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(11))).setOk().build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(12))).setOk().build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(13))).setOk().build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(14))).setOk().build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(15))).setOk().build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(16))).setOk().build()
        );
    }

    public static List<Event> getExpectedOutputPart1() {
        return List.of(
                new Event(deviceId1, EventType.NEW_DEVICE, beginRoot.plus(timeBetweenMessages.multipliedBy(11)))
        );
    }

    public static List<AirQSensorData> dataPart2() {
        final var begin = beginRoot.plus(Duration.ofDays(1));

        final var data = Arrays.asList(
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin).setWarmingUp(1000, 10).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages)).setWarmingUp(1000, 9).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(2))).setWarmingUp(1000, 8).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(3))).setOk().build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(4))).setWarmingUp(1000, 5).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(5))).setOk().build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(6))).setWarmingUp(1000, 3).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(7))).setOk().build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(8))).setWarmingUp(1000, 1).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(9))).setOk().build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(10))).setWarmingUp(1000, 0).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(11))).setOk().build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(12))).setOk().build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(13))).setOk().build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId1).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(14))).setOk().build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId2).setDeviceTimestamp(begin).setWarmingUp(1000, 10).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId2).setDeviceTimestamp(begin.plus(timeBetweenMessages)).setWarmingUp(1000, 9).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId2).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(2))).setWarmingUp(1000, 8).build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId2).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(3))).setOk().build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId2).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(4))).setOk().build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId2).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(5))).setOk().build(),
                new AirQSensorDataSampleBuilder().setDeviceId(deviceId2).setDeviceTimestamp(begin.plus(timeBetweenMessages.multipliedBy(6))).setOk().build()
        );
        data.sort((a, b) -> (int) (a.deviceTimestamp.toEpochMilli() - b.deviceTimestamp.toEpochMilli()));

        return data;
    }

    public static List<Event> getExpectedOutputPart2() {
        final var begin = beginRoot.plus(Duration.ofDays(1));

        return List.of(
                new Event(deviceId2, EventType.NEW_DEVICE, begin.plus(timeBetweenMessages.multipliedBy(5))),
                new Event(deviceId1, EventType.NEW_DEVICE, begin.plus(timeBetweenMessages.multipliedBy(13)))
        );
    }

    public static List<AirQSensorData> dataPart3() {
        return dataPart2();

    }

    public static List<Event> getExpectedOutputPart3() {
        final var begin = beginRoot.plus(Duration.ofDays(1));

        return List.of(
                new Event(deviceId1, EventType.DEVICE_MISBEHAVING, begin.plus(timeBetweenMessages.multipliedBy(4))),
                new Event(deviceId1, EventType.DEVICE_MISBEHAVING, begin.plus(timeBetweenMessages.multipliedBy(6))),
                new Event(deviceId1, EventType.DEVICE_MISBEHAVING, begin.plus(timeBetweenMessages.multipliedBy(8))),
                new Event(deviceId1, EventType.DEVICE_MISBEHAVING, begin.plus(timeBetweenMessages.multipliedBy(10)))
        );
    }
}
