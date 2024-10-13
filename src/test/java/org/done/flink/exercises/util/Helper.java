package org.done.flink.exercises.util;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.done.flink.exercises.data.airq.AirQSensorData;
import org.done.flink.exercises.data.events.Event;

import java.util.List;

public class Helper {
    public static String co2ToString(final AirQSensorData sensorData) {
        return "Device ID: " + sensorData.deviceId + ", Timestamp: " + sensorData.deviceTimestamp + ", CO2: " + sensorData.co2;
    }

    public static String temperatureAndHumidityToString(final AirQSensorData sensorData) {
        return "Device ID: " + sensorData.deviceId + ", Timestamp: " + sensorData.deviceTimestamp + ", Temperature: " + sensorData.temperature + ", Humidity: " + sensorData.humidity;
    }

    public static void printCO2Input(final KeyedStream<AirQSensorData, String> inputStream) throws Exception {
        inputStream.map(Helper::co2ToString)
                .executeAndCollect()
                .forEachRemaining(System.out::println);
    }

    public static void printTemperatureAndHumidity(final KeyedStream<AirQSensorData, String> inputStream) throws Exception {
        inputStream.map(Helper::temperatureAndHumidityToString)
                .executeAndCollect()
                .forEachRemaining(System.out::println);
    }

    public static void printCO2Input(final DataStream<AirQSensorData> inputStream) throws Exception {
        inputStream.map(Helper::co2ToString)
                .executeAndCollect()
                .forEachRemaining(System.out::println);
    }

    public static void printEvents(final List<Event> events) {
        events.forEach(System.out::println);
    }
}
