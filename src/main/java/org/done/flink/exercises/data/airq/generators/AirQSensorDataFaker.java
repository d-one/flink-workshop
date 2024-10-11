package org.done.flink.exercises.data.airq.generators;


import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.done.flink.exercises.data.airq.AirQSensorData;

public class AirQSensorDataFaker {
    // How many records to publish per second
    private short publishFrequency = 1;
    // Will basically make it an unbounded, never ending stream.
    // But... it can't be a Long, because the underlying DataGeneratorSource class there is some shitty type conversion from long to int...
    private long duration = Integer.MAX_VALUE;

    public AirQSensorDataFaker(short publishFrequency, long duration) {
        this.publishFrequency = publishFrequency;
        this.duration = duration;
    }

    public AirQSensorDataFaker() {
        this((short) 1, Integer.MAX_VALUE);
    }

    public DataGeneratorSource<AirQSensorData> getRandomDataGeneratorSource() {
        // Not the same as in the documentation on the internet
        return new DataGeneratorSource<AirQSensorData>(
                new AirQSensorRandomDataGenerator(),
                this.publishFrequency,
                this.duration);
    }

    public DataGeneratorSource<AirQSensorData> getRecordingDataGeneratorSource(final String recordingName) {
        return new DataGeneratorSource<AirQSensorData>(
                new AirQSensorRecordingDataGenerator(recordingName),
                this.publishFrequency,
                this.duration);
    }

    public DataGeneratorSource<AirQSensorData> getRecordingDataGeneratorSource() {
        return getRecordingDataGeneratorSource("single_room_recording.json");
    }

}
