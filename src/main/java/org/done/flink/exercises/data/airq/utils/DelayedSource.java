package org.done.flink.exercises.data.airq.utils;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.done.flink.exercises.data.airq.AirQSensorData;

import java.util.List;

// Create a custom delayed source
public class DelayedSource implements SourceFunction<AirQSensorData> {
    private final List<AirQSensorData> data;
    private volatile boolean isRunning = true;

    public DelayedSource(List<AirQSensorData> data) {
        this.data = data;
    }

    @Override
    public void run(SourceContext<AirQSensorData> ctx) throws InterruptedException {
        long lastTimestamp = -1; // Initialize with an invalid timestamp

        for (AirQSensorData sensorData : data) {
            long currentEventTimestamp = sensorData.deviceTimestamp.toEpochMilli();

            if (lastTimestamp != -1) { // If this is not the first event
                long delay = currentEventTimestamp - lastTimestamp; // Calculate the delay
                if (delay > 0) {
                    Thread.sleep(delay); // Wait for the calculated delay
                }
            }

            ctx.collect(sensorData); // Emit the data
            lastTimestamp = currentEventTimestamp; // Update the last timestamp
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
