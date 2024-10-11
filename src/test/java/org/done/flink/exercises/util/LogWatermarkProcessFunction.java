package org.done.flink.exercises.util;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.done.flink.exercises.data.airq.AirQSensorData;

public class LogWatermarkProcessFunction extends ProcessFunction<AirQSensorData, AirQSensorData> {

    @Override
    public void processElement(AirQSensorData value, Context ctx, Collector<AirQSensorData> out) throws Exception {
        // Log the current watermark
        long watermark = ctx.timerService().currentWatermark();
        System.out.println("Current Watermark: " + watermark);

        // Emit the element downstream
        out.collect(value);
    }
}
