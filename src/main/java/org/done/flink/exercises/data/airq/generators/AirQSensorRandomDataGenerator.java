package org.done.flink.exercises.data.airq.generators;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.done.flink.exercises.data.airq.AirQSensorData;

import java.time.Instant;

/**
 * A random AirQSensorData generator.
 * Adjusts the temperature based on a Gaussian distribution.
 */
public class AirQSensorRandomDataGenerator implements DataGenerator<AirQSensorData> {
    protected transient RandomDataGenerator random;
    protected transient AirQSensorData baseData;
    protected transient String deviceId;
    protected transient Instant lastEmit;

    @Override
    public void open(String s, FunctionInitializationContext functionInitializationContext, RuntimeContext runtimeContext) throws Exception {
        this.random = new RandomDataGenerator();

        this.baseData = AirQSensorData.sample();
        this.deviceId = this.random.nextHexString(this.baseData.deviceId.length());
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public AirQSensorData next() {
        this.lastEmit = Instant.now();

        AirQSensorData newRecord = new AirQSensorData(this.baseData);

        newRecord.deviceTimestamp = this.lastEmit;
        newRecord.temperature = this.baseData.temperature + this.random.nextGaussian(0, 5);
        this.baseData.temperature = newRecord.temperature;

        return newRecord;
    }
}
