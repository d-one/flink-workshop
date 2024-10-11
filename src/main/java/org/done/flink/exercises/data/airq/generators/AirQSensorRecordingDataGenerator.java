package org.done.flink.exercises.data.airq.generators;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.done.flink.exercises.data.airq.AirQSensorData;
import org.done.flink.exercises.data.airq.utils.Recordings;

import java.time.Instant;
import java.util.List;

public class AirQSensorRecordingDataGenerator implements DataGenerator<AirQSensorData> {
    protected String recordingName;
    protected String deviceId;
    protected transient List<AirQSensorData> recordings;
    protected transient int numberEmitted = 0;

    public AirQSensorRecordingDataGenerator(String recordingName) {
        this.recordingName = recordingName;
        this.deviceId = RandomStringUtils.randomAlphanumeric(10);
    }

    @Override
    public void open(String s, FunctionInitializationContext functionInitializationContext, RuntimeContext runtimeContext) throws Exception {
        this.recordings = Recordings.readRecording(this.recordingName);
    }

    @Override
    public boolean hasNext() {
        return this.numberEmitted < this.recordings.size();
    }

    @Override
    public AirQSensorData next() {
        var dataPoint = this.recordings.get(this.numberEmitted++);
        dataPoint.deviceTimestamp = Instant.now();

        return dataPoint;
    }
}
