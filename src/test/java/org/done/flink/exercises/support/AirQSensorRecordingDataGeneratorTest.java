package org.done.flink.exercises.support;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.done.flink.exercises.data.airq.AirQSensorData;
import org.done.flink.exercises.data.airq.generators.AirQSensorDataFaker;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AirQSensorRecordingDataGeneratorTest {
    @Test
    void testSetup() throws Exception {
        DataGeneratorSource<AirQSensorData> dataSource = new AirQSensorDataFaker((short) 10, Integer.MAX_VALUE).getRecordingDataGeneratorSource();
        StreamSource<AirQSensorData, DataGeneratorSource<AirQSensorData>> src = new StreamSource<>(dataSource);

        // KeyedOneInputStreamOperatorTestHarness<String, AirQSensorData, AirQSensorData> testHarness = setupHarness();
        // The keyed operator expects some more setup... not sure what right now
        AbstractStreamOperatorTestHarness<AirQSensorData> testHarness = new AbstractStreamOperatorTestHarness<AirQSensorData>(src, 1, 1, 0);
        testHarness.open();

        int totalNumber = 58;
        List<AirQSensorData> results = new ArrayList<>();

        dataSource.run(new SourceFunction.SourceContext<AirQSensorData>() {
            private final Object lock = new Object();
            private int emitNumber = 0;

            @Override
            public void collect(AirQSensorData element) {
                if (++emitNumber > totalNumber) {
                    dataSource.cancel();
                } else {
                    results.add(element);
                }
            }

            @Override
            public void collectWithTimestamp(AirQSensorData airQSensorData, long l) {
            }

            @Override
            public void emitWatermark(Watermark mark) {
            }

            @Override
            public void markAsTemporarilyIdle() {
            }

            @Override
            public Object getCheckpointLock() {
                return lock;
            }

            @Override
            public void close() {
            }
        });

        assertEquals(totalNumber, results.size());

        // TODO do something along https://stackoverflow.com/questions/63851735/apache-flink-calculate-the-difference-of-value-between-two-consecutive-event-w

        testHarness.close();
    }

    private KeyedOneInputStreamOperatorTestHarness<String, AirQSensorData, AirQSensorData> setupHarness() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, AirQSensorData, AirQSensorData> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new SomeProcessFunction()),
                        x -> x.deviceId,
                        Types.STRING);

        testHarness.setup();
        testHarness.open();

        return testHarness;
    }

    private static class SomeProcessFunction extends KeyedProcessFunction<String, AirQSensorData, AirQSensorData> {
        private transient ValueState<AirQSensorData> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<AirQSensorData> stateDescriptor = new ValueStateDescriptor<>("latestMeasurementForDevice", AirQSensorData.class);

            state = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(AirQSensorData value, KeyedProcessFunction<String, AirQSensorData, AirQSensorData>.Context context, Collector<AirQSensorData> collector) throws Exception {
            state.update(value);
            collector.collect(value);
        }
    }
}