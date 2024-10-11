package org.done.flink.exercises.support;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.done.flink.exercises.data.airq.AirQSensorData;
import org.done.flink.exercises.data.airq.generators.AirQSensorDataFaker;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AirQSensorDataFakerTest {
    // @ClassRule
    // public static MiniClusterWithClientresource flinkCluster = new MiniClusterWithClientResource(
    //         new MiniClusterResourceConfiguration.Builder()
    //                 .setNumberSlotsPerTaskManager(2)
    //                 .setNumberTaskManagers(1)
    //                 .build()
    // );

    /*
    protected StreamExecutionEnvironment env;

    @Before
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.setStateBackend(new HashMapStateBackend());
    }
     */

    @Test
    void testSetup() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, AirQSensorData, AirQSensorData> testHarness = setupHarness();

        assertEquals(0, testHarness.numKeyedStateEntries());
        testHarness.setProcessingTime(0L);

        testHarness.processElement(new StreamRecord<>(AirQSensorData.sample()));

        assertEquals(1, testHarness.numKeyedStateEntries());

        testHarness.close();
    }

    @Test
    void testGeneration() throws Exception {
        // TODO look into https://github.com/ververica/flink-training-exercises/blob/master/src/test/java/com/ververica/flinktraining/exercises/datastream_java/state/RidesAndFaresTest.java
        DataGeneratorSource<AirQSensorData> dataSource = new AirQSensorDataFaker().getRandomDataGeneratorSource();
        StreamSource<AirQSensorData, DataGeneratorSource<AirQSensorData>> src = new StreamSource<>(dataSource);

        // KeyedOneInputStreamOperatorTestHarness<String, AirQSensorData, AirQSensorData> testHarness = setupHarness();
        // The keyed operator expects some more setup... not sure what right now
        AbstractStreamOperatorTestHarness<AirQSensorData> testHarness = new AbstractStreamOperatorTestHarness<AirQSensorData>(src, 1, 1, 0);
        testHarness.open();

        int totalNumber = 10;
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

        testHarness.close();

        // OneInputStreamOperatorTestHarness<String, String> testHarness =
        //         new KeyedOneInputStreamOperatorTestHarness<>(
        //                 new KeyedProcessOperator<>(myProcessFunction), x -> "1", Types.STRING);

        // OneInputStreamOperatorTestHarness<AirQSensorData> testHarness = new KeyedOneInputStreamOperatorTest
    }

    private static class CollectSink implements SinkFunction<AirQSensorData> {
        public static final List<AirQSensorData> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(AirQSensorData value, SinkFunction.Context context) {
            values.add(value);
        }
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
}