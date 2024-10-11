package org.done.flink.exercises.support;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.done.flink.exercises.data.airq.AirQSensorData;
import org.done.flink.exercises.data.airq.AirQSensorDataSampleBuilder;
import org.done.flink.exercises.data.events.Event;
import org.done.flink.exercises.data.events.EventType;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class PipelineTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testEventDetection() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        var input = env.fromElements(
                        new AirQSensorDataSampleBuilder().setWarmingUp().build(),
                        new AirQSensorDataSampleBuilder().setOk().build(),
                        new AirQSensorDataSampleBuilder().setOk().setCo2(2000.0).build(),
                        new AirQSensorDataSampleBuilder().setOk().setCo2(2000.0).build(),
                        new AirQSensorDataSampleBuilder().setOk().setCo2(2000.0).build(),
                        new AirQSensorDataSampleBuilder().setOk().setCo2(2000.0).build(),
                        new AirQSensorDataSampleBuilder().setOk().setCo2(2000.0).build(),
                        new AirQSensorDataSampleBuilder().setOk().setCo2(2100.0).build())
                .map((MapFunction<AirQSensorData, AirQSensorData>) airQSensorData -> {
                    airQSensorData.deviceTimestamp = Instant.now();
                    return airQSensorData;
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(AirQSensorData airQSensorData) {
                        return airQSensorData.deviceTimestamp.toEpochMilli();
                    }
                })
                .keyBy(e -> e.deviceId);

        CollectSink sink = new CollectSink();

        Pattern<AirQSensorData, ?> newDeviceReadyPattern = Pattern.<AirQSensorData>begin("deviceWarmUp")
                .where(SimpleCondition.of(AirQSensorData::isWarmingUp))
                .followedBy("deviceReady")
                .where(SimpleCondition.of(AirQSensorData::isOk));

        CEP.pattern(input, newDeviceReadyPattern)
                .process(new PatternProcessFunction<AirQSensorData, Event>() {
                    @Override
                    public void processMatch(Map<String, List<AirQSensorData>> map, Context context, Collector<Event> collector) throws Exception {
                        final var lastMatch = map.get("deviceReady").get(map.get("deviceReady").size() - 1);
                        collector.collect(new Event(lastMatch.deviceId, EventType.NEW_DEVICE, lastMatch.deviceTimestamp));
                    }
                })
                .addSink(sink);

        Pattern<AirQSensorData, ?> co2DangerPattern = Pattern.<AirQSensorData>begin("co2Danger")
                .where(SimpleCondition.of(AirQSensorData::isOk))
                .where(SimpleCondition.of(airQSensorData -> airQSensorData.co2 > 2000))
                .timesOrMore(5)
                .greedy();

        CEP.pattern(input, co2DangerPattern)
                .process(
                        new PatternProcessFunction<AirQSensorData, Event>() {
                            @Override
                            public void processMatch(Map<String, List<AirQSensorData>> map, Context context, Collector<Event> collector) throws Exception {
                                final var lastMatch = map.get("co2Danger").get(map.get("co2Danger").size() - 1);
                                collector.collect(new Event(lastMatch.deviceId, EventType.CO2_DANGER, lastMatch.deviceTimestamp));
                            }
                        })
                .addSink(sink);

        env.execute();

        System.out.println("Collected: ");
        for (final Event e : CollectSink.values) {
            System.out.println(e);
        }

        assertTrue(CollectSink.values.size() > 0);
    }

    private static class CollectSink implements SinkFunction<Event> {
        public static final List<Event> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Event value, SinkFunction.Context context) throws Exception {
            values.add(value);
        }
    }
}
