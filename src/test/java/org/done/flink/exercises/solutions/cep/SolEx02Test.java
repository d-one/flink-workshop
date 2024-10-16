package org.done.flink.exercises.solutions.cep;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.done.flink.exercises.data.airq.AirQSensorData;
import org.done.flink.exercises.data.airq.utils.DelayedSource;
import org.done.flink.exercises.data.events.Event;
import org.done.flink.exercises.data.events.EventType;
import org.done.flink.exercises.util.ExerciseTest;
import org.done.flink.exercises.util.Helper;
import org.done.flink.exercises.util.LogWatermarkProcessFunction;
import org.done.flink.exercises.util.cep.Ex02;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class SolEx02Test extends ExerciseTest {
    /**
     * Detect when the C02 value in a room is dangerous and when the levels reached warning status
     * Hint: Do not consider a single spike.
     * WARNING threshold for co2 = 880ppm
     * DANGER threshold for co2 = 1500ppm
     */
    @Test
    public void part1() throws Exception {
        final var input = prepareData(Ex02.data(), env);

        Helper.printCO2Input(input);

        Pattern<AirQSensorData, ?> dangerousCO2LevelsDetected = Pattern.<AirQSensorData>begin("begin")
                .next("co2Healthy")
                .where(SimpleCondition.of(AirQSensorData::isCo2LevelsHealthy))
                .next("co2LevelsWarning")
                .where(SimpleCondition.of(x -> x.co2 > 880 && x.co2 <= 1500))
                .timesOrMore(2).optional()
                .next("co2LevelsDangerous")
                .where(SimpleCondition.of(x -> x.co2 > 1500))
                .times(2);

        CEP.pattern(input, dangerousCO2LevelsDetected)
                .process(new PatternProcessFunction<AirQSensorData, Event>() {
                    @Override
                    public void processMatch(Map<String, List<AirQSensorData>> pattern, Context context, Collector<Event> collector) {
                        if (pattern.containsKey("co2LevelsWarning")) {
                            final var warningMatch = pattern.get("co2LevelsWarning").get(0);
                            collector.collect(new Event(warningMatch.deviceId, EventType.CO2_WARNING, warningMatch.deviceTimestamp));
                        }
                        if (pattern.containsKey("co2LevelsDangerous")) {
                            final var dangerMatch = pattern.get("co2LevelsDangerous").get(0);
                            collector.collect(new Event(dangerMatch.deviceId, EventType.CO2_DANGER, dangerMatch.deviceTimestamp));
                        }
                    }
                })
                .executeAndCollect().forEachRemaining(output::add);

        Helper.printEvents(output);

        assertTrue(checkOutput("Ex02, Part 1", output, Ex02.getExpectedOutputPart1()));
    }

    protected static DataStream<AirQSensorData> prepareDataWithWatermark(final List<AirQSensorData> data, final StreamExecutionEnvironment env) {
        return env.fromCollection(data)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<AirQSensorData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, timestamp) -> event.deviceTimestamp.toEpochMilli())
                )
                .keyBy(AirQSensorData::getDeviceId) // Keying the stream
                .process(new LogWatermarkProcessFunction()); // Add the watermark logging here

    }

    /**
     * But now do it over a long time period. How do you deal with late data?
     * If we consider data older than 10 seconds to be old, alter the code to not take them into account
     * Bonus: Divert late data to a different stream
     */
    @Test
    public void part2() throws Exception {
        // Ingest data, but this time we will simulate real world delay
        // Sets watermark for incoming data to 10 seconds
        DataStream<AirQSensorData> input = env.addSource(new DelayedSource(Ex02.dataPart2()))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<AirQSensorData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, timestamp) -> event.deviceTimestamp.toEpochMilli())
                );

        Helper.printCO2Input(input);

        Pattern<AirQSensorData, ?> dangerousCO2LevelsDetected = Pattern.<AirQSensorData>begin("begin")
                .next("co2Healthy")
                .where(SimpleCondition.of(AirQSensorData::isCo2LevelsHealthy))
                .next("co2LevelsWarning")
                .where(SimpleCondition.of(x -> x.co2 > 880 && x.co2 <= 1500))
                .timesOrMore(2).consecutive().optional()
                .next("co2LevelsDangerous")
                .where(SimpleCondition.of(x -> x.co2 > 1500))
                .times(2).consecutive();

        OutputTag<AirQSensorData> lateDataOutputTag = new OutputTag<AirQSensorData>("late-data"){};

        // Apply the pattern to the input stream
        PatternStream<AirQSensorData> patternStream = CEP.pattern(input, dangerousCO2LevelsDetected);

        // Process the pattern matches and handle late data
        SingleOutputStreamOperator<Event> result = patternStream
                .sideOutputLateData(lateDataOutputTag)
                .process(new PatternProcessFunction<AirQSensorData, Event>() {
                    @Override
                    public void processMatch(Map<String, List<AirQSensorData>> pattern, Context context, Collector<Event> collector) {
                        if (pattern.containsKey("co2LevelsWarning")) {
                            final var warningMatch = pattern.get("co2LevelsWarning").get(0);
                            collector.collect(new Event(warningMatch.deviceId, EventType.CO2_WARNING, warningMatch.deviceTimestamp));
                        }
                        if (pattern.containsKey("co2LevelsDangerous")) {
                            final var dangerMatch = pattern.get("co2LevelsDangerous").get(0);
                            collector.collect(new Event(dangerMatch.deviceId, EventType.CO2_DANGER, dangerMatch.deviceTimestamp));
                        }
                    }
                });

        // Retrieve late data
        DataStream<AirQSensorData> lateData = result.getSideOutput(lateDataOutputTag);

        // For debugging: Print late data for debugging
        Helper.printCO2Input(lateData);

        // Collect output events
        result.executeAndCollect().forEachRemaining(output::add);

        assertTrue(checkOutput("Ex02, Part 2", output, Ex02.getExpectedOutputPart2()));
    }
}

