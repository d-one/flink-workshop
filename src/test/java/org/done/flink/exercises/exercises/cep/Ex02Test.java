package org.done.flink.exercises.exercises.cep;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.done.flink.exercises.data.airq.AirQSensorData;
import org.done.flink.exercises.data.airq.utils.DelayedSource;
import org.done.flink.exercises.data.events.Event;
import org.done.flink.exercises.data.events.EventType;
import org.done.flink.exercises.util.ExerciseTest;
import org.done.flink.exercises.util.LogWatermarkProcessFunction;
import org.done.flink.exercises.util.cep.Ex02;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class Ex02Test extends ExerciseTest {
    /**
     * Detect when the C02 value in a room is dangerous and when the levels reached warning status
     * Hint: Do not consider a single spike.
     * WARNING threshold for co2 = 880ppm
     * DANGER threshold for co2 = 1500ppm
     */
    @Test
    public void part1() throws Exception {
        final var input = prepareData(Ex02.data(), env);

        // The pattern of events that you want to capture.
        // We need to know when we passed the danger threshold and how early we got the first warning (if at all)
        Pattern<AirQSensorData, ?> dangerousCO2LevelsDetected = Pattern.<AirQSensorData>begin("begin");
        // TODO add code here

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


        // If you want to debug something
         for (final Event e : output) {
             System.out.println(e);
         }

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

        printInput(input);

        //TODO: Copy here the pattern you created in part1
        Pattern<AirQSensorData, ?> dangerousCO2LevelsDetected = Pattern.<AirQSensorData>begin("begin");

        //TODO: Alter this so that you can separate late data
        //Apply the pattern to the input stream
        CEP.pattern(input, dangerousCO2LevelsDetected).process(new PatternProcessFunction<AirQSensorData, Event>() {
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

        //for (final Event e : output) {
        //    System.out.println(e);
        //}

        assertTrue(checkOutput("Ex02, Part 2", output, Ex02.getExpectedOutputPart2()));
    }

    protected static void printInput(DataStream<AirQSensorData> input) {
        input.map(sensorData -> {
            return "Device ID: " + sensorData.deviceId +
                    ", Timestamp: " + sensorData.deviceTimestamp +
                    ", CO2: " + sensorData.co2;
        }).print();
    }
}

