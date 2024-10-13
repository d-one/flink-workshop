package org.done.flink.exercises.solutions.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.done.flink.exercises.data.airq.AirQSensorData;
import org.done.flink.exercises.data.events.Event;
import org.done.flink.exercises.data.events.EventType;
import org.done.flink.exercises.util.ExerciseTest;
import org.done.flink.exercises.util.cep.Ex01;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class SolEx01Test extends ExerciseTest {
    /**
     * Simply detect when a device jumps from warming up to being ready.
     */
    @Test
    public void part1() throws Exception {
        final var input = prepareData(Ex01.dataPart1(), env);

        Pattern<AirQSensorData, ?> newDeviceReadyPattern = Pattern.<AirQSensorData>begin("deviceWarmUp")
                .where(SimpleCondition.of(AirQSensorData::isWarmingUp))
                .next("deviceReady")
                .where(SimpleCondition.of(AirQSensorData::isOk));

        CEP.pattern(input, newDeviceReadyPattern)
                .process(new PatternProcessFunction<AirQSensorData, Event>() {
                    @Override
                    public void processMatch(Map<String, List<AirQSensorData>> map, Context context, Collector<Event> collector) {
                        final var lastMatch = map.get("deviceReady").get(map.get("deviceReady").size() - 1);
                        collector.collect(new Event(lastMatch.deviceId, EventType.NEW_DEVICE, lastMatch.deviceTimestamp));
                    }
                })
                .executeAndCollect().forEachRemaining(output::add);

        assertTrue(checkOutput("Ex01, Part 1", output, Ex01.getExpectedOutputPart1()));
    }

    /**
     * Detect when a device jumps from warming up to being ready.
     * But be careful, devices might also jump back from being ready.
     * <p>
     * Every time a device is ready, emit a new event.
     * <p>
     * A device is considered to be ready, when it's state is OK for more than 2 messages.
     */
    @Test
    public void part2() throws Exception {
        final var input = prepareData(Ex01.dataPart2(), env);

        // Define the skip strategy to skip past the last event after a match
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();

        // Build the pattern with the skip strategy
        Pattern<AirQSensorData, ?> newDeviceReadyPattern = Pattern.<AirQSensorData>begin("deviceWarmUp", skipStrategy)
                .where(SimpleCondition.of(AirQSensorData::isWarmingUp))  // Step 1: Detect device warming up
                .next("deviceReady")
                .where(SimpleCondition.of(AirQSensorData::isOk))         // Step 2: First OK
                .next("deviceNotWarmingUpAgain")
                .where(SimpleCondition.of(AirQSensorData::isOk))         // Step 3: Consecutive OK
                .timesOrMore(2)
                .within(Time.seconds(60));                               // Expect exactly 2 more OKs (total of 3 OKs)

        CEP.pattern(input, newDeviceReadyPattern)
                .process(new PatternProcessFunction<AirQSensorData, Event>() {
                    @Override
                    public void processMatch(Map<String, List<AirQSensorData>> map, Context context, Collector<Event> collector) {
                        // Get the third OK from deviceOkRepeated
                        final var lastMatch = map.get("deviceNotWarmingUpAgain").get(map.get("deviceNotWarmingUpAgain").size() - 1);
                        collector.collect(new Event(lastMatch.deviceId, EventType.NEW_DEVICE, lastMatch.deviceTimestamp));
                    }
                })
                .executeAndCollect().forEachRemaining(output::add);

        assertTrue(checkOutput("Ex01, Part 2", output, Ex01.getExpectedOutputPart2()));
    }

    /**
     * Similar to Part 2. But
     * Detect when a device jumps back from being ready to warming up.
     */
    @Test
    public void part3() throws Exception {
        final var input = prepareData(Ex01.dataPart3(), env);

        Pattern<AirQSensorData, ?> newDeviceReadyPattern = Pattern.<AirQSensorData>begin("deviceWarmUp")
                .where(SimpleCondition.of(AirQSensorData::isWarmingUp))
                .next("deviceReady")
                .where(SimpleCondition.of(AirQSensorData::isOk))
                .next("deviceWarmingUpAgain")
                .where(SimpleCondition.of(AirQSensorData::isWarmingUp));

        CEP.pattern(input, newDeviceReadyPattern)
                .process(new PatternProcessFunction<AirQSensorData, Event>() {
                    @Override
                    public void processMatch(Map<String, List<AirQSensorData>> map, Context context, Collector<Event> collector) {
                        final var lastMatch = map.get("deviceWarmingUpAgain").get(map.get("deviceWarmingUpAgain").size() - 1);
                        collector.collect(new Event(lastMatch.deviceId, EventType.DEVICE_MISBEHAVING, lastMatch.deviceTimestamp));
                    }
                })
                .executeAndCollect().forEachRemaining(output::add);

        assertTrue(checkOutput("Ex01, Part 3", output, Ex01.getExpectedOutputPart3()));
    }
}