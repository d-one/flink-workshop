package org.done.flink.exercises.exercises.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.util.Collector;
import org.done.flink.exercises.util.Helper;
import org.done.flink.exercises.util.cep.Ex01;
import org.done.flink.exercises.data.airq.AirQSensorData;
import org.done.flink.exercises.data.events.Event;
import org.done.flink.exercises.data.events.EventType;
import org.done.flink.exercises.util.ExerciseTest;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class Ex01Test extends ExerciseTest {
    /**
     * Simply detect when a device jumps from warming up to being ready.
     */
    @Test
    public void part1() throws Exception {
        // Fetching your input here. It is a keyed, fixed stream of data. The key is the deviceId.
        final var input = prepareData(Ex01.dataPart1(), env);

        // If you want to see the input
        // Helper.printCO2Input(input);

        // The pattern of events that you want to capture.
        // Be careful, the explicit type is necessary here (even if Intellij tells you otherwise).
        Pattern<AirQSensorData, ?> newDeviceReadyPattern = Pattern.<AirQSensorData>begin("deviceWarmUp");
        // TODO add code here

        // Applying your pattern to the input and collecting it in the output variable.
        CEP.pattern(input, newDeviceReadyPattern)
                .process(new PatternProcessFunction<AirQSensorData, Event>() {
                    @Override
                    public void processMatch(Map<String, List<AirQSensorData>> map, Context context, Collector<Event> collector) {
                        // NOTE: The key `deviceReady` needs to exist in your pattern!
                        // If no part of the pattern has that name you will get a NullPointerException.
                        final var lastMatch = map.get("deviceReady").get(map.get("deviceReady").size() - 1);
                        collector.collect(new Event(lastMatch.deviceId, EventType.NEW_DEVICE, lastMatch.deviceTimestamp));
                    }
                })
                .executeAndCollect().forEachRemaining(output::add);

        // If you want to debug something
        // Helper.printEvents(output);

        // Validating your output against the expected one. Did you miss anything?
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

        Pattern<AirQSensorData, ?> newDeviceReadyPattern = Pattern.<AirQSensorData>begin("deviceWarmUp");
        // TODO Add code here

        CEP.pattern(input, newDeviceReadyPattern)
                .process(new PatternProcessFunction<AirQSensorData, Event>() {
                    @Override
                    public void processMatch(Map<String, List<AirQSensorData>> map, Context context, Collector<Event> collector) throws Exception {
                        final var lastMatch = map.get("deviceReady").get(map.get("deviceReady").size() - 1);
                        collector.collect(new Event(lastMatch.deviceId, EventType.NEW_DEVICE, lastMatch.deviceTimestamp));
                    }
                })
                .executeAndCollect().forEachRemaining(output::add);

        assertTrue(checkOutput("Ex01, Part 2", output, Ex01.getExpectedOutputPart2()));
    }

    /**
     * Similar to Part 2, and thus optional.
     * Detect when a device jumps back from being ready to warming up.
     */
    @Test
    public void part3() throws Exception {
        final var input = prepareData(Ex01.dataPart3(), env);

        Pattern<AirQSensorData, ?> newDeviceReadyPattern = Pattern.begin("deviceWarmUp");
        // TODO Add code here

        CEP.pattern(input, newDeviceReadyPattern)
                .process(new PatternProcessFunction<AirQSensorData, Event>() {
                    @Override
                    public void processMatch(Map<String, List<AirQSensorData>> map, Context context, Collector<Event> collector) throws Exception {
                        // NOTE: The key `deviceWarmingUpAgain` needs to exist in your pattern!
                        // If no part of the pattern has that name you will get a NullPointerException.
                        final var lastMatch = map.get("deviceWarmingUpAgain").get(map.get("deviceWarmingUpAgain").size() - 1);
                        collector.collect(new Event(lastMatch.deviceId, EventType.DEVICE_MISBEHAVING, lastMatch.deviceTimestamp));
                    }
                })
                .executeAndCollect().forEachRemaining(output::add);

        assertTrue(checkOutput("Ex01, Part 3", output, Ex01.getExpectedOutputPart3()));
    }
}