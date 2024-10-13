package org.done.flink.exercises.exercises.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.util.Collector;
import org.done.flink.exercises.data.airq.AirQSensorData;
import org.done.flink.exercises.data.events.Event;
import org.done.flink.exercises.data.events.EventType;
import org.done.flink.exercises.util.ExerciseTest;
import org.done.flink.exercises.util.Helper;
import org.done.flink.exercises.util.cep.Ex00;
import org.done.flink.exercises.util.cep.Ex01;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class Ex00Test extends ExerciseTest {
    /**
     * Sample to show how everything works.
     * We will detect if and when the air quality in a stream is below OSHA standards.
     */
    @Test
    public void part1() throws Exception {
        // Fetching your input here. It is a keyed, fixed stream of data. The key is the deviceId.
        final var input = prepareData(Ex00.dataPart1(), env);

        // Displaying the input
        Helper.printTemperatureAndHumidity(input);

        // OSHA regulation values
        double minOSHATemperature = 20.0;
        double maxOSHATemperature = 24.444;
        double minOSHAHumidity = 20.0;
        double maxOSHAHumidity = 60.0;

        // Defining the pattern.
        // It goes from OK to not OK. In the real world we would not look for a state transition, but the unhealthy
        // state existence. We do it only to demonstrate how the API works.
        // The `AfterMatchSkipStrategy` is necessary to discard additional matches (given that we use `timesOrMore`)
        Pattern<AirQSensorData, ?> newDeviceReadyPattern = Pattern.<AirQSensorData>begin("healthy", AfterMatchSkipStrategy.skipPastLastEvent())
                // The `begin` function marks the beginning of a pattern, it also names the first "state"
                .where(SimpleCondition.of(AirQSensorData::isOk))
                // Multiple `where` functions are equivalent to an `AND`.
                .where(SimpleCondition.of(x -> x.temperature >= minOSHATemperature))
                .where(SimpleCondition.of(x -> x.temperature <= maxOSHATemperature))
                .where(SimpleCondition.of(x -> (x.humidity >= minOSHAHumidity) && (x.humidity <= maxOSHAHumidity)))
                // `next` marks a new state, a transition
                .next("unhealthy")
                // The next state is again described using a `where` function.
                // We can also use more complicated expressions to check
                .where(SimpleCondition.of(x ->
                        Arrays.asList(
                                x.temperature < minOSHATemperature,
                                x.temperature > maxOSHATemperature,
                                x.humidity < minOSHAHumidity,
                                x.humidity > maxOSHAHumidity
                        ).contains(true)))
                // To ensure that it wasn't a fluke, we what to check that it occurs more than once
                .timesOrMore(3);


        // Applying your pattern to the input and collecting it in the output variable.
        CEP.pattern(input, newDeviceReadyPattern)
                .process(new PatternProcessFunction<AirQSensorData, Event>() {
                    @Override
                    public void processMatch(Map<String, List<AirQSensorData>> map, Context context, Collector<Event> collector) {
                        // NOTE: The key `deviceReady` needs to exist in your pattern!
                        // If no part of the pattern has that name you will get a NullPointerException.
                        final var beginningOfViolation= map.get("unhealthy").get(0);
                        collector.collect(new Event(beginningOfViolation.deviceId, EventType.OHSA_VIOLATION, beginningOfViolation.deviceTimestamp));
                    }
                })
                .executeAndCollect().forEachRemaining(output::add);

        // If you want to debug something
        Helper.printEvents(output);

        // Validating your output against the expected one. Did you miss anything?
        assertTrue(checkOutput("Ex00, Part 1", output, Ex00.getExpectedOutputPart1()));
    }
}