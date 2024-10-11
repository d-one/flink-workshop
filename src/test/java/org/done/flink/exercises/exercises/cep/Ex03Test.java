package org.done.flink.exercises.exercises.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.done.flink.exercises.util.cep.Ex03;
import org.done.flink.exercises.data.events.Event;
import org.done.flink.exercises.data.events.EventType;
import org.done.flink.exercises.util.ExerciseTest;
import org.junit.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class Ex03Test extends ExerciseTest {
    /**
     * This is a larger exercise. It consists of two inner parts:
     * The first goal is to map the measurements to a gradient.
     * Then you will use this gradient to detect patterns.
     */
    @Test
    public void part1() throws Exception {
        final var measurements = prepareData(Ex03.data(), env);

        // You will first have to create a new "view" on the data. So far we have looked a single data points.
        // Now we will need to create aggregation windows on the data with the goal to tell if a value is increasing, and if yes by how much.
        // For this purpose we use a window in which we aggregate.

        // Useful documentation: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/windows/

        // Just some random number... but does the trick.
        final double windowSizeMinutes = 5;

        // This is just a starter. When aggregating think about the type of window you want to run, and how you aggregate it.
        // In the end you want to have the gradient over the CO2 measurements.
        final SingleOutputStreamOperator<MeasurementGradient> increaseInCo2 = measurements
                .map(x -> new MeasurementGradient(x.deviceId, x.deviceTimestamp, x.co2));

        // You might want to double-check your calculations:
        // increaseInCo2.executeAndCollect(10).forEach(System.out::println);

        // The pattern where you detect an increase in the measurements. Which we interpret as an Event of type PERSON_HAS_ENTERED_THE_ROOM.
        Pattern<MeasurementGradient, ?> gradientIncrease = Pattern.begin("begin");

        CEP.pattern(increaseInCo2, gradientIncrease)
                .process(new PatternProcessFunction<MeasurementGradient, Event>() {
                    @Override
                    public void processMatch(Map<String, List<MeasurementGradient>> map, Context context, Collector<Event> collector) {
                        final var t = map.get("fromOneToTwo").get(0);
                        collector.collect(new Event(t.deviceId, EventType.PERSON_HAS_ENTERED_THE_ROOM, t.getDeviceTimestamp()));
                    }
                })
                .executeAndCollect().forEachRemaining(output::add);

        // The pattern where you detect a fast decrease in the measurements. Which we interpret as an Event of type WINDOW_OPENED.
        Pattern<MeasurementGradient, ?> windowOpeningPattern = Pattern.begin("begin");

        CEP.pattern(increaseInCo2, windowOpeningPattern)
                .process(new PatternProcessFunction<MeasurementGradient, Event>() {
                    @Override
                    public void processMatch(Map<String, List<MeasurementGradient>> map, Context context, Collector<Event> collector) {
                        final var t = map.get("windowOpening").get(0);
                        collector.collect(new Event(t.getDeviceId(), EventType.WINDOW_OPENED, t.getDeviceTimestamp()));
                    }
                })
                .executeAndCollect().forEachRemaining(output::add);

        // If you want to debug something
        // for (final Event e : output) {
        //     System.out.println(e);
        // }

        assertTrue(checkOutput("Ex03", output, Ex03.getExpectedOutput()));
    }

    /**
     * Simple helper class to help you a bit. You can also use Tuple2, Tuple3, ...
     */
    private static class MeasurementGradient {
        private final String deviceId;
        private final Instant deviceTimestamp;
        private final Double co2Gradient;

        public MeasurementGradient(String deviceId, Instant deviceTimestamp, Double co2Gradient) {
            this.deviceId = deviceId;
            this.deviceTimestamp = deviceTimestamp;
            this.co2Gradient = co2Gradient;
        }

        public String getDeviceId() {
            return deviceId;
        }

        public Instant getDeviceTimestamp() {
            return deviceTimestamp;
        }

        public Double getCo2Gradient() {
            return co2Gradient;
        }
    }

    ;
}