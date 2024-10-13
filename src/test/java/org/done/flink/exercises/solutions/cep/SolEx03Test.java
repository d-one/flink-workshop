package org.done.flink.exercises.solutions.cep;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.done.flink.exercises.data.airq.AirQSensorData;
import org.done.flink.exercises.data.events.Event;
import org.done.flink.exercises.data.events.EventType;
import org.done.flink.exercises.util.ExerciseTest;
import org.done.flink.exercises.util.cep.Ex03;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

public class SolEx03Test extends ExerciseTest {
    /**
     * Detect if a person is entering or leaving a room.
     */
    @Test
    public void part1() throws Exception {
        final var measurements = prepareData(Ex03.data(), env);

        final double windowSize = 5;

        final var increaseInCo2 = measurements
                .filter(x -> x.co2 != null)
                .keyBy(x -> x.deviceId)
                .window(TumblingEventTimeWindows.of(Time.minutes((long) windowSize)))
                .aggregate(new AggregateFunction<AirQSensorData, List<AirQSensorData>, MeasurementGradient>() {
                    @Override
                    public List<AirQSensorData> createAccumulator() {
                        return new ArrayList<>();
                    }

                    @Override
                    public List<AirQSensorData> add(AirQSensorData airQSensorData, List<AirQSensorData> accumulator) {

                        if (accumulator.size() > 0 && !Objects.equals(accumulator.get(0).deviceId, airQSensorData.deviceId)) {
                            throw new IllegalArgumentException("Something off with the key!");
                        }

                        return Stream.concat(accumulator.stream(), Stream.of(airQSensorData)).collect(Collectors.toList());
                    }

                    @Override
                    public MeasurementGradient getResult(List<AirQSensorData> accumulator) {
                        accumulator.sort(Comparator.comparing(a -> a.deviceTimestamp));

                        var first = accumulator.get(0);
                        var last = accumulator.get(accumulator.size() - 1);

                        // NB.: The duration can be 0 -> how would you deal with this then?
                        var gradient = (last.co2 - first.co2) / (Duration.between(first.deviceTimestamp, last.deviceTimestamp).toSeconds());

                        // System.out.println("window: (" + first.deviceTimestamp + ", " + last.deviceTimestamp + "), co2: (" + first.co2 + ", " +  last.co2 + "), gradient: " + gradient);

                        return new MeasurementGradient(
                                first.deviceId,
                                first.deviceTimestamp,
                                gradient);
                    }

                    @Override
                    public List<AirQSensorData> merge(List<AirQSensorData> a, List<AirQSensorData> b) {
                        if (Stream.concat(a.stream(), b.stream()).map(x -> x.deviceId).distinct().count() > 1) {
                            throw new IllegalArgumentException("Something off with the key!");
                        }

                        return Stream.concat(a.stream(), b.stream()).collect(Collectors.toList());
                    }
                });

        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();

        Pattern<MeasurementGradient, ?> pattern = Pattern.<MeasurementGradient>begin("begin", skipStrategy)
                .where(SimpleCondition.of(x -> x.getCo2Gradient() < 0.25))
                .next("fromOneToTwo")
                .where(SimpleCondition.of(x -> x.getCo2Gradient() >= 0.25))
                .timesOrMore(2);

        CEP.pattern(increaseInCo2, pattern)
                .process(new PatternProcessFunction<MeasurementGradient, Event>() {
                    @Override
                    public void processMatch(Map<String, List<MeasurementGradient>> map, Context context, Collector<Event> collector) {
                        final var t = map.get("fromOneToTwo").get(0);
                        collector.collect(new Event(t.getDeviceId(), EventType.PERSON_HAS_ENTERED_THE_ROOM, t.getDeviceTimestamp()));
                    }
                })
                .executeAndCollect().forEachRemaining(output::add);

        Pattern<MeasurementGradient, ?> windowOpeningPattern = Pattern.<MeasurementGradient>begin("begin", skipStrategy)
                .where(SimpleCondition.of(x -> x.getCo2Gradient() >= 0))
                .next("windowOpening")
                .where(SimpleCondition.of(x -> x.getCo2Gradient() < -0.5))
                .timesOrMore(2);

        CEP.pattern(increaseInCo2, windowOpeningPattern)
                .process(new PatternProcessFunction<MeasurementGradient, Event>() {
                    @Override
                    public void processMatch(Map<String, List<MeasurementGradient>> map, Context context, Collector<Event> collector) {
                        final var t = map.get("windowOpening").get(0);
                        collector.collect(new Event(t.getDeviceId(), EventType.WINDOW_OPENED, t.getDeviceTimestamp()));
                    }
                })
                .executeAndCollect().forEachRemaining(output::add);

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
}