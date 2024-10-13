package org.done.flink.exercises.util;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.done.flink.exercises.data.airq.AirQSensorData;
import org.done.flink.exercises.data.events.Event;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.util.ArrayList;
import java.util.List;

public class ExerciseTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());
    protected List<Event> output;
    protected StreamExecutionEnvironment env;

    protected static KeyedStream<AirQSensorData, String> prepareData(final List<AirQSensorData> data, final StreamExecutionEnvironment env) {
        return env.fromCollection(data)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(AirQSensorData airQSensorData) {
                        return airQSensorData.deviceTimestamp.toEpochMilli();
                    }
                })
                .keyBy(e -> e.deviceId);
    }

    protected static boolean checkOutput(final String exerciseIdentifier, final List<Event> actual, final List<Event> expected) {
        if (actual.size() > expected.size()) {
            System.out.println("--- Issue with exercise " + exerciseIdentifier + " ---");
            System.out.println("Got more events than expected!");
            System.out.println("You should only emit:");

            for (final Event e : expected) {
                System.out.println(e);
            }

            System.out.println("But you found:");

            if (actual.size() > 0) {
                for (final Event e : actual) {
                    System.out.println(e);
                }
            } else {
                System.out.println("Nothing!");
            }

            System.out.println("Try again!");
            return false;
        } else if (actual.size() < expected.size()) {
            System.out.println("--- Issue with exercise " + exerciseIdentifier + " ---");
            System.out.println("Got less events than expected!");
            System.out.println("You should emit:");

            for (final Event e : expected) {
                System.out.println(e);
            }

            System.out.println("But you found:");

            if (actual.size() > 0) {
                for (final Event e : actual) {
                    System.out.println(e);
                }
            } else {
                System.out.println("Nothing!");
            }
            System.out.println("Try again!");
            return false;
        } else {
            // Due to the keying the order in the list is not preserved
            // Using `CollectionUtils.isEqualCollection()` will give a false-positive
            if (CollectionUtils.removeAll(expected, actual).size() == 0) {
                System.out.println("You solved exercise " + exerciseIdentifier + "!");
                return true;
            } else {
                System.out.println("--- Issue with exercise " + exerciseIdentifier + " ---");
                System.out.println("Still missing some:");
                for (final var missing : CollectionUtils.removeAll(expected, actual)) {
                    System.out.println(missing);
                }
                return false;
            }
        }
    }

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        output = new ArrayList<>();
    }

    @After
    public void teardown() {
        output.clear();
    }

}
