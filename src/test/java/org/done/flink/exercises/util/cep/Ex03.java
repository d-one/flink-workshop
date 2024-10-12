package org.done.flink.exercises.util.cep;

import org.done.flink.exercises.data.airq.AirQSensorData;
import org.done.flink.exercises.data.airq.utils.Recordings;
import org.done.flink.exercises.data.events.Event;
import org.done.flink.exercises.data.events.EventType;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

public class Ex03 {
    public static List<AirQSensorData> data() throws IOException {
        return Recordings.readMultiPeople();
    }

    public static List<Event> getExpectedOutput() {
        return List.of(
                new Event("339966b51f42510ca487d3f9609483b4", EventType.PERSON_HAS_ENTERED_THE_ROOM, Instant.parse("2024-09-05T13:56:46Z")),
                new Event("339966b51f42510ca487d3f9609483b4", EventType.WINDOW_OPENED, Instant.parse("2024-09-05T14:20:53Z"))
        );
    }
}
