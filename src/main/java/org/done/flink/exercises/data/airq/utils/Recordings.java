package org.done.flink.exercises.data.airq.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.done.flink.exercises.data.airq.AirQSensorData;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Recordings {
    public static List<AirQSensorData> readRecording(final String name) throws IOException {
        InputStream is = Recordings.class.getResourceAsStream("/recordings/" + name);

        if (is == null) {
            throw new IllegalArgumentException("No such recording known. Given: " + name);
        }

        final JsonNode jsonNode = new ObjectMapper().readTree(is);

        final var recordings = new ArrayList<AirQSensorData>();
        jsonNode.elements().forEachRemaining(i -> {
            if (!Objects.equals(i.toString(), "")) {
                recordings.add(AirQSensorData.fromJsonString(i.toString()));
            }
        });

        is.close();

        return recordings;
    }

    public static List<AirQSensorData> readSmall() throws IOException {
        return readRecording("single_room_recording.json");
    }

    public static List<AirQSensorData> readMultiPeople() throws IOException {
        return readRecording("multiple_people.json");
    }

    public static List<AirQSensorData> readMultiDay() throws IOException {
        return readRecording("multiple_days.json");
    }
}
