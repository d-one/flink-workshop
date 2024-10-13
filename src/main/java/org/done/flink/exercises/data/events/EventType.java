package org.done.flink.exercises.data.events;

public enum EventType {
    NEW_DEVICE("new device"),
    DEVICE_MISBEHAVING("device misbehaving"),
    CO2_WARNING("co2 value above 880ppm"),
    CO2_DANGER("co2 value above 1500ppm"),
    WINDOW_OPENED("a window has been opened"),
    PERSON_HAS_ENTERED_THE_ROOM("a person has entered the room"),
    OHSA_VIOLATION("the measurements to not align with OSHA requirements")
    ;

    private final String value;

    EventType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
