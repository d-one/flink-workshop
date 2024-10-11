package org.done.flink.exercises.data.events;

import java.time.Instant;
import java.util.Objects;

/**
 * The basic event
 */
public class Event {
    public String deviceId;
    public EventType eventType;
    public Instant occurredAt;

    public Event() {
    }

    public Event(final String deviceId, final EventType eventType, final Instant occuredAt) {
        this.deviceId = deviceId;
        this.eventType = eventType;
        this.occurredAt = occuredAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event that = (Event) o;
        return deviceId.equals(that.deviceId) && eventType == that.eventType && occurredAt.equals(that.occurredAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deviceId, eventType, occurredAt);
    }

    @Override
    public String toString() {
        return "Event{" +
                "deviceId='" + deviceId + '\'' +
                ", eventType=" + eventType +
                ", occurredAt=" + occurredAt +
                '}';
    }
}
