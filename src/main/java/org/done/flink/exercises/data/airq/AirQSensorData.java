package org.done.flink.exercises.data.airq;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * A simple POJO that holds the sensor data of an AirQ device.
 */
public class AirQSensorData implements Serializable {
    public Instant deviceTimestamp;
    public String deviceId;
    public String status;
    public int health;
    public int performance;
    public Double co2;
    public Double co2Error;
    public Double co;
    public Double coError;
    public Double dewpt;
    public Double dewptError;
    public Double humidity;
    public Double humidityError;
    public Double humidityAbs;
    public Double humidityAbsError;
    public Double pressure;
    public Double pressureError;
    public Double sound;
    public Double soundError;
    public Double soundMax;
    public Double soundMaxError;
    public Double temperature;
    public Double temperatureError;
    public Double tvoc;
    public Double tvocError;

    public AirQSensorData() {
    }

    public AirQSensorData(final AirQSensorData airQSensorData) {
        this.deviceTimestamp = airQSensorData.deviceTimestamp;
        this.deviceId = airQSensorData.deviceId;
        this.status = airQSensorData.status;
        this.health = airQSensorData.health;
        this.performance = airQSensorData.performance;
        this.co2 = airQSensorData.co2;
        this.co2Error = airQSensorData.co2Error;
        this.co = airQSensorData.co;
        this.coError = airQSensorData.coError;
        this.dewpt = airQSensorData.dewpt;
        this.dewptError = airQSensorData.dewptError;
        this.humidity = airQSensorData.humidity;
        this.humidityError = airQSensorData.humidityError;
        this.humidityAbs = airQSensorData.humidityAbs;
        this.humidityAbsError = airQSensorData.humidityAbsError;
        this.pressure = airQSensorData.pressure;
        this.pressureError = airQSensorData.pressureError;
        this.sound = airQSensorData.sound;
        this.soundError = airQSensorData.soundError;
        this.soundMax = airQSensorData.soundMax;
        this.soundMaxError = airQSensorData.soundMaxError;
        this.temperature = airQSensorData.temperature;
        this.temperatureError = airQSensorData.temperatureError;
        this.tvoc = airQSensorData.tvoc;
        this.tvocError = airQSensorData.tvocError;
    }

    public static AirQSensorData fromJsonString(final String rawData) {
        AirQSensorData record = new AirQSensorData();

        final JsonObject payload = new Gson().fromJson(rawData, JsonObject.class);

        record.deviceId = payload.get("DeviceID").getAsString();
        record.safeLoadMeasurement(payload, "co");
        record.safeLoadMeasurement(payload, "co2");
        record.safeLoadMeasurement(payload, "dewpt");
        record.safeLoadMeasurement(payload, "humidity");
        record.safeLoadMeasurement(payload, "humidity_abs", "humidityAbs");
        record.safeLoadMeasurement(payload, "pressure");
        record.safeLoadMeasurement(payload, "sound");
        record.safeLoadMeasurement(payload, "sound_max", "soundMax");
        record.safeLoadMeasurement(payload, "temperature");
        record.safeLoadMeasurement(payload, "tvoc");

        record.status = payload.get("Status").toString();

        record.health = payload.get("health").getAsInt();
        record.performance = payload.get("performance").getAsInt();

        record.deviceTimestamp = Instant.ofEpochMilli(payload.get("timestamp").getAsLong());

        return record;
    }

    public static AirQSensorData sample() {
        final String rawString = "{" +
                "\"sound_max\":[80.80001,1.8], " +
                "\"humidity_abs\":[7.607,0.42]," +
                "\"health\":1000," +
                "\"sound\":[34.28,9.8]," +
                "\"timestamp\":1711542740000," +
                "\"DeviceID\":\"339966b51f42510ca487d3f9609483b4\"," +
                "\"Status\":{" +
                "  \"co\":\"co sensor still in warm up phase; waiting time = 1646 s\"," +
                "  \"tvoc\":\"tvoc sensor still in warm up phase; waiting time = 86 s\"}," +
                "\"humidity\":[50.872,3.48]," +
                "\"performance\":757," +
                "\"dewpt\":[7.378,0.81]," +
                "\"pressure\":[935.2,1.0]," +
                "\"temperature\":[17.485,0.55]," +
                "\"co2\":[712.8,71.4]," +
                "\"tvoc\":null," +
                "\"co\":null" +
                "}";

        return AirQSensorData.fromJsonString(rawString);
    }

    /**
     * Helper build on top of the status.
     * Remember that the status is still raw JSON object. It can either be a single string or a full object.
     *
     * @return true, if the device is OK, false otherwise.
     */
    public boolean isOk() {
        return status.equals("\"OK\"");
    }

    /**
     * Helper build on top of the status.
     * Remember that the status is still raw JSON object. It can either be a single string or a full object.
     *
     * @return true, if at least one sensor is still warming up, false otherwise.
     */
    public boolean isWarmingUp() {
        return status.contains("warm up phase");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AirQSensorData that = (AirQSensorData) o;
        return health == that.health
                && performance == that.performance
                && deviceTimestamp.equals(that.deviceTimestamp)
                && deviceId.equals(that.deviceId)
                && status.equals(that.status)
                && Objects.equals(co2, that.co2)
                && Objects.equals(co2Error, that.co2Error)
                && Objects.equals(co, that.co)
                && Objects.equals(coError, that.coError)
                && Objects.equals(dewpt, that.dewpt)
                && Objects.equals(dewptError, that.dewptError)
                && Objects.equals(humidity, that.humidity)
                && Objects.equals(humidityError, that.humidityError)
                && Objects.equals(humidityAbs, that.humidityAbs)
                && Objects.equals(humidityAbsError, that.humidityAbsError)
                && Objects.equals(pressure, that.pressure)
                && Objects.equals(pressureError, that.pressureError)
                && Objects.equals(sound, that.sound)
                && Objects.equals(soundError, that.soundError)
                && Objects.equals(soundMax, that.soundMax)
                && Objects.equals(soundMaxError, that.soundMaxError)
                && Objects.equals(temperature, that.temperature)
                && Objects.equals(temperatureError, that.temperatureError)
                && Objects.equals(tvoc, that.tvoc)
                && Objects.equals(tvocError, that.tvocError);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deviceTimestamp, deviceId, status, health, performance, co2, co2Error, co, coError, dewpt, dewptError, humidity, humidityError, humidityAbs, humidityAbsError, pressure, pressureError, sound, soundError, soundMax, soundMaxError, temperature, temperatureError, tvoc, tvocError);
    }

    private void safeLoadMeasurement(final JsonObject payload, final String payloadPath) {
        this.safeLoadMeasurement(payload, payloadPath, payloadPath);

    }

    /**
     * Loads from s JsonObject safely into the object.
     * Assumes that every field is a Double (which is strictly speaking not true, but good enough for now.
     *
     * @param payload     The JsonObject payload
     * @param payloadPath The full path in the payload
     * @param field       The target field name
     */
    private void safeLoadMeasurement(final JsonObject payload, final String payloadPath, final String field) {
        if (!payload.has(payloadPath)) {
            throw new IllegalArgumentException("No such field: '" + payloadPath + "' in the given payload: '" + payload.getAsString() + "'.");
        }

        try {
            if (payload.get(payloadPath).isJsonNull()) {
                AirQSensorData.class.getDeclaredField(field).set(this, null);
                AirQSensorData.class.getDeclaredField(field + "Error").set(this, null);
            } else {
                final JsonArray values = payload.getAsJsonArray(payloadPath);
                AirQSensorData.class.getDeclaredField(field).set(this, values.get(0).getAsDouble());
                AirQSensorData.class.getDeclaredField(field + "Error").set(this, values.get(1).getAsDouble());
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public String getDeviceId() {
        return deviceId;
    }


    /**
     * Helper build on top of the co2.
     * @return true, if co2 is within healthy range
     */
    public boolean isCo2LevelsHealthy() {
        return co2 < 880;
    }

    public boolean isCo2LevelDangerous() {
        return co2 > 1500;
    }
}
