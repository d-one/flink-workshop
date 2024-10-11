package org.done.flink.exercises.data.airq;

import java.time.Instant;

/**
 * AirQSensorData sample builder.
 */
public class AirQSensorDataSampleBuilder {
    private final AirQSensorData base;

    public AirQSensorDataSampleBuilder() {
        base = AirQSensorData.sample();
    }

    public AirQSensorDataSampleBuilder setDeviceId(String deviceId) {
        base.deviceId = deviceId;
        return this;
    }

    public AirQSensorDataSampleBuilder setStatus(String status) {
        base.status = status;
        return this;
    }

    public AirQSensorDataSampleBuilder setOk() {
        // This is just because in the base-sample both are set to null.
        base.co = 2.690_485;
        base.coError = 0.426_985;

        base.tvoc = 272.0;
        base.tvocError = 10.0;
        return this.setStatus("\"OK\"");
    }

    public AirQSensorDataSampleBuilder setWarmingUp(int waiting_time_co_s, int waiting_time_tvoc_s) {
        return this.setStatus("{" +
                "\"co\":\"co sensor still in warm up phase; waiting time = " + waiting_time_co_s + " s\"," +
                "\"tvoc\":\"tvoc sensor still in warm up phase; waiting time = " + waiting_time_tvoc_s + " s\"" +
                "}");
    }

    public AirQSensorDataSampleBuilder setWarmingUp() {
        return setWarmingUp(1646, 86);
    }

    public AirQSensorDataSampleBuilder setCo2(double co2) {
        base.co2 = co2;
        return this;
    }

    public AirQSensorDataSampleBuilder setDeviceTimestamp(final Instant deviceTimestamp) {
        base.deviceTimestamp = deviceTimestamp;
        return this;
    }


    public AirQSensorData build() {
        return base;
    }
}
