package ru.yandex.practicum.collector.mapper;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.collector.dto.sensor.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SensorEventMapper {

    public static SensorEventAvro toAvro(SensorEvent event) {
        SensorEventAvro.Builder b = SensorEventAvro.newBuilder();
        b.setHubId(event.getHubId());
        b.setId(event.getId());
        b.setTimestamp(event.getTimestamp());
        b.setPayload(getPayload(event));
        return b.build();
    }

    private static Object getPayload(SensorEvent event) {
        if (event instanceof ClimateSensorEvent x) {
            return ClimateSensorAvro.newBuilder()
                    .setCo2Level(x.getCo2Level())
                    .setHumidity(x.getHumidity())
                    .setTemperatureC(x.getTemperatureC())
                    .build();
        }
        if (event instanceof LightSensorEvent x) {
            return LightSensorAvro.newBuilder()
                    .setLinkQuality(x.getLinkQuality())
                    .setLuminosity(x.getLuminosity())
                    .build();
        }
        if (event instanceof MotionSensorEvent x) {
            return MotionSensorAvro.newBuilder()
                    .setLinkQuality(x.getLinkQuality())
                    .setMotion(x.getMotion())
                    .setVoltage(x.getVoltage())
                    .build();
        }
        if (event instanceof SwitchSensorEvent x) {
            return SwitchSensorAvro.newBuilder()
                    .setState(x.getState())
                    .build();
        }
        if (event instanceof TemperatureSensorEvent x) {
            return TemperatureSensorAvro.newBuilder()
                    .setTemperatureC(x.getTemperatureC())
                    .setTemperatureF(x.getTemperatureF())
                    .build();
        }
        throw new IllegalArgumentException("Unknown sensor event: " + event);
    }
}
