package ru.yandex.practicum.collector.mapper;

import com.google.protobuf.Timestamp;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SensorEventMapper {

    public static SensorEventAvro toAvro(SensorEventProto event) {
        Timestamp ts = event.getTimestamp();

        SensorEventAvro.Builder b = SensorEventAvro.newBuilder();
        b.setHubId(event.getHubId());
        b.setId(event.getId());
        b.setTimestamp(Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos()));
        b.setPayload(getPayload(event));
        return b.build();
    }

    private static SpecificRecordBase getPayload(SensorEventProto event) {
        SensorEventProto.PayloadCase payloadCase = event.getPayloadCase();

        switch (payloadCase) {
            case TEMPERATURE_SENSOR -> {
                var temperatureEvent = event.getTemperatureSensor();

                return TemperatureSensorAvro.newBuilder()
                        .setTemperatureC(temperatureEvent.getTemperatureC())
                        .setTemperatureF(temperatureEvent.getTemperatureF())
                        .build();
            }
            case CLIMATE_SENSOR -> {
                var climateEvent = event.getClimateSensor();

                return ClimateSensorAvro.newBuilder()
                        .setCo2Level(climateEvent.getCo2Level())
                        .setHumidity(climateEvent.getHumidity())
                        .setTemperatureC(climateEvent.getTemperatureC())
                        .build();
            }
            case LIGHT_SENSOR -> {
                var lightEvent = event.getLightSensor();

                return LightSensorAvro.newBuilder()
                        .setLinkQuality(lightEvent.getLinkQuality())
                        .setLuminosity(lightEvent.getLuminosity())
                        .build();
            }
            case MOTION_SENSOR -> {
                var motionEvent = event.getMotionSensor();

                return MotionSensorAvro.newBuilder()
                        .setLinkQuality(motionEvent.getLinkQuality())
                        .setMotion(motionEvent.getMotion())
                        .setVoltage(motionEvent.getVoltage())
                        .build();
            }
            case SWITCH_SENSOR -> {
                var switchEvent = event.getSwitchSensor();

                return SwitchSensorAvro.newBuilder()
                        .setState(switchEvent.getState())
                        .build();
            }
            default -> throw new IllegalArgumentException("Unknown sensor event: " + event);
        }
    }
}
