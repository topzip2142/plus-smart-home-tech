package ru.yandex.practicum.collector.mapper;

import com.google.protobuf.Timestamp;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class HubEventMapper {

    public static HubEventAvro toAvro(HubEventProto event) {
        Timestamp ts = event.getTimestamp();

        HubEventAvro.Builder b = HubEventAvro.newBuilder();
        b.setHubId(event.getHubId());
        b.setTimestamp(Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos()));
        b.setPayload(getPayload(event));
        return b.build();
    }

    private static Object getPayload(HubEventProto e) {
        return switch (e.getPayloadCase()) {
            case DEVICE_ADDED_EVENT -> {
                var x = e.getDeviceAddedEvent();
                yield DeviceAddedEventAvro.newBuilder()
                        .setId(x.getId())
                        .setType(DeviceTypeAvro.valueOf(x.getType().name()))
                        .build();
            }
            case DEVICE_REMOVED_EVENT -> {
                var x = e.getDeviceRemovedEvent();
                yield DeviceRemovedEventAvro.newBuilder()
                        .setId(x.getId())
                        .build();
            }
            case SCENARIO_ADDED_EVENT -> {
                var x = e.getScenarioAddedEvent();
                yield ScenarioAddedEventAvro.newBuilder()
                        .setName(x.getName())
                        .setConditions(x.getConditionsList().stream()
                                .map(HubEventMapper::toCondition)
                                .toList())
                        .setActions(x.getActionsList().stream()
                                .map(HubEventMapper::toAction)
                                .toList())
                        .build();
            }
            case SCENARIO_REMOVED_EVENT -> {
                var x = e.getScenarioRemovedEvent();
                yield ScenarioRemovedEventAvro.newBuilder()
                        .setName(x.getName())
                        .build();
            }
            default -> throw new IllegalArgumentException(
                    "Unknown hub event payload: " + e.getPayloadCase());
        };
    }

    private static ScenarioConditionAvro toCondition(ScenarioConditionProto c) {
        ScenarioConditionAvro.Builder b = ScenarioConditionAvro.newBuilder();
        b.setSensorId(c.getSensorId());
        b.setType(ConditionTypeAvro.valueOf(c.getType().name()));
        b.setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()));
        b.setValue(extractConditionValue(c));
        return b.build();
    }

    private static DeviceActionAvro toAction(DeviceActionProto d) {
        DeviceActionAvro.Builder b = DeviceActionAvro.newBuilder();
        b.setSensorId(d.getSensorId());
        b.setType(ActionTypeAvro.valueOf(d.getType().name()));
        b.setValue(d.getValue());
        return b.build();
    }

    private static Object extractConditionValue(ScenarioConditionProto c) {
        return switch (c.getValueCase()) {
            case INT_VALUE     -> c.getIntValue();
            case BOOL_VALUE    -> c.getBoolValue();
            case VALUE_NOT_SET -> null;
        };
    }
}

