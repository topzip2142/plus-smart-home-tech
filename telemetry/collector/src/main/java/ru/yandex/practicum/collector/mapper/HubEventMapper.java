package ru.yandex.practicum.collector.mapper;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.collector.dto.hub.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class HubEventMapper {

    public static HubEventAvro toAvro(HubEvent hubEvent) {
        HubEventAvro.Builder builder = HubEventAvro.newBuilder();
        builder.setHubId(hubEvent.getHubId());
        builder.setTimestamp(hubEvent.getTimestamp());
        builder.setPayload(getPayload(hubEvent));
        return builder.build();
    }

    private static Object getPayload(HubEvent e) {
        if (e instanceof DeviceAddedEvent x)
            return DeviceAddedEventAvro.newBuilder()
                    .setId(x.getId())
                    .setType(DeviceTypeAvro.valueOf(x.getDeviceType().name()))
                    .build();
        if (e instanceof DeviceRemovedEvent x)
            return DeviceRemovedEventAvro.newBuilder().setId(x.getId()).build();
        if (e instanceof ScenarioAddedEvent x)
            return ScenarioAddedEventAvro.newBuilder()
                    .setName(x.getName())
                    .setConditions(x.getConditions().stream().map(HubEventMapper::toCondition).toList())
                    .setActions(x.getActions().stream().map(HubEventMapper::toAction).toList())
                    .build();
        if (e instanceof ScenarioRemovedEvent x)
            return ScenarioRemovedEventAvro.newBuilder().setName(x.getName()).build();
        throw new IllegalArgumentException("Unknown hub event: " + e);
    }

    private static ScenarioConditionAvro toCondition(ScenarioCondition c) {
        ScenarioConditionAvro.Builder b = ScenarioConditionAvro.newBuilder();
        b.setSensorId(c.getSensorId());
        b.setType(ConditionTypeAvro.valueOf(c.getType().name()));
        b.setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()));
        b.setValue(c.getValue());
        return b.build();
    }

    private static DeviceActionAvro toAction(DeviceAction d) {
        DeviceActionAvro.Builder b = DeviceActionAvro.newBuilder();
        b.setSensorId(d.getSensorId());
        b.setType(ActionTypeAvro.valueOf(d.getType().name()));
        b.setValue(d.getValue());
        return b.build();
    }
}
