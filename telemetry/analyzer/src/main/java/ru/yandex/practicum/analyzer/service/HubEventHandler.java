package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.entity.*;
import ru.yandex.practicum.analyzer.repository.ActionRepository;
import ru.yandex.practicum.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioActionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.analyzer.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

import java.util.List;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventHandler {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;

    @Transactional
    public void handle(String hubId, Object payload) {
        switch (payload) {
            case DeviceAddedEventAvro deviceAdded -> handleDeviceAdded(hubId, deviceAdded);
            case DeviceRemovedEventAvro deviceRemoved -> handleDeviceRemoved(hubId, deviceRemoved);
            case ScenarioAddedEventAvro scenarioAdded -> handleScenarioAdded(hubId, scenarioAdded);
            case ScenarioRemovedEventAvro scenarioRemoved -> handleScenarioRemoved(hubId, scenarioRemoved);
            default -> log.warn("Unknown hub event type: {}",
                    payload.getClass().getSimpleName());
        }
    }

    private void handleDeviceAdded(String hubId, DeviceAddedEventAvro event) {
        String sensorId = event.getId();
        if (sensorRepository.existsById(sensorId)) {
            log.debug("Sensor {} already exists, skipping", sensorId);
            return;
        }
        sensorRepository.save(new Sensor(sensorId, hubId));
        log.info("Sensor {} added for hub {}", sensorId, hubId);
    }

    private void handleDeviceRemoved(String hubId, DeviceRemovedEventAvro event) {
        String sensorId = event.getId();
        Optional<Sensor> sensor = sensorRepository.findByIdAndHubId(sensorId, hubId);
        if (sensor.isEmpty()) {
            log.debug("Sensor {} not found for hub {}, skipping removal", sensorId, hubId);
            return;
        }

        List<ScenarioCondition> conditions = scenarioConditionRepository.findAll().stream()
                .filter(sc -> sc.getSensor().getId().equals(sensorId))
                .toList();
        scenarioConditionRepository.deleteAll(conditions);

        List<ScenarioAction> actions = scenarioActionRepository.findAll().stream()
                .filter(sa -> sa.getSensor().getId().equals(sensorId))
                .toList();
        scenarioActionRepository.deleteAll(actions);

        for (Scenario scenario : scenarioRepository.findByHubId(hubId)) {
            if (scenarioConditionRepository.findByScenarioId(scenario.getId()).isEmpty()) {
                scenarioActionRepository.deleteByScenarioId(scenario.getId());
                scenarioRepository.delete(scenario);
                log.info("Scenario {} deleted (no conditions left)", scenario.getName());
            }
        }

        sensorRepository.delete(sensor.get());
        log.info("Sensor {} removed from hub {}", sensorId, hubId);
    }

    private void handleScenarioAdded(String hubId, ScenarioAddedEventAvro event) {
        String scenarioName = event.getName();

        scenarioRepository.findByHubIdAndName(hubId, scenarioName).ifPresent(old -> {
            Long oldId = old.getId();
            scenarioConditionRepository.deleteByScenarioId(oldId);
            scenarioActionRepository.deleteByScenarioId(oldId);
            scenarioRepository.deleteById(oldId);
            log.debug("Deleted existing scenario '{}' for hub {}", scenarioName, hubId);
        });

        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(scenarioName);
        scenario = scenarioRepository.save(scenario);
        Long scenarioId = scenario.getId();

        for (ScenarioConditionAvro conditionAvro : event.getConditions()) {
            ConditionType conditionType;
            try {
                conditionType = ConditionType.valueOf(conditionAvro.getType().name());
            } catch (IllegalArgumentException e) {
                log.warn("Unknown condition type {} in scenario '{}', skipping",
                        conditionAvro.getType(), scenarioName);
                continue;
            }

            Sensor sensor = sensorRepository
                    .findByIdAndHubId(conditionAvro.getSensorId(), hubId)
                    .orElse(null);
            if (sensor == null) {
                log.warn("Sensor {} not found for condition in scenario '{}'",
                        conditionAvro.getSensorId(), scenarioName);
                continue;
            }

            Condition condition = new Condition();
            condition.setType(conditionType);
            condition.setOperation(conditionAvro.getOperation().name());
            condition.setValue(extractConditionValue(conditionAvro));
            condition = conditionRepository.save(condition);

            ScenarioConditionId scId = new ScenarioConditionId(
                    scenarioId, sensor.getId(), condition.getId());
            scenarioConditionRepository.save(
                    new ScenarioCondition(scId, scenario, sensor, condition));
        }

        for (DeviceActionAvro actionAvro : event.getActions()) {
            Sensor sensor = sensorRepository
                    .findByIdAndHubId(actionAvro.getSensorId(), hubId)
                    .orElse(null);
            if (sensor == null) {
                log.warn("Sensor {} not found for action in scenario '{}'",
                        actionAvro.getSensorId(), scenarioName);
                continue;
            }

            Action action = new Action();
            action.setType(actionAvro.getType().name());
            action.setValue(actionAvro.getValue());
            action = actionRepository.save(action);

            ScenarioActionId saId = new ScenarioActionId(
                    scenarioId, sensor.getId(), action.getId());
            scenarioActionRepository.save(
                    new ScenarioAction(saId, scenario, sensor, action));
        }

        log.info("Scenario '{}' added for hub {} with {} conditions, {} actions",
                scenarioName, hubId,
                event.getConditions().size(), event.getActions().size());
    }

    private void handleScenarioRemoved(String hubId, ScenarioRemovedEventAvro event) {
        String scenarioName = event.getName();
        scenarioRepository.findByHubIdAndName(hubId, scenarioName).ifPresentOrElse(scenario -> {
            Long scenarioId = scenario.getId();
            scenarioConditionRepository.deleteByScenarioId(scenarioId);
            scenarioActionRepository.deleteByScenarioId(scenarioId);
            scenarioRepository.deleteById(scenarioId);
            log.info("Scenario '{}' removed from hub {}", scenarioName, hubId);
        }, () -> log.debug("Scenario '{}' not found for hub {}, skipping removal",
                scenarioName, hubId));
    }

    private Integer extractConditionValue(ScenarioConditionAvro conditionAvro) {
        Object val = conditionAvro.getValue();
        if (val == null) return null;
        if (val instanceof Integer intVal) return intVal;
        if (val instanceof Boolean boolVal) return boolVal ? 1 : 0;
        return null;
    }
}