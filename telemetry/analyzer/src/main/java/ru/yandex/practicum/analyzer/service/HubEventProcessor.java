package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.config.KafkaAnalyzerProperties;
import ru.yandex.practicum.analyzer.entity.Action;
import ru.yandex.practicum.analyzer.entity.Condition;
import ru.yandex.practicum.analyzer.entity.Scenario;
import ru.yandex.practicum.analyzer.entity.ScenarioAction;
import ru.yandex.practicum.analyzer.entity.ScenarioActionId;
import ru.yandex.practicum.analyzer.entity.ScenarioCondition;
import ru.yandex.practicum.analyzer.entity.ScenarioConditionId;
import ru.yandex.practicum.analyzer.entity.Sensor;
import ru.yandex.practicum.analyzer.repository.ActionRepository;
import ru.yandex.practicum.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioActionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.analyzer.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final KafkaConsumer<String, HubEventAvro> hubEventConsumer;
    private final KafkaAnalyzerProperties kafkaProperties;
    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;

    @Override
    public void run() {
        try {
            hubEventConsumer.subscribe(Collections.singleton(kafkaProperties.getTopics().getHubs()));
            log.info("HubEventProcessor subscribed to topic: {}", kafkaProperties.getTopics().getHubs());

            while (true) {
                ConsumerRecords<String, HubEventAvro> records = hubEventConsumer.poll(
                        Duration.ofMillis(kafkaProperties.getPollTimeoutMs()));

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    HubEventAvro event = record.value();
                    String hubId = event.getHubId();
                    log.debug("Processing hub event from hub: {}", hubId);

                    switch (event.getPayload()) {
                        case DeviceAddedEventAvro deviceAdded -> handleDeviceAdded(hubId, deviceAdded);
                        case DeviceRemovedEventAvro deviceRemoved -> handleDeviceRemoved(hubId, deviceRemoved);
                        case ScenarioAddedEventAvro scenarioAdded -> handleScenarioAdded(hubId, scenarioAdded);
                        case ScenarioRemovedEventAvro scenarioRemoved -> handleScenarioRemoved(hubId, scenarioRemoved);
                        default -> log.warn("Unknown hub event type: {}", event.getPayload().getClass().getSimpleName());
                    }
                }
            }
        } catch (WakeupException ignored) {
            log.info("HubEventProcessor shutdown signal received");
        } catch (Exception e) {
            log.error("Error while processing hub events", e);
        } finally {
            hubEventConsumer.close();
        }
    }

    private void handleDeviceAdded(String hubId, DeviceAddedEventAvro event) {
        String sensorId = event.getId();
        if (sensorRepository.existsById(sensorId)) {
            log.debug("Sensor {} already exists, skipping", sensorId);
            return;
        }
        Sensor sensor = new Sensor(sensorId, hubId);
        sensorRepository.save(sensor);
        log.info("Sensor {} added for hub {}", sensorId, hubId);
    }

    private void handleDeviceRemoved(String hubId, DeviceRemovedEventAvro event) {
        String sensorId = event.getId();
        Optional<Sensor> sensor = sensorRepository.findByIdAndHubId(sensorId, hubId);
        if (sensor.isEmpty()) {
            log.debug("Sensor {} not found for hub {}, skipping removal", sensorId, hubId);
            return;
        }

        // Удаляем связи с условиями и действиями сценариев
        List<ScenarioCondition> conditions = scenarioConditionRepository.findAll().stream()
                .filter(sc -> sc.getSensor().getId().equals(sensorId))
                .toList();
        scenarioConditionRepository.deleteAll(conditions);

        List<ScenarioAction> actions = scenarioActionRepository.findAll().stream()
                .filter(sa -> sa.getSensor().getId().equals(sensorId))
                .toList();
        scenarioActionRepository.deleteAll(actions);

        // Удаляем сценарии, у которых не осталось условий
        List<Scenario> scenariosForHub = scenarioRepository.findByHubId(hubId);
        for (Scenario scenario : scenariosForHub) {
            List<ScenarioCondition> remainingConditions =
                    scenarioConditionRepository.findByScenarioId(scenario.getId());
            if (remainingConditions.isEmpty()) {
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

        // Удаляем старый сценарий если существует
        Optional<Scenario> existingScenario = scenarioRepository.findByHubIdAndName(hubId, scenarioName);
        if (existingScenario.isPresent()) {
            Long oldId = existingScenario.get().getId();
            scenarioConditionRepository.deleteByScenarioId(oldId);
            scenarioActionRepository.deleteByScenarioId(oldId);
            scenarioRepository.deleteById(oldId);
            log.debug("Deleted existing scenario '{}' for hub {}", scenarioName, hubId);
        }

        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(scenarioName);
        scenario = scenarioRepository.save(scenario);
        Long scenarioId = scenario.getId();

        // Сохраняем условия
        for (ScenarioConditionAvro conditionAvro : event.getConditions()) {
            Condition condition = new Condition();
            condition.setType(conditionAvro.getType().name());
            condition.setOperation(conditionAvro.getOperation().name());
            condition.setValue(extractConditionValue(conditionAvro));
            condition = conditionRepository.save(condition);

            Sensor sensor = sensorRepository.findById(conditionAvro.getSensorId()).orElse(null);
            if (sensor == null) {
                log.warn("Sensor {} not found for condition in scenario '{}'",
                        conditionAvro.getSensorId(), scenarioName);
                continue;
            }

            ScenarioConditionId scId = new ScenarioConditionId(
                    scenarioId, sensor.getId(), condition.getId());
            ScenarioCondition sc = new ScenarioCondition(scId, scenario, sensor, condition);
            scenarioConditionRepository.save(sc);
        }

        // Сохраняем действия
        for (DeviceActionAvro actionAvro : event.getActions()) {
            Action action = new Action();
            action.setType(actionAvro.getType().name());
            action.setValue(actionAvro.getValue());
            action = actionRepository.save(action);

            Sensor sensor = sensorRepository.findById(actionAvro.getSensorId()).orElse(null);
            if (sensor == null) {
                log.warn("Sensor {} not found for action in scenario '{}'",
                        actionAvro.getSensorId(), scenarioName);
                continue;
            }

            ScenarioActionId saId = new ScenarioActionId(
                    scenarioId, sensor.getId(), action.getId());
            ScenarioAction sa = new ScenarioAction(saId, scenario, sensor, action);
            scenarioActionRepository.save(sa);
        }

        log.info("Scenario '{}' added for hub {} with {} conditions, {} actions",
                scenarioName, hubId,
                event.getConditions().size(),
                event.getActions().size());
    }

    private void handleScenarioRemoved(String hubId, ScenarioRemovedEventAvro event) {
        String scenarioName = event.getName();
        Optional<Scenario> scenario = scenarioRepository.findByHubIdAndName(hubId, scenarioName);
        if (scenario.isEmpty()) {
            log.debug("Scenario '{}' not found for hub {}, skipping removal", scenarioName, hubId);
            return;
        }

        Long scenarioId = scenario.get().getId();
        scenarioConditionRepository.deleteByScenarioId(scenarioId);
        scenarioActionRepository.deleteByScenarioId(scenarioId);
        scenarioRepository.deleteById(scenarioId);
        log.info("Scenario '{}' removed from hub {}", scenarioName, hubId);
    }

    private Integer extractConditionValue(ScenarioConditionAvro conditionAvro) {
        Object val = conditionAvro.getValue();
        if (val == null) return null;
        if (val instanceof Integer intVal) return intVal;
        if (val instanceof Boolean boolVal) return boolVal ? 1 : 0;
        return null;
    }

    public void stop() {
        hubEventConsumer.wakeup();
    }
}
