package ru.yandex.practicum.analyzer.service;

import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.config.KafkaAnalyzerProperties;
import ru.yandex.practicum.analyzer.entity.*;
import ru.yandex.practicum.analyzer.repository.ScenarioActionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Component
public class SnapshotProcessor {

    private final KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer;
    private final KafkaAnalyzerProperties kafkaProperties;
    private final ScenarioRepository scenarioRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public SnapshotProcessor(
            KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer,
            KafkaAnalyzerProperties kafkaProperties,
            ScenarioRepository scenarioRepository,
            ScenarioConditionRepository scenarioConditionRepository,
            ScenarioActionRepository scenarioActionRepository,
            @GrpcClient("hub-router") HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient) {
        this.snapshotConsumer = snapshotConsumer;
        this.kafkaProperties = kafkaProperties;
        this.scenarioRepository = scenarioRepository;
        this.scenarioConditionRepository = scenarioConditionRepository;
        this.scenarioActionRepository = scenarioActionRepository;
        this.hubRouterClient = hubRouterClient;
    }

    public void start() {
        try {
            snapshotConsumer.subscribe(Collections.singleton(kafkaProperties.getTopics().getSnapshots()));
            log.info("SnapshotProcessor subscribed to topic: {}", kafkaProperties.getTopics().getSnapshots());

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = snapshotConsumer.poll(
                        Duration.ofMillis(kafkaProperties.getPollTimeoutMs()));

                Set<TopicPartition> failedPartitions = new HashSet<>();

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());

                    if (failedPartitions.contains(tp)) {
                        continue;
                    }

                    try {
                        SensorsSnapshotAvro snapshot = record.value();
                        log.debug("Processing snapshot from hub: {}", snapshot.getHubId());
                        processSnapshot(snapshot);

                        snapshotConsumer.commitSync(Collections.singletonMap(
                                tp, new OffsetAndMetadata(record.offset() + 1)));
                    } catch (WakeupException e) {
                        throw e;
                    } catch (Exception e) {
                        failedPartitions.add(tp);
                        log.error("Failed to process snapshot at offset {} (partition {}), " +
                                        "will retry after restart",
                                record.offset(), record.partition(), e);
                    }
                }
            }
        } catch (WakeupException ignored) {
            log.info("SnapshotProcessor shutdown signal received");
        } catch (Exception e) {
            log.error("Fatal error while processing snapshots", e);
        } finally {
            snapshotConsumer.close();
        }
    }

    private void processSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();

        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        if (scenarios.isEmpty()) {
            log.debug("No scenarios found for hub {}", hubId);
            return;
        }

        Map<Long, List<ScenarioCondition>> conditionsByScenario =
                scenarioConditionRepository.findByScenarioHubId(hubId).stream()
                        .collect(Collectors.groupingBy(sc -> sc.getScenario().getId()));
        Map<Long, List<ScenarioAction>> actionsByScenario =
                scenarioActionRepository.findByScenarioHubId(hubId).stream()
                        .collect(Collectors.groupingBy(sa -> sa.getScenario().getId()));

        for (Scenario scenario : scenarios) {
            List<ScenarioCondition> conditions =
                    conditionsByScenario.getOrDefault(scenario.getId(), List.of());
            List<ScenarioAction> actions =
                    actionsByScenario.getOrDefault(scenario.getId(), List.of());

            if (conditions.isEmpty() || actions.isEmpty()) {
                continue;
            }

            boolean allConditionsMet = conditions.stream()
                    .allMatch(sc -> evaluateCondition(sc, sensorsState));

            if (allConditionsMet) {
                log.info("Scenario '{}' conditions satisfied for hub {}", scenario.getName(), hubId);
                executeActions(hubId, scenario.getName(), actions, snapshot.getTimestamp().toEpochMilli());
            }
        }
    }

    private boolean evaluateCondition(ScenarioCondition sc, Map<String, SensorStateAvro> sensorsState) {
        String sensorId = sc.getSensor().getId();
        Condition condition = sc.getCondition();

        Integer refValue = condition.getValue();
        if (refValue == null) {
            log.warn("Condition value is null for sensor {} in scenario, skipping", sensorId);
            return false;
        }

        SensorStateAvro state = sensorsState.get(sensorId);
        if (state == null) {
            log.debug("Sensor {} not found in snapshot state", sensorId);
            return false;
        }

        Integer sensorValue = extractSensorValue(condition.getType(), state);
        if (sensorValue == null) {
            log.warn("Cannot extract value for sensor {} with condition type {}", sensorId, condition.getType());
            return false;
        }

        ConditionOperation operation = condition.getOperation();
        if (operation == null) {
            log.warn("Condition operation is null for sensor {}, skipping", sensorId);
            return false;
        }

        int ref = refValue;
        return switch (operation) {
            case GREATER_THAN -> sensorValue > ref;
            case LOWER_THAN -> sensorValue < ref;
            case EQUALS -> sensorValue == ref;
        };
    }

    private Integer extractSensorValue(ConditionType type, SensorStateAvro state) {
        if (type == null) {
            return null;
        }
        return switch (type) {
            case TEMPERATURE -> {
                if (state.getData() instanceof TemperatureSensorAvro temp) {
                    yield temp.getTemperatureC();
                } else if (state.getData() instanceof ClimateSensorAvro climate) {
                    yield climate.getTemperatureC();
                }
                yield null;
            }
            case HUMIDITY -> {
                if (state.getData() instanceof ClimateSensorAvro climate) {
                    yield climate.getHumidity();
                }
                yield null;
            }
            case CO2LEVEL -> {
                if (state.getData() instanceof ClimateSensorAvro climate) {
                    yield climate.getCo2Level();
                }
                yield null;
            }
            case LUMINOSITY -> {
                if (state.getData() instanceof LightSensorAvro light) {
                    yield light.getLuminosity();
                }
                yield null;
            }
            case MOTION -> {
                if (state.getData() instanceof MotionSensorAvro motion) {
                    yield motion.getMotion() ? 1 : 0;
                }
                yield null;
            }
            case SWITCH -> {
                if (state.getData() instanceof SwitchSensorAvro sw) {
                    yield sw.getState() ? 1 : 0;
                }
                yield null;
            }
        };
    }

    private void executeActions(String hubId, String scenarioName, List<ScenarioAction> actions, long snapshotTimestamp) {
        for (ScenarioAction sa : actions) {
            Action entityAction = sa.getAction();
            ActionType actionType = entityAction.getType();
            if (actionType == null) {
                log.warn("Action type is null for sensor {} in scenario '{}', skipping",
                        sa.getSensor().getId(), scenarioName);
                continue;
            }

            ActionTypeProto actionTypeProto = switch (actionType) {
                case ACTIVATE -> ActionTypeProto.ACTIVATE;
                case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
                case INVERSE -> ActionTypeProto.INVERSE;
                case SET_VALUE -> ActionTypeProto.SET_VALUE;
            };

            DeviceActionProto.Builder builder = DeviceActionProto.newBuilder()
                    .setSensorId(sa.getSensor().getId())
                    .setType(actionTypeProto);
            if (entityAction.getValue() != null) {
                builder.setValue(entityAction.getValue());
            }
            DeviceActionProto actionProto = builder.build();

            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(hubId)
                    .setScenarioName(scenarioName)
                    .setAction(actionProto)
                    .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                            .setSeconds(snapshotTimestamp / 1000)
                            .build())
                    .build();

            try {
                hubRouterClient.handleDeviceAction(request);
                log.info("Sent action for sensor {} in scenario '{}' for hub {}",
                        sa.getSensor().getId(), scenarioName, hubId);
            } catch (Exception e) {
                log.error("Failed to send action for sensor {} in scenario '{}'",
                        sa.getSensor().getId(), scenarioName, e);
            }
        }
    }

    public void stop() {
        snapshotConsumer.wakeup();
    }
}