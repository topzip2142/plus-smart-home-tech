package ru.yandex.practicum.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.aggregator.config.KafkaProperties;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private final KafkaConsumer<String, SensorEventAvro> consumer;
    private final KafkaProducer<String, SensorsSnapshotAvro> producer;
    private final SnapshotService snapshotService;
    private final KafkaProperties kafkaProperties;

    public void start() {
        try {
            consumer.subscribe(Collections.singleton(kafkaProperties.getTopics().getSensors()));
            log.info("Subscribed to topic {}", kafkaProperties.getTopics().getSensors());

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(
                        Duration.ofMillis(kafkaProperties.getPollTimeoutMs()));
                log.info("Polled {} records from Kafka", records.count());

                for (ConsumerRecord<String, SensorEventAvro> consumerRecord : records) {
                    SensorEventAvro event = consumerRecord.value();
                    log.debug("Processing event from sensor {} of hub {}", event.getId(), event.getHubId());

                    Optional<SensorsSnapshotAvro> updated = snapshotService.updateState(event);
                    updated.ifPresent(snapshot -> {
                        ProducerRecord<String, SensorsSnapshotAvro> producerRecord = new ProducerRecord<>(
                                kafkaProperties.getTopics().getSnapshots(),
                                snapshot.getHubId(),
                                snapshot
                        );
                        producer.send(producerRecord, (metadata, exception) -> {
                            if (exception != null) {
                                log.error("Failed to send snapshot for hub {} to topic {}",
                                        snapshot.getHubId(), kafkaProperties.getTopics().getSnapshots(), exception);
                            } else {
                                log.debug("Snapshot for hub {} sent to {} [partition={}, offset={}]",
                                        snapshot.getHubId(), metadata.topic(), metadata.partition(), metadata.offset());
                            }
                        });
                    });
                }

                if (!records.isEmpty()) {
                    producer.flush();
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    for (ConsumerRecord<String, SensorEventAvro> consumerRecord : records) {
                        offsets.put(
                                new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                                new OffsetAndMetadata(consumerRecord.offset() + 1)
                        );
                    }
                    consumer.commitSync(offsets);
                    log.debug("Committed {} offsets", offsets.size());
                }
            }
        } catch (WakeupException ignored) {
            log.info("Shutdown signal received");
        } catch (Exception e) {
            log.error("Error while processing sensor events", e);
        } finally {
            try {
                producer.flush();
                consumer.commitSync();
            } finally {
                consumer.close();
                producer.close();
            }
        }
    }
}
