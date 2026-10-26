package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.config.KafkaAnalyzerProperties;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final KafkaConsumer<String, HubEventAvro> hubEventConsumer;
    private final KafkaAnalyzerProperties kafkaProperties;
    private final HubEventHandler hubEventHandler;

    @Override
    public void run() {
        try {
            hubEventConsumer.subscribe(Collections.singleton(kafkaProperties.getTopics().getHubs()));
            log.info("HubEventProcessor subscribed to topic: {}", kafkaProperties.getTopics().getHubs());

            while (true) {
                ConsumerRecords<String, HubEventAvro> records = hubEventConsumer.poll(
                        Duration.ofMillis(kafkaProperties.getPollTimeoutMs()));

                Set<TopicPartition> failedPartitions = new HashSet<>();

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());

                    if (failedPartitions.contains(tp)) {
                        continue;
                    }

                    try {
                        HubEventAvro event = record.value();
                        log.debug("Processing hub event from hub: {}", event.getHubId());
                        hubEventHandler.handle(event.getHubId(), event.getPayload());

                        hubEventConsumer.commitSync(Collections.singletonMap(
                                tp, new OffsetAndMetadata(record.offset() + 1)));
                    } catch (WakeupException e) {
                        throw e;
                    } catch (Exception e) {
                        failedPartitions.add(tp);
                        log.error("Failed to process hub event at offset {} (partition {}), " +
                                        "will retry after restart",
                                record.offset(), record.partition(), e);
                    }
                }
            }
        } catch (WakeupException ignored) {
            log.info("HubEventProcessor shutdown signal received");
        } catch (Exception e) {
            log.error("Fatal error while processing hub events", e);
        } finally {
            hubEventConsumer.close();
        }
    }

    public void stop() {
        hubEventConsumer.wakeup();
    }
}