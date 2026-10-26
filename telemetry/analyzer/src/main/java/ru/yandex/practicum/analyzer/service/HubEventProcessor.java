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

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    try {
                        HubEventAvro event = record.value();
                        log.debug("Processing hub event from hub: {}", event.getHubId());
                        hubEventHandler.handle(event.getHubId(), event.getPayload());

                        hubEventConsumer.commitSync(Collections.singletonMap(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)));
                    } catch (Exception e) {
                        log.error("Failed to process hub event at offset {}: {}",
                                record.offset(), record.value(), e);
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

    public void stop() {
        hubEventConsumer.wakeup();
    }
}