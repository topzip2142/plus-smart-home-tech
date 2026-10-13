package ru.yandex.practicum.collector.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.dto.hub.HubEvent;
import ru.yandex.practicum.collector.dto.sensor.SensorEvent;
import ru.yandex.practicum.collector.mapper.HubEventMapper;
import ru.yandex.practicum.collector.mapper.SensorEventMapper;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
@Service
public class KafkaEventService {

    private final Producer<String, SpecificRecordBase> producer;
    private final String hubsTopic;
    private final String sensorsTopic;

    public KafkaEventService(
            Producer<String, SpecificRecordBase> producer,
            @Value("${collector.kafka.topics.hubs}") String hubsTopic,
            @Value("${collector.kafka.topics.sensors}") String sensorsTopic) {
        this.producer = producer;
        this.hubsTopic = hubsTopic;
        this.sensorsTopic = sensorsTopic;
    }

    public void send(SensorEvent event) {
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                sensorsTopic,
                null,
                event.getTimestamp().toEpochMilli(),
                event.getHubId(),
                SensorEventMapper.toAvro(event)
        );
        sendRecord(record, event.getClass().getSimpleName());
    }

    public void send(HubEvent event) {
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                hubsTopic,
                null,
                event.getTimestamp().toEpochMilli(),
                event.getHubId(),
                HubEventMapper.toAvro(event)
        );
        sendRecord(record, event.getClass().getSimpleName());
    }

    private void sendRecord(ProducerRecord<String, SpecificRecordBase> record, String eventClass) {
        Future<RecordMetadata> future = producer.send(record);
        producer.flush();
        try {
            RecordMetadata metadata = future.get();
            log.info("Событие {} успешно сохранено в топик {} партиция {} смещение {}",
                    eventClass, metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Отправка события {} в топик {} была прервана", eventClass, record.topic(), e);
        } catch (ExecutionException e) {
            log.warn("Не удалось записать событие {} в топик {}", eventClass, record.topic(), e);
        }
    }
}