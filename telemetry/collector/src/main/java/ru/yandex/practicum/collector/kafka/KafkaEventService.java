package ru.yandex.practicum.collector.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.mapper.HubEventMapper;
import ru.yandex.practicum.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.collector.dto.hub.HubEvent;
import ru.yandex.practicum.collector.dto.sensor.SensorEvent;

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

    public void send(SensorEvent e) {
        producer.send(new ProducerRecord<>(sensorsTopic, e.getHubId(), SensorEventMapper.toAvro(e)));
    }
    public void send(HubEvent e) {
        producer.send(new ProducerRecord<>(hubsTopic, e.getHubId(), HubEventMapper.toAvro(e)));
    }
}
