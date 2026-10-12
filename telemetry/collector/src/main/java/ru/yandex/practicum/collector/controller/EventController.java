package ru.yandex.practicum.collector.controller;

import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.collector.kafka.KafkaEventService;
import ru.yandex.practicum.collector.dto.hub.HubEvent;
import ru.yandex.practicum.collector.dto.sensor.SensorEvent;

/**
 * API для передачи событий от датчиков и хабов
 */
@RestController
@RequestMapping("/events")
@AllArgsConstructor
public class EventController {

    private final KafkaEventService kafkaEventService;

    /**
     * Эндпоинт для обработки событий от датчиков
     * @param sensorEvent Данные события датчика (показания, изменение состояния и т.д)
     */
    @PostMapping("/sensors")
    public ResponseEntity<Void> handleSensorEvent(@Valid @RequestBody SensorEvent sensorEvent) {
        kafkaEventService.send(sensorEvent);
        return ResponseEntity.ok().build();
    }

    /**
     * Эндпоинт для обработки событий от хаба
     * @param hubEvent Данные события хаба (регистрация/удаление устройств в хабе, добавление/удаление сценария умного дома)
     */
    @PostMapping("/hubs")
    public ResponseEntity<Void> handleHubEvent(@Valid @RequestBody HubEvent hubEvent) {
        kafkaEventService.send(hubEvent);
        return ResponseEntity.ok().build();
    }
}
