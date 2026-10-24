package ru.yandex.practicum.analyzer.entity;

import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Embeddable
@AllArgsConstructor
@NoArgsConstructor
public class ScenarioActionId implements Serializable {

    private Long scenarioId;
    private String sensorId;
    private Long actionId;
}
