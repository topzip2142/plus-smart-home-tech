package ru.yandex.practicum.analyzer.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "sensors")
@AllArgsConstructor
@NoArgsConstructor
public class Sensor {

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "hub_id")
    private String hubId;
}
