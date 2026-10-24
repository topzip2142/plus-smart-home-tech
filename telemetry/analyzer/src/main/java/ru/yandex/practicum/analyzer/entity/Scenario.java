package ru.yandex.practicum.analyzer.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "scenarios",
        uniqueConstraints = @UniqueConstraint(columnNames = {"hub_id", "name"}))
@AllArgsConstructor
@NoArgsConstructor
public class Scenario {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "hub_id")
    private String hubId;

    @Column(name = "name")
    private String name;
}
