package ru.yandex.practicum.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.yandex.practicum.analyzer.entity.ScenarioCondition;
import ru.yandex.practicum.analyzer.entity.ScenarioConditionId;

import java.util.List;

public interface ScenarioConditionRepository
        extends JpaRepository<ScenarioCondition, ScenarioConditionId> {

    @Query("select sc from ScenarioCondition sc " +
            "join fetch sc.scenario " +
            "join fetch sc.sensor " +
            "join fetch sc.condition " +
            "where sc.scenario.id = :scenarioId")
    List<ScenarioCondition> findByScenarioId(@Param("scenarioId") Long scenarioId);

    @Query("select sc from ScenarioCondition sc " +
            "join fetch sc.scenario " +
            "join fetch sc.sensor " +
            "join fetch sc.condition " +
            "where sc.sensor.id = :sensorId")
    List<ScenarioCondition> findBySensorId(@Param("sensorId") String sensorId);

    @Query("select sc from ScenarioCondition sc " +
            "join fetch sc.scenario " +
            "join fetch sc.sensor " +
            "join fetch sc.condition " +
            "where sc.scenario.hubId = :hubId")
    List<ScenarioCondition> findByScenarioHubId(@Param("hubId") String hubId);

    @Modifying
    @Query("delete from ScenarioCondition sc where sc.scenario.id = :scenarioId")
    void deleteByScenarioId(@Param("scenarioId") Long scenarioId);
}