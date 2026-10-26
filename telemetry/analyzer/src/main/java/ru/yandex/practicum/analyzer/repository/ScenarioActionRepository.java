package ru.yandex.practicum.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.yandex.practicum.analyzer.entity.ScenarioAction;
import ru.yandex.practicum.analyzer.entity.ScenarioActionId;

import java.util.List;

public interface ScenarioActionRepository
        extends JpaRepository<ScenarioAction, ScenarioActionId> {

    @Query("select sa from ScenarioAction sa " +
            "join fetch sa.scenario " +
            "join fetch sa.sensor " +
            "join fetch sa.action " +
            "where sa.scenario.id = :scenarioId")
    List<ScenarioAction> findByScenarioId(@Param("scenarioId") Long scenarioId);

    @Query("select sa from ScenarioAction sa " +
            "join fetch sa.scenario " +
            "join fetch sa.sensor " +
            "join fetch sa.action " +
            "where sa.sensor.id = :sensorId")
    List<ScenarioAction> findBySensorId(@Param("sensorId") String sensorId);

    @Query("select sa from ScenarioAction sa " +
            "join fetch sa.scenario " +
            "join fetch sa.sensor " +
            "join fetch sa.action " +
            "where sa.scenario.hubId = :hubId")
    List<ScenarioAction> findByScenarioHubId(@Param("hubId") String hubId);

    @Modifying
    @Query("delete from ScenarioAction sa where sa.scenario.id = :scenarioId")
    void deleteByScenarioId(@Param("scenarioId") Long scenarioId);
}