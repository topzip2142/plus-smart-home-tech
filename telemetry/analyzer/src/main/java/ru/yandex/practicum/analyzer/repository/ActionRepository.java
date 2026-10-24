package ru.yandex.practicum.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.analyzer.entity.Action;

public interface ActionRepository extends JpaRepository<Action, Long> {
}
