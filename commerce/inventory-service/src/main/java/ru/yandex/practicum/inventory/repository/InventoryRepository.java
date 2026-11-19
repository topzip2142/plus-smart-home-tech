package ru.yandex.practicum.inventory.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.inventory.entity.InventoryItem;

import java.util.List;
import java.util.Optional;

public interface InventoryRepository extends JpaRepository<InventoryItem, Long> {

    Optional<InventoryItem> findByProductId(Long productId);

    boolean existsByProductId(Long productId);

    List<InventoryItem> findAllByOrderByProductIdAsc();
}
