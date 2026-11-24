package ru.yandex.practicum.inventory.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.inventory.dto.InventoryDto;
import ru.yandex.practicum.inventory.dto.ReserveRequest;
import ru.yandex.practicum.inventory.dto.ReserveResponse;
import ru.yandex.practicum.inventory.dto.UpdateInventoryRequest;
import ru.yandex.practicum.inventory.entity.InventoryItem;
import ru.yandex.practicum.inventory.exception.AlreadyExistsException;
import ru.yandex.practicum.inventory.exception.NotFoundException;
import ru.yandex.practicum.inventory.repository.InventoryRepository;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class InventoryService {

    private final InventoryRepository inventoryRepository;

    public List<InventoryDto> findAll() {
        return inventoryRepository.findAllByOrderByProductIdAsc().stream()
                .map(InventoryService::toDto)
                .toList();
    }

    public InventoryDto findByProductId(Long productId) {
        return toDto(getItem(productId));
    }

    @Transactional
    public InventoryDto create(UpdateInventoryRequest request) {
        if (inventoryRepository.existsByProductId(request.productId())) {
            throw new AlreadyExistsException("Складская запись для товара productId="
                    + request.productId() + " уже существует");
        }
        InventoryItem saved = inventoryRepository.save(new InventoryItem(request.productId(), request.quantity()));
        log.info("Создана складская запись productId={}, quantity={}", saved.getProductId(), saved.getQuantity());
        return toDto(saved);
    }

    @Transactional
    public InventoryDto updateQuantity(UpdateInventoryRequest request) {
        InventoryItem item = getItem(request.productId());
        item.changeQuantity(request.quantity());
        log.info("Обновлено количество productId={}: quantity={}", item.getProductId(), item.getQuantity());
        return toDto(item);
    }

    @Transactional
    public ReserveResponse reserve(ReserveRequest request) {
        InventoryItem item = getItem(request.productId());
        item.reserve(request.quantity());
        log.info("Зарезервировано {} шт. товара productId={}, доступно {}",
                request.quantity(), item.getProductId(), item.getAvailableQuantity());
        return new ReserveResponse(true, item.getAvailableQuantity(), "Товар успешно зарезервирован");
    }

    @Transactional
    public ReserveResponse release(ReserveRequest request) {
        log.debug("Возвращение товара с id {}", request.productId());
        InventoryItem item = getItem(request.productId());
        if (item.getReservedQuantity() < request.quantity()) {
            log.warn("Нельзя отменить резервирование товара с id {}: зарезервировано {}, к отмене {}",
                    item.getProductId(), item.getReservedQuantity(), request.quantity());
            throw new IllegalArgumentException("Нельзя отменить резервирование товара c id " + item.getProductId());
        }
        item.changeQuantity(item.getReservedQuantity() - request.quantity());
        InventoryDto dto = toDto(inventoryRepository.save(item));
        log.info("Отмена резервирования товара с id {}", dto.productId());
        return new ReserveResponse(true, dto.availableQuantity(), "Отмена резервирования, - успешна");
    }

    private InventoryItem getItem(Long productId) {
        return inventoryRepository.findByProductId(productId)
                .orElseThrow(() -> new NotFoundException(
                        "Складская запись для товара productId=" + productId + " не найдена"));
    }

    private static InventoryDto toDto(InventoryItem item) {
        return new InventoryDto(
                item.getId(),
                item.getProductId(),
                item.getQuantity(),
                item.getReservedQuantity(),
                item.getAvailableQuantity()
        );
    }
}
