package ru.yandex.practicum.order.feign;

public record InventoryRequest(
        Long productId,
        Integer quantity
) {
}