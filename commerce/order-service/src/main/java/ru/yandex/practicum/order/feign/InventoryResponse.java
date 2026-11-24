package ru.yandex.practicum.order.feign;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record InventoryResponse(
        boolean success,
        Integer availableQuantity,
        String message
) {
}