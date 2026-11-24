package ru.yandex.practicum.order.feign;

import ru.yandex.practicum.order.breaker.InventoryClientFallbackFactory;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "inventory-service", fallbackFactory = InventoryClientFallbackFactory.class)
public interface InventoryClient {

    @PostMapping("/api/inventory/reserve")
    InventoryResponse reserve(@RequestBody InventoryRequest request);

    @PostMapping("/api/inventory/release")
    InventoryResponse release(@RequestBody InventoryRequest request);
}