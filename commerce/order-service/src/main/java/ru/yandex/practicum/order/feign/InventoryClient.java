package ru.yandex.practicum.order.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "inventory-service")
public interface InventoryClient {

    @PostMapping("/api/inventory/reserve")
    InventoryResponse reserveStock(@RequestBody InventoryRequest request);

    @PostMapping("/api/inventory/release")
    InventoryResponse releaseStock(@RequestBody InventoryRequest request);
}