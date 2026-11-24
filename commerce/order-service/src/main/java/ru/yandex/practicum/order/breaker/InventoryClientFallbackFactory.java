package ru.yandex.practicum.order.breaker;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.order.exception.InventoryServiceUnavailableException;
import ru.yandex.practicum.order.feign.InventoryClient;
import ru.yandex.practicum.order.feign.InventoryRequest;
import ru.yandex.practicum.order.feign.InventoryResponse;

@Component
@Slf4j
public class InventoryClientFallbackFactory implements FallbackFactory<InventoryClient> {

    @Override
    public InventoryClient create(Throwable cause) {
        return new InventoryClient() {

            @Override
            public InventoryResponse reserve(InventoryRequest request) {
                log.warn("inventory-service недоступен при резервировании товара id = {}", request.productId(), cause);
                throw new InventoryServiceUnavailableException(request.productId(), cause);
            }

            @Override
            public InventoryResponse release(InventoryRequest request) {
                log.warn("inventory-service недоступен при снятии резерва товара id = {}", request.productId(), cause);
                throw new InventoryServiceUnavailableException(request.productId(), cause);
            }
        };
    }
}