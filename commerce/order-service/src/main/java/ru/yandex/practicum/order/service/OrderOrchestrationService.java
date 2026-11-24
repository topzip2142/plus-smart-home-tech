package ru.yandex.practicum.order.service;

import feign.FeignException;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemRequest;
import ru.yandex.practicum.order.exception.OrderProcessingException;
import ru.yandex.practicum.order.feign.InventoryClient;
import ru.yandex.practicum.order.feign.ProductClient;
import ru.yandex.practicum.order.feign.ProductDto;
import ru.yandex.practicum.order.feign.InventoryRequest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

@Service
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class OrderOrchestrationService {
    ProductClient productClient;
    InventoryClient inventoryClient;
    OrderService service;

    private ProductDto getProduct(Long productId) {
        try {
            return productClient.getProductById(productId);
        } catch (FeignException.NotFound exception) {
            log.warn("Товар с id {} не найден", productId);
            throw new OrderProcessingException(format("Товар с id %d не найден", productId));
        } catch (FeignException exception) {
            log.warn("Не удалось получить данные товара c id {}", productId);
            throw new OrderProcessingException(format("Не удалось получить данные товара c id %d", productId));
        }
    }

    private void getReserve(InventoryRequest request) {
        try {
            inventoryClient.reserve(request);
        } catch (FeignException.NotFound exception) {
            log.warn("Складская запись для товара с id {} не найдена", request.productId());
            throw new OrderProcessingException(format("Складская запись для товара с id %d не найдена",
                    request.productId()));
        } catch (FeignException.Conflict exception) {
            log.warn("Для заказа недостаточно товара с id {}", request.productId());
            throw new OrderProcessingException(format("Для заказа недостаточно товара с id %d", request.productId()));
        } catch (FeignException exception) {
            log.warn("Товар с id {} не удалось зарезервировать", request.productId());
            throw new OrderProcessingException(format("Товар с id %d не удалось зарезервировать", request.productId()));
        }
    }

    private void getRelease(List<InventoryRequest> requestReserve) {
        for (int i = requestReserve.size() - 1; i >= 0; i--) {
            InventoryRequest request = requestReserve.get(i);
            try {
                inventoryClient.release(request);
            } catch (FeignException exception) {
                log.warn("Не удалось отменить резерв {} товара с id {}", request.quantity(), request.productId());
                throw new OrderProcessingException(format("Не удалось отменить резерв %d товара с id %d",
                        request.quantity(), request.productId()));
            }
        }
    }

    private boolean isLegacySnapshot(OrderItemRequest itemDto) {
        return itemDto.productName() != null && itemDto.price() != null;
    }

    private OrderItemRequest getSnapshot(OrderItemRequest itemDto, ProductDto productDto) {
        return new OrderItemRequest(itemDto.productId(), productDto.name(), itemDto.quantity(), productDto.price());
    }

    private Map<Long, Integer> getAggregate(List<OrderItemRequest> items) {
        Map<Long, Integer> quantityMap = new LinkedHashMap<>();
        items.forEach(itemRequest -> quantityMap.merge(itemRequest.productId(), itemRequest.quantity(),
                Integer::sum));
        return quantityMap;
    }

    private Map<Long, ProductDto> uploadProducts(Iterable<Long> productIds) {
        Map<Long, ProductDto> productMap = new LinkedHashMap<>();
        productIds.forEach(productId -> {
            ProductDto dto = getProduct(productId);
            if (!Boolean.TRUE.equals(dto.active())) {
                log.warn("Товар с id {} снят с продажи", productId);
                throw new OrderProcessingException(format("Товар с id %d снят с продажи", productId));
            }
            productMap.put(productId, dto);
        });
        return productMap;
    }

    public OrderDto create(CreateOrderRequest request) {
        if (request.items().stream().anyMatch(this::isLegacySnapshot)) {
            return service.create(request);
        }
        Map<Long, Integer> quantityMap = getAggregate(request.items());
        Map<Long, ProductDto> productMap = uploadProducts(quantityMap.keySet());
        List<InventoryRequest> reserveList = new ArrayList<>();
        try {
            quantityMap.forEach((key, value) -> {
                InventoryRequest inventoryRequest = new InventoryRequest(key, value);
                getReserve(inventoryRequest);
                reserveList.add(inventoryRequest);
            });
            List<OrderItemRequest> itemsSnapshotList = request.items()
                    .stream()
                    .map(itemRequest -> getSnapshot(itemRequest, productMap.get(itemRequest.productId())))
                    .toList();
            CreateOrderRequest orderRequest = new CreateOrderRequest(
                    request.customerName(),
                    request.customerEmail(),
                    itemsSnapshotList
            );
            return service.create(orderRequest);
        } catch (RuntimeException exception) {
            log.warn("Не удалось завершить создание заказа, отмена резервирования");
            getRelease(reserveList);
            throw new OrderProcessingException("Не удалось завершить создание заказа, попробуйте зарезервировать позже");
        }
    }
}