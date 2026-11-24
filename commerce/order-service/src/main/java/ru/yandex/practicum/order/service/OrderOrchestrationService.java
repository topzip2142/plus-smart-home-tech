package ru.yandex.practicum.order.service;

import feign.FeignException;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.order.breaker.ServiceCallResult;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemRequest;
import ru.yandex.practicum.order.exception.InventoryServiceUnavailableException;
import ru.yandex.practicum.order.exception.OrderProcessingException;
import ru.yandex.practicum.order.exception.ProductServiceUnavailableException;
import ru.yandex.practicum.order.feign.*;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

@Service
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class OrderOrchestrationService {
    ProductClient productClient;
    InventoryClient inventoryClient;
    OrderService service;

    private ServiceCallResult<ProductDto> getProduct(Long productId) {
        try {
            ProductDto dto = productClient.getProductById(productId);
            if (!Boolean.TRUE.equals(dto.active())) {
                log.warn("Товар с id {} снят с продажи", productId);
                return new ServiceCallResult.Failure<>(format("Товар с id %d снят с продажи", productId));
            }
            return new ServiceCallResult.Success<>(dto);
        } catch (FeignException.NotFound exception) {
            log.warn("Товар с id {} не найден", productId);
            return new ServiceCallResult.Failure<>(format("Товар с id %d не найден", productId));
        } catch (ProductServiceUnavailableException exception) {
            log.warn("Каталог недоступен: {}", exception.getMessage());
            return new ServiceCallResult.Degraded<>(format("Каталог недоступен, товар с id %d будет проверен позже",
                    productId));
        } catch (RuntimeException exception) {
            log.warn("Не удалось получить данные товара c id {}: {}", productId, exception.getMessage());
            return new ServiceCallResult.Degraded<>(format("Не удалось получить данные товара c id %d", productId));

        }
    }

    private ServiceCallResult<InventoryResponse> getReserve(InventoryRequest request) {
        try {
            return new ServiceCallResult.Success<>(inventoryClient.reserve(request));
        } catch (FeignException.NotFound exception) {
            log.warn("Складская запись для товара с id {} не найдена", request.productId());
            return new ServiceCallResult.Failure<>(format("Складская запись для товара с id %d не найдена",
                    request.productId()));
        } catch (FeignException.Conflict exception) {
            log.warn("Для заказа недостаточно товара с id {}", request.productId());
            return new ServiceCallResult.Failure<>(format("Для заказа недостаточно товара с id %d",
                    request.productId()));
        } catch (InventoryServiceUnavailableException exception) {
            log.warn("Склад временно недоступен");
            return new ServiceCallResult.Degraded<>("Склад временно недоступен");
        } catch (FeignException exception) {
            if (exception.status() >= 400 && exception.status() < 500) {
                log.warn("Склад отклонил резерв товара с id {}", request.productId());
                return new ServiceCallResult.Failure<>(
                        format("Склад отклонил резерв товара с id %d", request.productId())
                );
            }
            log.warn("Товар с id {} не удалось зарезервировать", request.productId());
            return new ServiceCallResult.Degraded<>(format("Товар с id %d не удалось зарезервировать",
                    request.productId()));
        }
    }

    private boolean reserve(InventoryRequest request, List<InventoryRequest> reserved) {
        ServiceCallResult<InventoryResponse> result = getReserve(request);
        return switch (result) {
            case ServiceCallResult.Success<InventoryResponse> success -> {
                reserved.add(request);
                yield true;
            }
            case ServiceCallResult.Failure<InventoryResponse> failure ->
                    throw new OrderProcessingException(failure.message());
            case ServiceCallResult.Degraded<InventoryResponse> degraded -> {
                log.warn("Склад временно недоступен: {}", degraded.reason());
                yield false;
            }
        };
    }

    private void getRelease(List<InventoryRequest> requestReserve) {
        for (int i = requestReserve.size() - 1; i >= 0; i--) {
            InventoryRequest request = requestReserve.get(i);
            try {
                inventoryClient.release(request);
            } catch (RuntimeException exception) {
                log.warn("Не удалось отменить резерв {} товара с id {}", request.quantity(), request.productId());
            }
        }
    }

    private OrderItemRequest getSnapshot(OrderItemRequest itemDto, ProductDto productDto) {
        if (productDto == null) {
            // каталог деградировал: данные недоверенные — цену в итог не включаем, уточним при подтверждении
            String name = itemDto.productName() != null
                    ? itemDto.productName()
                    : format("Товар c id %d (ожидает проверки)", itemDto.productId());
            return new OrderItemRequest(itemDto.productId(), name, itemDto.quantity(), BigDecimal.ZERO);
        }
        return new OrderItemRequest(itemDto.productId(), productDto.name(), itemDto.quantity(), productDto.price());
    }

    private Map<Long, Integer> getAggregate(List<OrderItemRequest> items) {
        Map<Long, Integer> quantityMap = new LinkedHashMap<>();
        items.forEach(itemRequest -> quantityMap.merge(itemRequest.productId(), itemRequest.quantity(),
                Integer::sum));
        return quantityMap;
    }

    public OrderDto createOrder(CreateOrderRequest request) {
        Map<Long, Integer> quantityMap = getAggregate(request.items());
        Map<Long, ProductDto> productMap = new LinkedHashMap<>();
        List<InventoryRequest> reserveList = new ArrayList<>();
        Set<Long> degradedProductIds = new LinkedHashSet<>();
        AtomicBoolean isFullyReserved = new AtomicBoolean(true);
        try {
            quantityMap.forEach((productId, quantity) -> {
                ServiceCallResult<ProductDto> productResult = getProduct(productId);
                switch (productResult) {
                    case ServiceCallResult.Success<ProductDto> success ->
                            productMap.put(productId, success.value());
                    case ServiceCallResult.Failure<ProductDto> failure ->
                            throw new OrderProcessingException(failure.message());
                    case ServiceCallResult.Degraded<ProductDto> degraded -> {
                        log.warn("Каталог недоступен ({}): товар с id {} будет проверен при подтверждении",
                                degraded.reason(), productId);
                        degradedProductIds.add(productId);
                    }
                }
            });
            quantityMap.forEach((productId, quantity) -> {
                if (!reserve(new InventoryRequest(productId, quantity), reserveList)) {
                    isFullyReserved.set(false);
                }
            });
            if (!degradedProductIds.isEmpty()) {
                isFullyReserved.set(false);
            }
            List<OrderItemRequest> itemsSnapshotList = request.items()
                    .stream()
                    .map(itemRequest -> getSnapshot(itemRequest,
                            productMap.get(itemRequest.productId())))
                    .toList();
            CreateOrderRequest orderRequest = new CreateOrderRequest(
                    request.customerName(),
                    request.customerEmail(),
                    itemsSnapshotList
            );
            return service.create(orderRequest, isFullyReserved.get());
        } catch (OrderProcessingException exception) {
            log.warn("Не удалось завершить создание заказа, отменяем резервы: {}", exception.getMessage());
            getRelease(reserveList);
            throw exception;
        } catch (RuntimeException exception) {
            log.warn("Не удалось завершить создание заказа, отмена резервирования");
            getRelease(reserveList);
            throw new OrderProcessingException("Не удалось завершить создание заказа, попробуйте зарезервировать позже");
        }
    }
}