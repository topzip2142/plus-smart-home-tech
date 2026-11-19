package ru.yandex.practicum.order.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemDto;
import ru.yandex.practicum.order.dto.OrderItemRequest;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.entity.OrderItem;

import java.math.RoundingMode;

@Component
public class OrderMapper {

    public OrderItem toEntity(OrderItemRequest request) {
        OrderItem item = new OrderItem();
        item.setProductId(request.productId());
        item.setProductName(request.productName());
        item.setQuantity(request.quantity());
        item.setPrice(request.price().setScale(2, RoundingMode.HALF_UP));
        return item;
    }

    public OrderDto toDto(Order order) {
        return new OrderDto(
                order.getId(),
                order.getCustomerName(),
                order.getCustomerEmail(),
                order.getStatus().name(),
                order.getTotalPrice(),
                order.getStatusDetails(),
                order.getCreatedAt(),
                order.getItems().stream().map(this::toDto).toList()
        );
    }

    private OrderItemDto toDto(OrderItem item) {
        return new OrderItemDto(
                item.getId(),
                item.getProductId(),
                item.getProductName(),
                item.getQuantity(),
                item.getPrice()
        );
    }
}