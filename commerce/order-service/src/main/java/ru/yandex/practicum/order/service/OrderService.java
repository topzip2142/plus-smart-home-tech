package ru.yandex.practicum.order.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.entity.OrderItem;
import ru.yandex.practicum.order.entity.OrderStatus;
import ru.yandex.practicum.order.exception.NotFoundException;
import ru.yandex.practicum.order.mapper.OrderMapper;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderMapper mapper;

    @Transactional
    public OrderDto create(CreateOrderRequest request) {
        Order order = new Order();
        order.setCustomerName(request.customerName());
        order.setCustomerEmail(request.customerEmail());
        order.setStatus(OrderStatus.CREATED);
        request.items().stream()
                .map(mapper::toEntity)
                .forEach(order::addItem);
        order.setTotalPrice(calculateTotal(order.getItems()));

        Order saved = orderRepository.save(order);
        log.info("Создан заказ id={} для {}, сумма {}", saved.getId(), saved.getCustomerEmail(), saved.getTotalPrice());
        return mapper.toDto(saved);
    }

    public OrderDto findById(Long id) {
        return orderRepository.findWithItemsById(id)
                .map(mapper::toDto)
                .orElseThrow(() -> new NotFoundException("Заказ с id=" + id + " не найден"));
    }

    public List<OrderDto> findAll() {
        return orderRepository.findAllByOrderByCreatedAtDescIdDesc().stream()
                .map(mapper::toDto)
                .toList();
    }

    public List<OrderDto> findByEmail(String email) {
        return orderRepository.findAllByCustomerEmailIgnoreCaseOrderByCreatedAtDescIdDesc(email.trim()).stream()
                .map(mapper::toDto)
                .toList();
    }

    private static BigDecimal calculateTotal(List<OrderItem> items) {
        return items.stream()
                .map(OrderItem::getLineTotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .setScale(2, RoundingMode.HALF_UP);
    }
}
