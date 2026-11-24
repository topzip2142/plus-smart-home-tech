package ru.yandex.practicum.order.breaker;

public sealed interface ServiceCallResult<T> {

    record Success<T>(T value) implements ServiceCallResult<T> { }

    record Failure<T>(String message) implements ServiceCallResult<T> { }

    record Degraded<T>(String reason) implements ServiceCallResult<T> { }
}