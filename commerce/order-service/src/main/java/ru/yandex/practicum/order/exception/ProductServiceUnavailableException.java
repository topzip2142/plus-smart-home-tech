package ru.yandex.practicum.order.exception;

public class ProductServiceUnavailableException extends RuntimeException {
    public ProductServiceUnavailableException(Object productId, Throwable cause) {
    }

    public ProductServiceUnavailableException(String message) {
        super(message);
    }

    public ProductServiceUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}