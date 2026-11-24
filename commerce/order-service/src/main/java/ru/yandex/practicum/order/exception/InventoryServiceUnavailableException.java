package ru.yandex.practicum.order.exception;

public class InventoryServiceUnavailableException extends RuntimeException {

    public InventoryServiceUnavailableException(Long id, Throwable cause) {
    }

    public InventoryServiceUnavailableException(String message) {
        super(message);
    }

    public InventoryServiceUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}