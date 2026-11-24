package ru.yandex.practicum.inventory.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.inventory.exception.InsufficientStockException;
import ru.yandex.practicum.inventory.exception.InvalidQuantityException;

@Entity
@Table(name = "inventory")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class InventoryItem {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "product_id", nullable = false, unique = true)
    private Long productId;

    @Column(name = "quantity", nullable = false)
    private int quantity;

    @Column(name = "reserved_quantity", nullable = false)
    private int reservedQuantity;

    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    public InventoryItem(Long productId, int quantity) {
        this.productId = productId;
        this.quantity = quantity;
        this.reservedQuantity = 0;
    }

    public int getAvailableQuantity() {
        return quantity - reservedQuantity;
    }

    public void changeQuantity(int newQuantity) {
        if (newQuantity < reservedQuantity) {
            throw new InvalidQuantityException("Общее количество товара productId=" + productId
                    + " не может быть меньше зарезервированного (" + reservedQuantity + ")");
        }
        this.quantity = newQuantity;
    }

    public void reserve(int amount) {
        if (amount > getAvailableQuantity()) {
            throw new InsufficientStockException("Недостаточно товара productId=" + productId
                    + ": запрошено " + amount + ", доступно " + getAvailableQuantity());
        }
        this.reservedQuantity += amount;
    }

    public void release(int amount) {
        if (amount < 0) {
            throw new InvalidQuantityException(
                    "Количество к снятию резерва не может быть отрицательным: " + amount);
        }
        if (amount > reservedQuantity) {
            throw new InvalidQuantityException(
                    "Нельзя снять резерв " + amount
                            + " для productId=" + productId
                            + " — зарезервировано только " + reservedQuantity);
        }
        this.reservedQuantity -= amount;
    }
}