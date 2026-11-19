package ru.yandex.practicum.product.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.product.dto.CreateProductRequest;
import ru.yandex.practicum.product.dto.ProductDto;
import ru.yandex.practicum.product.dto.UpdateProductRequest;
import ru.yandex.practicum.product.entity.Product;
import ru.yandex.practicum.product.exception.NotFoundException;
import ru.yandex.practicum.product.mapper.CatalogMapper;
import ru.yandex.practicum.product.repository.ProductRepository;

import java.math.RoundingMode;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ProductService {

    private final ProductRepository productRepository;
    private final CategoryService categoryService;
    private final CatalogMapper mapper;

    public List<ProductDto> findAllActive() {
        return toDtos(productRepository.findAllByActiveTrueOrderByIdAsc());
    }

    public ProductDto findById(Long id) {
        return mapper.toDto(getProduct(id));
    }

    public List<ProductDto> findByCategory(Long categoryId) {
        categoryService.getCategory(categoryId);
        return toDtos(productRepository.findAllByCategoryIdAndActiveTrueOrderByIdAsc(categoryId));
    }

    public List<ProductDto> search(String query) {
        if (query == null || query.isBlank()) {
            return findAllActive();
        }
        return toDtos(productRepository.findAllByActiveTrueAndNameContainingIgnoreCaseOrderByIdAsc(query.trim()));
    }

    @Transactional
    public ProductDto create(CreateProductRequest request) {
        Product product = new Product();
        product.setName(request.name());
        product.setDescription(request.description());
        product.setPrice(request.price().setScale(2, RoundingMode.HALF_UP));
        product.setImageUrl(request.imageUrl());
        product.setActive(true);
        if (request.categoryId() != null) {
            product.setCategory(categoryService.getCategory(request.categoryId()));
        }
        Product saved = productRepository.save(product);
        log.info("Создан товар id={}, name={}", saved.getId(), saved.getName());
        return mapper.toDto(saved);
    }

    @Transactional
    public ProductDto update(Long id, UpdateProductRequest request) {
        Product product = getProduct(id);
        if (request.name() != null) {
            if (request.name().isBlank()) {
                throw new IllegalArgumentException("Название товара не может быть пустым");
            }
            product.setName(request.name());
        }
        if (request.description() != null) {
            product.setDescription(request.description());
        }
        if (request.price() != null) {
            product.setPrice(request.price().setScale(2, RoundingMode.HALF_UP));
        }
        if (request.categoryId() != null) {
            product.setCategory(categoryService.getCategory(request.categoryId()));
        }
        if (request.imageUrl() != null) {
            product.setImageUrl(request.imageUrl());
        }
        if (request.active() != null) {
            product.setActive(request.active());
        }
        log.info("Обновлён товар id={}", id);
        return mapper.toDto(product);
    }

    private Product getProduct(Long id) {
        return productRepository.findWithCategoryById(id)
                .orElseThrow(() -> new NotFoundException("Товар с id=" + id + " не найден"));
    }

    private List<ProductDto> toDtos(List<Product> products) {
        return products.stream().map(mapper::toDto).toList();
    }
}
