package ru.yandex.practicum.product.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.product.dto.CategoryDto;
import ru.yandex.practicum.product.dto.CreateCategoryRequest;
import ru.yandex.practicum.product.entity.Category;
import ru.yandex.practicum.product.exception.NotFoundException;
import ru.yandex.practicum.product.mapper.CatalogMapper;
import ru.yandex.practicum.product.repository.CategoryRepository;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CategoryService {

    private final CategoryRepository categoryRepository;
    private final CatalogMapper mapper;

    public List<CategoryDto> findAll() {
        return categoryRepository.findAllByOrderByIdAsc().stream()
                .map(mapper::toDto)
                .toList();
    }

    public CategoryDto findById(Long id) {
        return mapper.toDto(getCategory(id));
    }

    @Transactional
    public CategoryDto create(CreateCategoryRequest request) {
        Category saved = categoryRepository.save(mapper.toEntity(request));
        log.info("Создана категория id={}, name={}", saved.getId(), saved.getName());
        return mapper.toDto(saved);
    }

    Category getCategory(Long id) {
        return categoryRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Категория с id=" + id + " не найдена"));
    }
}
