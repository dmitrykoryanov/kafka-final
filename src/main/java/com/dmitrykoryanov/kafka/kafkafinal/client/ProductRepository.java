package com.dmitrykoryanov.kafka.kafkafinal.client;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.util.List;

// Каталог товаров в PostgreSQL (таблица products, см. schema.sql): по нему CLIENT API ищет товары
@Repository
@RequiredArgsConstructor
public class ProductRepository {

    private final JdbcTemplate jdbc;

    public record Product(String productId, String name, String category, String brand,
                          BigDecimal price, String currency, int available, String storeId) {
    }

    // Добавляет товар или обновляет его, если магазин прислал товар повторно
    public void save(JsonNode product, String json) {
        jdbc.update("""
                        INSERT INTO products (product_id, name, description, category, brand, price, currency,
                                              available, store_id, data, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, now())
                        ON CONFLICT (product_id) DO UPDATE SET
                            name = EXCLUDED.name, description = EXCLUDED.description, category = EXCLUDED.category,
                            brand = EXCLUDED.brand, price = EXCLUDED.price, currency = EXCLUDED.currency,
                            available = EXCLUDED.available, store_id = EXCLUDED.store_id, data = EXCLUDED.data,
                            updated_at = now()
                        """,
                product.path("product_id").asString(),
                product.path("name").asString(""),
                product.path("description").asString(""),
                product.path("category").asString(""),
                product.path("brand").asString(""),
                product.path("price").path("amount").decimalValue(BigDecimal.ZERO),
                product.path("price").path("currency").asString(""),
                product.path("stock").path("available").asInt(0),
                product.path("store_id").asString(""),
                json);
    }

    // Поиск по части названия без учёта регистра
    public List<Product> searchByName(String text) {
        return jdbc.query("""
                        SELECT product_id, name, category, brand, price, currency, available, store_id
                        FROM products WHERE name ILIKE ? ORDER BY name LIMIT 20
                        """,
                (rs, row) -> new Product(rs.getString("product_id"), rs.getString("name"), rs.getString("category"),
                        rs.getString("brand"), rs.getBigDecimal("price"), rs.getString("currency"),
                        rs.getInt("available"), rs.getString("store_id")),
                "%" + text + "%");
    }

    public int count() {
        Integer count = jdbc.queryForObject("SELECT count(*) FROM products", Integer.class);
        return count == null ? 0 : count;
    }
}
