package com.dmitrykoryanov.kafka.kafkafinal.config;

// Топики платформы. Ключ и значение везде — строки, значение — JSON
public final class Topics {

    // ---- Основной кластер ----

    // Товары от магазинов (SHOP API). Ключ — product_id, значение — JSON товара (пример — data/products.json)
    public static final String SHOP_PRODUCTS = "shop-products";

    // Список запрещённых товаров, compacted. Ключ — product_id,
    // значение — {"product_id", "reason", "banned_at"}; значение null (tombstone) снимает запрет
    public static final String BANNED_PRODUCTS = "banned-products";

    // Разрешённые товары: результат фильтра Kafka Streams. Формат как у shop-products
    public static final String PRODUCTS = "products";

    // Запросы клиентов (CLIENT API). Ключ — user_id,
    // значение — {"user_id", "type": SEARCH|RECOMMENDATIONS, "query", "product_ids", "timestamp"}
    public static final String CLIENT_REQUESTS = "client-requests";

    // ---- Второй кластер ----

    // Рекомендации от аналитики (Spark), compacted. Ключ — user_id,
    // значение — {"user_id", "generated_at", "products": [{"rank", "product_id", "name", "category", "price", "currency"}]}
    public static final String RECOMMENDATIONS = "recommendations";

    private Topics() {
    }
}
