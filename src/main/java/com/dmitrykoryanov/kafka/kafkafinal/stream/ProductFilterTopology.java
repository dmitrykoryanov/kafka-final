package com.dmitrykoryanov.kafka.kafkafinal.stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import static com.dmitrykoryanov.kafka.kafkafinal.config.Topics.BANNED_PRODUCTS;
import static com.dmitrykoryanov.kafka.kafkafinal.config.Topics.PRODUCTS;
import static com.dmitrykoryanov.kafka.kafkafinal.config.Topics.SHOP_PRODUCTS;

// Топология потоковой проверки товаров:
//   shop-products ──leftJoin по product_id──► GlobalKTable(banned-products) ──► только разрешённые ──► products
// Список запрещённых товаров читается в GlobalKTable целиком на каждом экземпляре приложения, поэтому
// топики не обязаны быть ко-партиционированы, а изменения списка (ban-add/ban-remove) действуют сразу
@Slf4j
public final class ProductFilterTopology {

    // Хранилище со списком запрещённых товаров, из него же читает команда ban-list
    public static final String BANNED_PRODUCTS_STORE = "banned-products-store";

    private ProductFilterTopology() {
    }

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        // Ключ — product_id, значение — причина запрета (JSON). Tombstone удаляет товар из таблицы.
        // Хранилище в памяти: при старте Kafka Streams заново читает в него весь compacted-топик
        GlobalKTable<String, String> bannedProducts = builder.globalTable(BANNED_PRODUCTS,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String>as(Stores.inMemoryKeyValueStore(BANNED_PRODUCTS_STORE))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        builder.stream(SHOP_PRODUCTS, Consumed.with(Serdes.String(), Serdes.String()))
                // Ищем товар в списке запрещённых по ключу сообщения (product_id).
                // Для запрещённого товара соединение даёт null, для разрешённого — исходный JSON
                .leftJoin(bannedProducts,
                        (productId, product) -> productId,
                        (product, ban) -> ban == null ? product : null)
                .filter((productId, product) -> {
                    if (product == null) {
                        log.info("Фильтр: товар {} в списке запрещённых, дальше не передаётся", productId);
                        return false;
                    }
                    log.info("Фильтр: товар {} разрешён -> {}", productId, PRODUCTS);
                    return true;
                })
                .to(PRODUCTS, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}
