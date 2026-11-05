package com.dmitrykoryanov.kafka.kafkafinal.stream;

import com.dmitrykoryanov.kafka.kafkafinal.config.KafkaClients;
import com.dmitrykoryanov.kafka.kafkafinal.config.PlatformProperties;
import com.dmitrykoryanov.kafka.kafkafinal.config.Principals;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

// Запускает фильтр запрещённых товаров (ProductFilterTopology) от имени User:stream-filter
@Slf4j
@Component
@RequiredArgsConstructor
public class ProductFilter {

    private final KafkaClients clients;
    private final PlatformProperties properties;

    private KafkaStreams streams;

    public void start() {
        Properties config = new Properties();
        config.putAll(clients.primary(Principals.STREAM_FILTER));
        // application.id — это и group.id консьюмеров Kafka Streams (для группы выдан ACL)
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, Principals.STREAM_FILTER_GROUP);
        config.put(StreamsConfig.STATE_DIR_CONFIG, properties.streams().stateDir());
        // Отправка в products подтверждается всеми репликами из ISR
        config.put(StreamsConfig.producerPrefix("acks"), "all");

        streams = new KafkaStreams(ProductFilterTopology.build(), config);
        streams.setStateListener((newState, oldState) -> log.info("Фильтр Kafka Streams: {} -> {}", oldState, newState));
        streams.setUncaughtExceptionHandler(exception -> {
            log.error("Ошибка в фильтре Kafka Streams, фильтр остановлен", exception);
            return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        streams.start();
    }

    public KafkaStreams.State state() {
        return streams == null ? KafkaStreams.State.NOT_RUNNING : streams.state();
    }

    // Текущий список запрещённых товаров: product_id -> причина (JSON) из глобального хранилища Kafka Streams
    public Map<String, String> bannedProducts() {
        if (state() != KafkaStreams.State.RUNNING) {
            throw new IllegalStateException("фильтр Kafka Streams ещё не запущен, состояние " + state());
        }
        ReadOnlyKeyValueStore<String, String> store = streams.store(StoreQueryParameters.fromNameAndType(
                ProductFilterTopology.BANNED_PRODUCTS_STORE, QueryableStoreTypes.keyValueStore()));
        Map<String, String> banned = new TreeMap<>();
        try (KeyValueIterator<String, String> all = store.all()) {
            all.forEachRemaining((KeyValue<String, String> entry) -> banned.put(entry.key, entry.value));
        }
        return banned;
    }

    @PreDestroy
    public void stop() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(10));
        }
    }
}
