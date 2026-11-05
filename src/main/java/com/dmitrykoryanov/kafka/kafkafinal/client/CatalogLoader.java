package com.dmitrykoryanov.kafka.kafkafinal.client;

import com.dmitrykoryanov.kafka.kafkafinal.config.KafkaClients;
import com.dmitrykoryanov.kafka.kafkafinal.config.Principals;
import com.dmitrykoryanov.kafka.kafkafinal.config.Topics;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;

// Загружает разрешённые товары (топик products) в каталог PostgreSQL, по которому CLIENT API ищет товары.
// Работает в отдельном потоке от имени User:client-api, группа консьюмеров client-api-catalog
@Slf4j
@Component
@RequiredArgsConstructor
public class CatalogLoader {

    private final KafkaClients clients;
    private final ProductRepository products;
    private final JsonMapper mapper = new JsonMapper();

    private KafkaConsumer<String, String> consumer;
    private Thread thread;

    public void start() {
        consumer = clients.primaryConsumer(Principals.CLIENT_API, Principals.CLIENT_API_CATALOG_GROUP);
        thread = new Thread(this::poll, "catalog-loader");
        thread.start();
    }

    private void poll() {
        try {
            consumer.subscribe(List.of(Topics.PRODUCTS));
            while (true) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                    save(record);
                }
            }
        } catch (WakeupException e) {
            // stop(): выходим из цикла
        } catch (Exception e) {
            log.error("Загрузка каталога остановлена из-за ошибки", e);
        } finally {
            consumer.close();
        }
    }

    private void save(ConsumerRecord<String, String> record) {
        try {
            JsonNode product = mapper.readTree(record.value());
            products.save(product, record.value());
            log.info("Каталог: товар {} «{}» сохранён в PostgreSQL", record.key(), product.path("name").asString(""));
        } catch (Exception e) {
            log.warn("Каталог: товар {} пропущен: {}", record.key(), e.getMessage());
        }
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        if (thread != null) {
            consumer.wakeup();
            thread.join(10_000);
        }
    }
}
