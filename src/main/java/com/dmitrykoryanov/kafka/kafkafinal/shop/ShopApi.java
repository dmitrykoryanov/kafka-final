package com.dmitrykoryanov.kafka.kafkafinal.shop;

import com.dmitrykoryanov.kafka.kafkafinal.config.KafkaClients;
import com.dmitrykoryanov.kafka.kafkafinal.config.PlatformProperties;
import com.dmitrykoryanov.kafka.kafkafinal.config.Principals;
import com.dmitrykoryanov.kafka.kafkafinal.config.Topics;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

// Эмуляция SHOP API: магазины присылают товары, здесь они читаются из JSON-файла (массив товаров)
// и отправляются в топик shop-products от имени User:shop-api. Ключ сообщения — product_id
@Component
public class ShopApi {

    private final JsonMapper mapper = new JsonMapper();
    private final Path productsFile;
    private final KafkaProducer<String, String> producer;

    public ShopApi(PlatformProperties properties, KafkaClients clients) {
        this.productsFile = Path.of(properties.shop().productsFile());
        this.producer = clients.primaryProducer(Principals.SHOP_API);
    }

    // Отправляет все товары из файла и печатает, куда записан каждый. Возвращает число отправленных
    public int sendProducts() throws ExecutionException, InterruptedException {
        JsonNode products = mapper.readTree(productsFile.toFile());
        int sent = 0;
        for (JsonNode product : products) {
            String productId = product.path("product_id").asString("");
            if (productId.isBlank()) {
                System.out.println("  пропущен товар без product_id: " + product);
                continue;
            }
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(Topics.SHOP_PRODUCTS, productId, mapper.writeValueAsString(product));
            // Ждём подтверждения каждой записи, чтобы сразу увидеть ошибку (нет прав, мало реплик в ISR)
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("  %s «%s» -> %s, партиция %d, смещение %d%n", productId,
                    product.path("name").asString(""), metadata.topic(), metadata.partition(), metadata.offset());
            sent++;
        }
        return sent;
    }

    public Path productsFile() {
        return productsFile;
    }

    @PreDestroy
    public void close() {
        producer.close();
    }
}
