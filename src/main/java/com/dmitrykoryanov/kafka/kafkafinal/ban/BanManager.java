package com.dmitrykoryanov.kafka.kafkafinal.ban;

import com.dmitrykoryanov.kafka.kafkafinal.config.KafkaClients;
import com.dmitrykoryanov.kafka.kafkafinal.config.Principals;
import com.dmitrykoryanov.kafka.kafkafinal.config.Topics;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.concurrent.ExecutionException;

// Управление списком запрещённых товаров (команды ban-add, ban-remove). Список хранится в compacted-топике
// banned-products, писать в него может только User:ban-manager. Фильтр Kafka Streams читает топик в GlobalKTable
@Component
public class BanManager {

    private final JsonMapper mapper = new JsonMapper();
    private final KafkaProducer<String, String> producer;

    public BanManager(KafkaClients clients) {
        this.producer = clients.primaryProducer(Principals.BAN_MANAGER);
    }

    public void ban(String productId, String reason) throws ExecutionException, InterruptedException {
        ObjectNode ban = mapper.createObjectNode()
                .put("product_id", productId)
                .put("reason", reason)
                .put("banned_at", Instant.now().toString());
        producer.send(new ProducerRecord<>(Topics.BANNED_PRODUCTS, productId, mapper.writeValueAsString(ban))).get();
    }

    // Tombstone (значение null): после compaction в топике не останется записи о запрете товара
    public void unban(String productId) throws ExecutionException, InterruptedException {
        producer.send(new ProducerRecord<>(Topics.BANNED_PRODUCTS, productId, null)).get();
    }

    @PreDestroy
    public void close() {
        producer.close();
    }
}
