package com.dmitrykoryanov.kafka.kafkafinal.client;

import com.dmitrykoryanov.kafka.kafkafinal.client.ProductRepository.Product;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

// Эмуляция CLIENT API: поиск товара по названию и получение персональных рекомендаций.
// Каждый запрос клиента записывается в PostgreSQL (client_requests) и в Kafka (client-requests, User:client-api):
// оттуда Mirror Maker 2 копирует его во второй кластер для аналитики
@Component
public class ClientApi {

    public static final String SEARCH = "SEARCH";
    public static final String RECOMMENDATIONS = "RECOMMENDATIONS";

    private final ProductRepository products;
    private final ClientRequestRepository requests;
    private final RecommendationReader recommendations;
    private final KafkaProducer<String, String> producer;
    private final JsonMapper mapper = new JsonMapper();

    public ClientApi(ProductRepository products, ClientRequestRepository requests,
                     RecommendationReader recommendations, KafkaClients clients) {
        this.products = products;
        this.requests = requests;
        this.recommendations = recommendations;
        this.producer = clients.primaryProducer(Principals.CLIENT_API);
    }

    // Поиск в каталоге PostgreSQL. В событие для аналитики попадают id найденных товаров:
    // по ним аналитика определяет, какие категории интересны пользователю
    public List<Product> search(String userId, String text) throws ExecutionException, InterruptedException {
        List<Product> found = products.searchByName(text);
        requests.save(userId, SEARCH, text, found.size());
        send(userId, SEARCH, text, found.stream().map(Product::productId).toList());
        return found;
    }

    // Готовые рекомендации от аналитики (JSON), если они уже посчитаны
    public Optional<String> recommendations(String userId) throws ExecutionException, InterruptedException {
        Optional<String> result = recommendations.latestFor(userId);
        requests.save(userId, RECOMMENDATIONS, null, result.isPresent() ? 1 : 0);
        send(userId, RECOMMENDATIONS, null, List.of());
        return result;
    }

    private void send(String userId, String type, String query, List<String> productIds)
            throws ExecutionException, InterruptedException {
        ObjectNode event = mapper.createObjectNode()
                .put("user_id", userId)
                .put("type", type)
                .put("query", query)
                .put("timestamp", Instant.now().toString());
        productIds.forEach(event.putArray("product_ids")::add);
        producer.send(new ProducerRecord<>(Topics.CLIENT_REQUESTS, userId, mapper.writeValueAsString(event))).get();
    }

    @PreDestroy
    public void close() {
        producer.close();
    }
}
