package com.dmitrykoryanov.kafka.kafkafinal.client;

import com.dmitrykoryanov.kafka.kafkafinal.config.KafkaClients;
import com.dmitrykoryanov.kafka.kafkafinal.config.Topics;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// Читает готовые рекомендации из топика recommendations второго кластера (их пишет аналитика Spark)
@Component
@RequiredArgsConstructor
public class RecommendationReader {

    private final KafkaClients clients;

    // Последние рекомендации пользователя: читаем топик с начала до текущего конца
    // и запоминаем последнее сообщение с ключом userId. Топик compacted и небольшой
    public Optional<String> latestFor(String userId) {
        try (KafkaConsumer<String, String> consumer = clients.backupConsumer()) {
            List<TopicPartition> partitions = consumer.partitionsFor(Topics.RECOMMENDATIONS, Duration.ofSeconds(10))
                    .stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);

            String latest = null;
            while (partitions.stream().anyMatch(partition -> consumer.position(partition) < end.get(partition))) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (userId.equals(record.key())) {
                        latest = record.value();
                    }
                }
            }
            return Optional.ofNullable(latest);
        }
    }
}
