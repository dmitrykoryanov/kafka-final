package com.dmitrykoryanov.kafka.kafkafinal.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

// Создаёт клиентов Kafka. В основной кластер каждый компонент ходит по TLS со своим сертификатом
// (principal = CN сертификата), во второй — без шифрования
@Component
@RequiredArgsConstructor
public class KafkaClients {

    private final PlatformProperties properties;

    // Подключение к основному кластеру от имени principal
    public Map<String, Object> primary(String principal) {
        PlatformProperties.Kafka kafka = properties.kafka();
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        // keystore: закрытый ключ и сертификат клиента — по нему брокер узнаёт пользователя
        config.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
        config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, Path.of(kafka.certsDir(), principal, principal + ".p12").toString());
        config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, kafka.password());
        config.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, kafka.password());
        // truststore: сертификат CA — по нему клиент проверяет брокера
        config.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
        config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, Path.of(kafka.certsDir(), "truststore.p12").toString());
        config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, kafka.password());
        return config;
    }

    public KafkaProducer<String, String> primaryProducer(String principal) {
        return producer(primary(principal));
    }

    public KafkaConsumer<String, String> primaryConsumer(String principal, String groupId) {
        Map<String, Object> config = primary(principal);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return consumer(config);
    }

    public Admin primaryAdmin() {
        return Admin.create(primary(Principals.ADMIN));
    }

    // Подключение ко второму кластеру
    public Map<String, Object> backup() {
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, properties.backupKafka().bootstrapServers());
        return config;
    }

    // Консьюмер без группы: партиции назначаются вручную (assign)
    public KafkaConsumer<String, String> backupConsumer() {
        return consumer(backup());
    }

    public Admin backupAdmin() {
        return Admin.create(backup());
    }

    private static KafkaProducer<String, String> producer(Map<String, Object> config) {
        // Запись подтверждается, когда её получили все реплики из ISR (в основном кластере их минимум 2)
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        // Если брокер недоступен или нет прав, send() не ждёт дольше 10 секунд
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        return new KafkaProducer<>(config, new StringSerializer(), new StringSerializer());
    }

    private static KafkaConsumer<String, String> consumer(Map<String, Object> config) {
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer());
    }
}
