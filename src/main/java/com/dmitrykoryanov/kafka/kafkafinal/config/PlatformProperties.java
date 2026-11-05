package com.dmitrykoryanov.kafka.kafkafinal.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

// Настройки platform.* из application.properties
@ConfigurationProperties("platform")
public record PlatformProperties(Kafka kafka, BackupKafka backupKafka, Shop shop, Streams streams) {

    // Основной кластер (TLS + ACL). У каждого компонента свой сертификат: certsDir/<имя>/<имя>.p12,
    // общий truststore с сертификатом CA — certsDir/truststore.p12. Пароль у всех хранилищ один
    public record Kafka(String bootstrapServers, String certsDir, String password) {
    }

    // Второй кластер (без TLS): копия данных от Mirror Maker 2 и рекомендации аналитики
    public record BackupKafka(String bootstrapServers) {
    }

    // SHOP API: файл с товарами магазинов
    public record Shop(String productsFile) {
    }

    // Каталог служебных файлов Kafka Streams
    public record Streams(String stateDir) {
    }
}
