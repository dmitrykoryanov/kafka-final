package com.dmitrykoryanov.kafka.kafkafinal;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

// Два режима запуска:
// - по умолчанию — консоль платформы (console/ConsoleRunner): SHOP API, CLIENT API, фильтр Kafka Streams,
//   управление списком запрещённых товаров;
// - профиль setup — создание топиков и ACL (setup/KafkaSetup), запускается в контейнере kafka-setup
@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaFinalApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaFinalApplication.class, args);
    }

}
