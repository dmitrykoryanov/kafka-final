package com.dmitrykoryanov.kafka.kafkafinal.console;

import com.dmitrykoryanov.kafka.kafkafinal.config.KafkaClients;
import com.dmitrykoryanov.kafka.kafkafinal.config.Principals;
import com.dmitrykoryanov.kafka.kafkafinal.config.Topics;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.logging.LogLevel;
import org.springframework.boot.logging.LoggingSystem;
import org.springframework.stereotype.Component;

import java.time.Duration;

// Команда acl-check: клиенты пытаются сделать то, что им не разрешено, и получают отказ брокера.
// Показывает, что топики закрыты для записи и чтения всем, кроме назначенных пользователей
@Component
@RequiredArgsConstructor
public class AclCheck {

    private static final String KAFKA_CLIENTS_LOGGER = "org.apache.kafka.clients";

    private final KafkaClients clients;
    private final LoggingSystem loggingSystem;

    public void run() {
        // Отказы здесь ожидаемы: клиенты Kafka сами пишут о них предупреждения и ошибки, на время проверки
        // отключаем эти логи и печатаем только итог каждой попытки
        loggingSystem.setLogLevel(KAFKA_CLIENTS_LOGGER, LogLevel.OFF);
        try {
            checkWrite(Principals.SHOP_API, Topics.PRODUCTS,
                    "SHOP API пишет напрямую в products, минуя фильтр запрещённых товаров");
            checkWrite(Principals.CLIENT_API, Topics.BANNED_PRODUCTS,
                    "CLIENT API меняет список запрещённых товаров");
            checkWrite(Principals.CONNECT, Topics.SHOP_PRODUCTS,
                    "Kafka Connect пишет в топик магазинов");
            checkRead(Principals.CLIENT_API, Topics.SHOP_PRODUCTS,
                    "CLIENT API читает неотфильтрованные товары магазинов");
        } finally {
            loggingSystem.setLogLevel(KAFKA_CLIENTS_LOGGER, null);
        }
    }

    private void checkWrite(String principal, String topic, String description) {
        System.out.printf("User:%s -> запись в %s (%s)%n", principal, topic, description);
        try (KafkaProducer<String, String> producer = clients.primaryProducer(principal)) {
            producer.send(new ProducerRecord<>(topic, "acl-check", "{}")).get();
            System.out.println("  РАЗРЕШЕНО — права настроены неверно!");
        } catch (Exception e) {
            printDenied(e);
        }
    }

    private void checkRead(String principal, String topic, String description) {
        System.out.printf("User:%s -> чтение %s (%s)%n", principal, topic, description);
        try (KafkaConsumer<String, String> consumer = clients.primaryConsumer(principal, "acl-check")) {
            consumer.partitionsFor(topic, Duration.ofSeconds(10));
            System.out.println("  РАЗРЕШЕНО — права настроены неверно!");
        } catch (Exception e) {
            printDenied(e);
        }
    }

    private static void printDenied(Exception e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        System.out.printf("  отказано: %s: %s%n", cause.getClass().getSimpleName(), cause.getMessage());
    }
}
