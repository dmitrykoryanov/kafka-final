package com.dmitrykoryanov.kafka.kafkafinal;

import com.dmitrykoryanov.kafka.kafkafinal.console.ConsoleRunner;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

// Контекст поднимается без Kafka и PostgreSQL: консоль заменена заглушкой (фоновые компоненты не стартуют),
// бины создаются лениво, таблицы не создаются
@SpringBootTest(properties = {"spring.main.lazy-initialization=true", "spring.sql.init.mode=never"})
class KafkaFinalApplicationTests {

    @MockitoBean
    private ConsoleRunner consoleRunner;

    @Test
    void contextLoads() {
    }

}
