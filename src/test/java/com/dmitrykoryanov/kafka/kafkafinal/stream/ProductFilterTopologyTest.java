package com.dmitrykoryanov.kafka.kafkafinal.stream;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static com.dmitrykoryanov.kafka.kafkafinal.config.Topics.BANNED_PRODUCTS;
import static com.dmitrykoryanov.kafka.kafkafinal.config.Topics.PRODUCTS;
import static com.dmitrykoryanov.kafka.kafkafinal.config.Topics.SHOP_PRODUCTS;
import static org.assertj.core.api.Assertions.assertThat;

// Проверка фильтра запрещённых товаров без брокера
class ProductFilterTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> shopProducts;
    private TestInputTopic<String, String> bannedProducts;
    private TestOutputTopic<String, String> products;

    @BeforeEach
    void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "product-filter-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(ProductFilterTopology.build(), config);
        shopProducts = driver.createInputTopic(SHOP_PRODUCTS, new StringSerializer(), new StringSerializer());
        bannedProducts = driver.createInputTopic(BANNED_PRODUCTS, new StringSerializer(), new StringSerializer());
        products = driver.createOutputTopic(PRODUCTS, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void allowedProductPassesUnchanged() {
        shopProducts.pipeInput("12345", "{\"product_id\":\"12345\",\"name\":\"Умные часы XYZ\"}");

        assertThat(products.readKeyValuesToList())
                .containsExactly(KeyValue.pair("12345", "{\"product_id\":\"12345\",\"name\":\"Умные часы XYZ\"}"));
    }

    @Test
    void bannedProductIsDropped() {
        bannedProducts.pipeInput("12350", "{\"product_id\":\"12350\",\"reason\":\"запрещён\"}");

        shopProducts.pipeInput("12350", "{\"product_id\":\"12350\"}");
        shopProducts.pipeInput("12345", "{\"product_id\":\"12345\"}");

        assertThat(keys(products.readKeyValuesToList())).containsExactly("12345");
    }

    @Test
    void productPassesAgainAfterBanIsRemoved() {
        bannedProducts.pipeInput("12350", "{\"product_id\":\"12350\",\"reason\":\"запрещён\"}");
        shopProducts.pipeInput("12350", "{\"product_id\":\"12350\"}");

        // Tombstone снимает запрет
        bannedProducts.pipeInput("12350", (String) null);
        shopProducts.pipeInput("12350", "{\"product_id\":\"12350\"}");

        assertThat(keys(products.readKeyValuesToList())).isEqualTo(List.of("12350"));
    }

    private static List<String> keys(List<KeyValue<String, String>> records) {
        return records.stream().map(record -> record.key).toList();
    }
}
