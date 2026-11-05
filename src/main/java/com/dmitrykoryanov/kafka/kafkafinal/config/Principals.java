package com.dmitrykoryanov.kafka.kafkafinal.config;

// Пользователи основного кластера. Имя совпадает с CN сертификата (certs/<имя>/<имя>.p12):
// брокер превращает CN=shop-api в User:shop-api. Права каждого — в setup/KafkaSetup
public final class Principals {

    // Суперпользователь: создаёт топики и ACL (kafka-setup), CLI-утилиты внутри контейнеров
    public static final String ADMIN = "admin";
    // SHOP API: пишет товары в shop-products
    public static final String SHOP_API = "shop-api";
    // Управление списком запрещённых товаров: пишет в banned-products
    public static final String BAN_MANAGER = "ban-manager";
    // Фильтр Kafka Streams: читает shop-products и banned-products, пишет в products
    public static final String STREAM_FILTER = "stream-filter";
    // CLIENT API: читает products (каталог для поиска), пишет запросы клиентов в client-requests
    public static final String CLIENT_API = "client-api";
    // Kafka Connect: читает products и пишет их в файл
    public static final String CONNECT = "connect";
    // Mirror Maker 2: читает products и client-requests для копирования во второй кластер
    public static final String MIRROR_MAKER = "mirror-maker";

    // Группы консьюмеров, для которых выданы права
    // application.id Kafka Streams — это и group.id его консьюмеров
    public static final String STREAM_FILTER_GROUP = "stream-filter";
    public static final String CLIENT_API_CATALOG_GROUP = "client-api-catalog";
    // Kafka Connect называет группу sink-коннектора connect-<имя коннектора>
    public static final String CONNECT_FILE_SINK_GROUP = "connect-products-file-sink";

    private Principals() {
    }
}
