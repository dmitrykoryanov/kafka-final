package com.dmitrykoryanov.kafka.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

// Аналитическая система. Раз в INTERVAL_SECONDS секунд:
//  1. переносит данные из второго кластера Kafka в Data Lake на HDFS:
//       primary.products        -> hdfs:///datalake/products         (разрешённые товары)
//       primary.client-requests -> hdfs:///datalake/client-requests  (запросы клиентов)
//  2. читает Data Lake и считает рекомендации (Spark SQL):
//       товары, найденные пользователем через поиск -> интересные ему категории ->
//       до 5 товаров этих категорий в наличии, которых он ещё не находил
//  3. пишет рекомендации в топик recommendations второго кластера (ключ — user_id)
public final class RecommendationJob {

    private static final String PRODUCTS_TOPIC = "primary.products";
    private static final String REQUESTS_TOPIC = "primary.client-requests";
    private static final String RECOMMENDATIONS_TOPIC = "recommendations";

    private final SparkSession spark;
    private final String kafka;
    private final String productsPath;
    private final String requestsPath;

    private RecommendationJob(SparkSession spark, String kafka, String hdfs) {
        this.spark = spark;
        this.kafka = kafka;
        this.productsPath = hdfs + "/datalake/products";
        this.requestsPath = hdfs + "/datalake/client-requests";
    }

    public static void main(String[] args) throws InterruptedException {
        String kafka = env("KAFKA_BOOTSTRAP_SERVERS", "kafka-b:9092");
        String hdfs = env("HDFS_URL", "hdfs://namenode:8020");
        long intervalSeconds = Long.parseLong(env("INTERVAL_SECONDS", "30"));

        SparkSession spark = SparkSession.builder().appName("marketplace-recommendations").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        RecommendationJob job = new RecommendationJob(spark, kafka, hdfs);

        for (int iteration = 1; ; iteration++) {
            try {
                job.run(iteration);
            } catch (Exception e) {
                // Например, Mirror Maker ещё не создал топики во втором кластере — попробуем в следующий раз
                log("Итерация %d не выполнена: %s", iteration, e.getMessage());
            }
            Thread.sleep(intervalSeconds * 1000);
        }
    }

    private void run(int iteration) {
        long products = copyTopicToHdfs(PRODUCTS_TOPIC, productsPath);
        long requests = copyTopicToHdfs(REQUESTS_TOPIC, requestsPath);
        log("Итерация %d: в HDFS перенесено товаров %d (%s), запросов клиентов %d (%s)",
                iteration, products, productsPath, requests, requestsPath);

        Dataset<Row> recommendations = recommendations().cache();
        long users = recommendations.count();
        if (users > 0) {
            recommendations
                    .selectExpr("user_id AS key", "to_json(struct(user_id, generated_at, products)) AS value")
                    .write()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafka)
                    .option("topic", RECOMMENDATIONS_TOPIC)
                    .save();
            log("Итерация %d: рекомендации для пользователей (%d) записаны в топик %s",
                    iteration, users, RECOMMENDATIONS_TOPIC);
            recommendations.selectExpr("user_id", "products.name AS recommended").show(false);
        } else {
            log("Итерация %d: рекомендаций пока нет — нужен хотя бы один поиск, нашедший товар", iteration);
        }
        recommendations.unpersist();
    }

    // Читает топик целиком (батч от начала до текущего конца) и перезаписывает каталог HDFS:
    // одна строка JSON на сообщение. Возвращает число строк
    private long copyTopicToHdfs(String topic, String path) {
        spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load()
                .selectExpr("CAST(value AS STRING) AS value")
                .write()
                .mode(SaveMode.Overwrite)
                .text(path);
        return spark.read().text(path).count();
    }

    private Dataset<Row> recommendations() {
        spark.read()
                .schema("product_id STRING, name STRING, category STRING, updated_at STRING, "
                        + "price STRUCT<amount: DOUBLE, currency: STRING>, stock STRUCT<available: INT>")
                .json(productsPath)
                .createOrReplaceTempView("raw_products");
        spark.read()
                .schema("user_id STRING, type STRING, query STRING, product_ids ARRAY<STRING>, timestamp STRING")
                .json(requestsPath)
                .createOrReplaceTempView("raw_requests");

        // Последняя версия каждого товара: магазин мог прислать товар несколько раз
        spark.sql("""
                SELECT product_id, name, category, price, currency, available FROM (
                    SELECT product_id, name, category, price.amount AS price, price.currency AS currency,
                           stock.available AS available,
                           row_number() OVER (PARTITION BY product_id ORDER BY updated_at DESC) AS version
                    FROM raw_products WHERE product_id IS NOT NULL)
                WHERE version = 1
                """).createOrReplaceTempView("products");

        // Товары, которые пользователь находил через поиск
        spark.sql("""
                SELECT DISTINCT user_id, product_id
                FROM raw_requests LATERAL VIEW explode(product_ids) found AS product_id
                WHERE type = 'SEARCH'
                """).createOrReplaceTempView("seen");

        // Интерес пользователя к категории — сколько товаров этой категории он находил
        spark.sql("""
                SELECT s.user_id, p.category, count(*) AS score
                FROM seen s JOIN products p ON p.product_id = s.product_id
                GROUP BY s.user_id, p.category
                """).createOrReplaceTempView("interests");

        // Кандидаты: товары интересных категорий в наличии, которых пользователь ещё не видел.
        // Сначала самые интересные категории, внутри категории — дешёвые товары
        spark.sql("""
                SELECT i.user_id, p.product_id, p.name, p.category, p.price, p.currency,
                       row_number() OVER (PARTITION BY i.user_id ORDER BY i.score DESC, p.price ASC) AS rank
                FROM interests i
                JOIN products p ON p.category = i.category
                LEFT ANTI JOIN seen s ON s.user_id = i.user_id AND s.product_id = p.product_id
                WHERE p.available > 0
                """).createOrReplaceTempView("candidates");

        return spark.sql("""
                SELECT user_id,
                       date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss") AS generated_at,
                       sort_array(collect_list(struct(rank, product_id, name, category, price, currency))) AS products
                FROM candidates WHERE rank <= 5
                GROUP BY user_id
                """);
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static void log(String format, Object... args) {
        System.out.printf("%s [analytics] %s%n", LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")),
                String.format(format, args));
    }
}
