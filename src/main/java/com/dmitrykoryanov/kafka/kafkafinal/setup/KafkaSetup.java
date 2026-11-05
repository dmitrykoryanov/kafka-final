package com.dmitrykoryanov.kafka.kafkafinal.setup;

import com.dmitrykoryanov.kafka.kafkafinal.config.KafkaClients;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.dmitrykoryanov.kafka.kafkafinal.config.Principals.*;
import static com.dmitrykoryanov.kafka.kafkafinal.config.Topics.*;
import static org.apache.kafka.common.acl.AclOperation.*;

// Профиль setup (контейнер kafka-setup): создаёт топики и права доступа, после чего приложение завершается.
// Повторный запуск безопасен: существующие топики пропускаются, повторная выдача права ничего не меняет
@Slf4j
@Component
@Profile("setup")
@RequiredArgsConstructor
public class KafkaSetup implements CommandLineRunner {

    private static final int PARTITIONS = 3;
    private static final short PRIMARY_REPLICAS = 3;
    private static final String PRIMARY_MIN_INSYNC_REPLICAS = "2";

    private final KafkaClients clients;

    @Override
    public void run(String... args) throws Exception {
        try (Admin admin = clients.primaryAdmin()) {
            waitForBrokers(admin, PRIMARY_REPLICAS, "основной");
            createTopics(admin, List.of(
                    primaryTopic(SHOP_PRODUCTS, false),
                    // compacted: для каждого товара хранится только последнее решение (запрет или снятие запрета)
                    primaryTopic(BANNED_PRODUCTS, true),
                    primaryTopic(PRODUCTS, false),
                    primaryTopic(CLIENT_REQUESTS, false)));
            List<AclBinding> acls = acls();
            admin.createAcls(acls).all().get();
            log.info("Права доступа (ACL) выданы: {}", acls.size());
            acls.forEach(acl -> log.info("  {} {} {} {}", acl.entry().principal(), acl.entry().operation(),
                    acl.pattern().resourceType(), acl.pattern().name()));
        }
        try (Admin admin = clients.backupAdmin()) {
            waitForBrokers(admin, 1, "второй");
            createTopics(admin, List.of(new NewTopic(RECOMMENDATIONS, PARTITIONS, (short) 1)
                    .configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT))));
        }
        log.info("Настройка кластеров завершена");
    }

    // Топики основного кластера: 3 партиции, 3 реплики, запись подтверждается минимум двумя репликами
    private static NewTopic primaryTopic(String name, boolean compacted) {
        return new NewTopic(name, PARTITIONS, PRIMARY_REPLICAS).configs(Map.of(
                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, PRIMARY_MIN_INSYNC_REPLICAS,
                TopicConfig.CLEANUP_POLICY_CONFIG,
                compacted ? TopicConfig.CLEANUP_POLICY_COMPACT : TopicConfig.CLEANUP_POLICY_DELETE));
    }

    // Права на основном кластере. Каждый топик пишет только один пользователь
    private static List<AclBinding> acls() {
        List<AclBinding> acls = new ArrayList<>();
        // SHOP API пишет товары магазинов
        acls.addAll(topic(SHOP_API, SHOP_PRODUCTS, WRITE, DESCRIBE));
        // Список запрещённых товаров меняет только ban-manager
        acls.addAll(topic(BAN_MANAGER, BANNED_PRODUCTS, WRITE, DESCRIBE));
        // Фильтр Kafka Streams: читает товары и список запретов, пишет разрешённые товары
        acls.addAll(topic(STREAM_FILTER, SHOP_PRODUCTS, READ, DESCRIBE));
        acls.addAll(topic(STREAM_FILTER, BANNED_PRODUCTS, READ, DESCRIBE));
        acls.addAll(topic(STREAM_FILTER, PRODUCTS, WRITE, DESCRIBE));
        acls.add(group(STREAM_FILTER, STREAM_FILTER_GROUP));
        // CLIENT API: загружает разрешённые товары в PostgreSQL, пишет запросы клиентов
        acls.addAll(topic(CLIENT_API, PRODUCTS, READ, DESCRIBE));
        acls.add(group(CLIENT_API, CLIENT_API_CATALOG_GROUP));
        acls.addAll(topic(CLIENT_API, CLIENT_REQUESTS, WRITE, DESCRIBE));
        // Kafka Connect пишет разрешённые товары в файл
        acls.addAll(topic(CONNECT, PRODUCTS, READ, DESCRIBE));
        acls.add(group(CONNECT, CONNECT_FILE_SINK_GROUP));
        // Mirror Maker 2 копирует топики во второй кластер (читает без группы консьюмеров).
        // DESCRIBE_CONFIGS: перед созданием копии топика он читает настройки исходного
        acls.addAll(topic(MIRROR_MAKER, PRODUCTS, READ, DESCRIBE, DESCRIBE_CONFIGS));
        acls.addAll(topic(MIRROR_MAKER, CLIENT_REQUESTS, READ, DESCRIBE, DESCRIBE_CONFIGS));
        return acls;
    }

    private static List<AclBinding> topic(String principal, String topic, AclOperation... operations) {
        List<AclBinding> acls = new ArrayList<>();
        for (AclOperation operation : operations) {
            acls.add(allow(principal, ResourceType.TOPIC, topic, operation));
        }
        return acls;
    }

    private static AclBinding group(String principal, String group) {
        return allow(principal, ResourceType.GROUP, group, READ);
    }

    private static AclBinding allow(String principal, ResourceType type, String name, AclOperation operation) {
        return new AclBinding(new ResourcePattern(type, name, PatternType.LITERAL),
                new AccessControlEntry("User:" + principal, "*", operation, AclPermissionType.ALLOW));
    }

    private static void createTopics(Admin admin, List<NewTopic> topics) throws Exception {
        Set<String> existing = admin.listTopics().names().get();
        for (NewTopic topic : topics) {
            if (existing.contains(topic.name())) {
                log.info("Топик {} уже существует", topic.name());
                continue;
            }
            admin.createTopics(List.of(topic)).all().get();
            log.info("Создан топик {}: партиций {}, реплик {}, настройки {}",
                    topic.name(), topic.numPartitions(), topic.replicationFactor(), topic.configs());
        }
    }

    // Брокеры стартуют дольше, чем этот контейнер: ждём, пока в кластере зарегистрируются все
    private static void waitForBrokers(Admin admin, int expected, String cluster) throws InterruptedException {
        for (int attempt = 1; attempt <= 60; attempt++) {
            try {
                int brokers = admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).size();
                if (brokers >= expected) {
                    log.info("Кластер {}: брокеров {}", cluster, brokers);
                    return;
                }
                log.info("Кластер {}: брокеров {} из {}, ждём", cluster, brokers, expected);
            } catch (Exception e) {
                log.info("Кластер {} пока недоступен: {}", cluster, e.getMessage());
            }
            TimeUnit.SECONDS.sleep(3);
        }
        throw new IllegalStateException("Кластер " + cluster + " не запустился");
    }
}
