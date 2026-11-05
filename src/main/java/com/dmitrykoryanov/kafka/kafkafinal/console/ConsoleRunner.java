package com.dmitrykoryanov.kafka.kafkafinal.console;

import com.dmitrykoryanov.kafka.kafkafinal.ban.BanManager;
import com.dmitrykoryanov.kafka.kafkafinal.client.CatalogLoader;
import com.dmitrykoryanov.kafka.kafkafinal.client.ClientApi;
import com.dmitrykoryanov.kafka.kafkafinal.client.ProductRepository;
import com.dmitrykoryanov.kafka.kafkafinal.client.ProductRepository.Product;
import com.dmitrykoryanov.kafka.kafkafinal.shop.ShopApi;
import com.dmitrykoryanov.kafka.kafkafinal.stream.ProductFilter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// Консоль платформы: запускает фоновые компоненты (фильтр Kafka Streams и загрузку каталога в PostgreSQL)
// и выполняет команды SHOP API, CLIENT API и управления списком запрещённых товаров
@Component
@Profile("!setup")
@RequiredArgsConstructor
public class ConsoleRunner implements CommandLineRunner {

    private static final String HELP = """
            Команды:
              shop-load                        SHOP API: отправить товары из файла в Kafka (топик shop-products)
              ban-add <product_id> [причина]   запретить товар
              ban-remove <product_id>          снять запрет
              ban-list                         список запрещённых товаров
              search <user_id> <текст>         CLIENT API: поиск товара по названию
              recommend <user_id>              CLIENT API: персональные рекомендации
              status                           состояние фильтра и каталога
              acl-check                        проверить, что ACL не дают клиентам лишних прав
              help                             эта справка
              exit                             выход""";

    private final ApplicationContext context;
    private final ProductFilter productFilter;
    private final CatalogLoader catalogLoader;
    private final ShopApi shopApi;
    private final BanManager banManager;
    private final ClientApi clientApi;
    private final ProductRepository products;
    private final AclCheck aclCheck;
    private final JsonMapper mapper = new JsonMapper();

    @Override
    public void run(String... args) throws Exception {
        System.out.println("Запускаем фильтр Kafka Streams и загрузку каталога в PostgreSQL...");
        productFilter.start();
        catalogLoader.start();
        waitForFilter();
        System.out.println(HELP);

        Console console = System.console();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String line;
            if (console != null) {
                line = console.readLine("> ");
            } else {
                System.out.print("> ");
                line = reader.readLine();
            }
            if (line == null || line.trim().equals("exit")) {
                break;
            }
            if (!line.isBlank()) {
                execute(line.trim());
            }
        }
        System.exit(SpringApplication.exit(context));
    }

    private void execute(String line) {
        String[] words = line.split("\\s+", 3);
        try {
            switch (words[0]) {
                case "shop-load" -> shopLoad();
                case "ban-add" -> banAdd(arg(words, 1, "product_id"), words.length > 2 ? words[2] : "запрещён к продаже");
                case "ban-remove" -> banRemove(arg(words, 1, "product_id"));
                case "ban-list" -> banList();
                case "search" -> search(arg(words, 1, "user_id"), arg(words, 2, "текст"));
                case "recommend" -> recommend(arg(words, 1, "user_id"));
                case "status" -> status();
                case "acl-check" -> aclCheck.run();
                case "help" -> System.out.println(HELP);
                default -> System.out.println("Неизвестная команда. Список команд: help");
            }
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {
            System.out.println("Ошибка: " + e);
        }
    }

    private void shopLoad() throws Exception {
        System.out.println("SHOP API: отправляем товары из " + shopApi.productsFile());
        int sent = shopApi.sendProducts();
        System.out.println("Отправлено товаров: " + sent);
    }

    private void banAdd(String productId, String reason) throws Exception {
        banManager.ban(productId, reason);
        System.out.printf("Товар %s добавлен в список запрещённых (%s)%n", productId, reason);
    }

    private void banRemove(String productId) throws Exception {
        banManager.unban(productId);
        System.out.printf("Запрет с товара %s снят%n", productId);
    }

    private void banList() {
        Map<String, String> banned = productFilter.bannedProducts();
        if (banned.isEmpty()) {
            System.out.println("Список запрещённых товаров пуст");
            return;
        }
        System.out.println("Запрещённые товары:");
        banned.forEach((productId, ban) -> {
            JsonNode json = mapper.readTree(ban);
            System.out.printf("  %s — %s (с %s)%n", productId, json.path("reason").asString(""),
                    json.path("banned_at").asString(""));
        });
    }

    private void search(String userId, String text) throws Exception {
        List<Product> found = clientApi.search(userId, text);
        if (found.isEmpty()) {
            System.out.printf("По запросу «%s» ничего не найдено%n", text);
        } else {
            System.out.printf("Найдено по запросу «%s»: %d%n", text, found.size());
            found.forEach(product -> System.out.printf("  %s %s — %s, %s, %s %s, в наличии %d, магазин %s%n",
                    product.productId(), product.name(), product.category(), product.brand(), product.price(),
                    product.currency(), product.available(), product.storeId()));
        }
        System.out.println("Запрос записан в PostgreSQL (client_requests) и в Kafka (client-requests)");
    }

    private void recommend(String userId) throws Exception {
        Optional<String> recommendations = clientApi.recommendations(userId);
        if (recommendations.isEmpty()) {
            System.out.printf("Рекомендаций для %s пока нет. Они появятся после поиска и ближайшего расчёта аналитики "
                    + "(раз в 30 секунд)%n", userId);
            return;
        }
        JsonNode json = mapper.readTree(recommendations.get());
        System.out.printf("Рекомендации для %s (рассчитаны %s):%n", userId, json.path("generated_at").asString(""));
        for (JsonNode product : json.path("products")) {
            System.out.printf("  %d. %s %s — %s, %.2f %s%n", product.path("rank").asInt(),
                    product.path("product_id").asString(""), product.path("name").asString(""),
                    product.path("category").asString(""), product.path("price").asDouble(),
                    product.path("currency").asString(""));
        }
    }

    private void status() {
        System.out.println("Фильтр Kafka Streams: " + productFilter.state());
        System.out.println("Товаров в каталоге PostgreSQL: " + products.count());
    }

    private void waitForFilter() throws InterruptedException {
        for (int i = 0; i < 60 && productFilter.state() != KafkaStreams.State.RUNNING; i++) {
            Thread.sleep(500);
        }
        System.out.println("Фильтр Kafka Streams: " + productFilter.state());
    }

    private static String arg(String[] words, int index, String name) {
        if (words.length <= index || words[index].isBlank()) {
            throw new IllegalArgumentException("Не указан параметр <" + name + ">. Список команд: help");
        }
        return words[index];
    }
}
