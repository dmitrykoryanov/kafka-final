# Аналитическая платформа маркетплейса «Покупай выгодно»

Итоговый проект курса по Apache Kafka. Магазины присылают товары (SHOP API), фильтр Kafka Streams отбрасывает запрещённые товары, клиенты ищут товары и получают персональные рекомендации (CLIENT API). Данные дублируются во второй кластер Kafka, оттуда аналитика на Spark переносит их в HDFS и считает рекомендации. Кластеры мониторятся через Prometheus, Grafana и Alertmanager.

Команды выпуска сертификатов — в [COMMANDS.md](COMMANDS.md).

## Содержание

- [Схема](#схема)
- [Что реализовано](#что-реализовано)
- [Соответствие критериям](#соответствие-критериям)
- [Использованные технологии](#использованные-технологии)
- [Архитектура и логика работы](#архитектура-и-логика-работы)
- [Запуск](#запуск)
- [Проверка по шагам](#проверка-по-шагам)
- [Адреса и порты](#адреса-и-порты)
- [Остановка и сброс](#остановка-и-сброс)
- [Упрощения](#упрощения)

## Схема

![Архитектура платформы](docs/architecture.svg)

Как идут данные:

1. SHOP API отправляет товары магазинов в топик `shop-products`. Список запрещённых товаров хранится в топике `banned-products`, его меняют команды `ban-add` и `ban-remove`.
2. Фильтр Kafka Streams сверяет каждый товар со списком запрещённых и пропускает в топик `products` только разрешённые.
3. Из `products` товары попадают в файл `storage/products.jsonl` (Kafka Connect) и в каталог PostgreSQL (загрузка каталога), по которому ищет CLIENT API.
4. CLIENT API записывает каждый запрос клиента в топик `client-requests` и в PostgreSQL.
5. Mirror Maker 2 копирует `products` и `client-requests` во второй кластер: там это `primary.products` и `primary.client-requests`.
6. Spark раз в 30 секунд переносит эти данные в HDFS, считает рекомендации и пишет их в топик `recommendations`, откуда их читает CLIENT API.
7. Prometheus собирает метрики всех брокеров обоих кластеров через JMX Exporter, Grafana показывает их на дашборде, Alertmanager отправляет письма о сбоях в Mailpit.

## Что реализовано

| Шаг задания | Реализация |
|---|---|
| 1. SHOP API | Команда `shop-load` читает товары из [data/products.json](data/products.json) (14 товаров в формате из задания) и отправляет их в топик `shop-products`, ключ — `product_id` |
| 1. CLIENT API | Команды `search <user_id> <текст>` (поиск товара по названию в PostgreSQL) и `recommend <user_id>` (готовые рекомендации из топика `recommendations`). Каждый запрос пишется в Kafka (`client-requests`) и в PostgreSQL (`client_requests`) |
| 2. Кластер Kafka, TLS | 3 брокера `cp-kafka:7.4.4` (ZooKeeper). Все порты — SSL с обязательным клиентским сертификатом (mTLS), брокеры общаются между собой тоже по SSL |
| 2. Топики | Создаёт `kafka-setup` (Java, `setup/KafkaSetup`): `shop-products`, `banned-products` (compacted), `products`, `client-requests` — по 3 партиции; во втором кластере `recommendations` |
| 2. ACL | `AclAuthorizer`: у каждого компонента свой сертификат и только нужные права, каждый топик пишет один пользователь. Права выдаёт `kafka-setup`, проверяет команда `acl-check` |
| 2. Репликация | Replication factor 3, `min.insync.replicas=2`, продюсеры с `acks=all`: кластер переживает потерю одного брокера без потери записи |
| 2. Второй кластер | 1 брокер. Mirror Maker 2 копирует в него `products` и `client-requests` (там они называются `primary.products`, `primary.client-requests`) |
| 3. Аналитика (базовый вариант) | Задача Spark на Java (`analytics/RecommendationJob`) раз в 30 секунд переносит данные второго кластера в HDFS, считает рекомендации Spark SQL и пишет их в топик `recommendations` |
| 4. Потоковая обработка | Kafka Streams (`stream/ProductFilterTopology`): `shop-products` соединяется с GlobalKTable списка запрещённых товаров, в `products` проходят только разрешённые. Список управляется командами `ban-add`, `ban-remove`, `ban-list` |
| 5. Хранилище (базовый вариант) | Kafka Connect (FileStreamSink) пишет топик `products` — только отфильтрованные товары — в файл `storage/products.jsonl` |
| 6. Мониторинг | JMX Exporter (Java-агент в брокерах) → Prometheus → дашборд Grafana «Kafka — состояние кластеров»; алерты `KafkaBrokerDown` и `KafkaUnderReplicatedPartitions` → Alertmanager → письмо в Mailpit |
| 7. Документация | Этот файл |

## Соответствие критериям

| Критерий | Где реализовано | Как проверить |
|---|---|---|
| Kafka передаёт данные между сервисами | SHOP API → фильтр → Kafka Connect, каталог PostgreSQL, Mirror Maker 2 → аналитика → CLIENT API, см. [схему](#схема) | шаги [1](#шаг-1-список-запрещённых-товаров-и-потоковая-фильтрация)–[5](#шаг-5-аналитика-hdfs-spark-рекомендации) |
| Включён TLS, работают ACL | `docker-compose.yml` (SSL, `ssl.client.auth=required`, `AclAuthorizer`), `setup/KafkaSetup` (права) | [шаг 6](#шаг-6-tls-и-acl) |
| Репликация топиков, минимальное число реплик | 3 реплики, `min.insync.replicas=2`, `acks=all` | [шаг 6](#шаг-6-tls-и-acl) (`kafka-topics --describe`), [шаг 7](#шаг-7-мониторинг-и-отказоустойчивость) (работа без одного брокера) |
| Дублирование во второй кластер | Mirror Maker 2, `config/mirror-maker/mm2.properties` | [шаг 4](#шаг-4-второй-кластер-mirror-maker-2) |
| Фильтрация запрещённых товаров | Kafka Streams, `stream/ProductFilterTopology`; список — команды `ban-*` | [шаг 1](#шаг-1-список-запрещённых-товаров-и-потоковая-фильтрация) |
| Отфильтрованные данные в системе хранения | Kafka Connect → `storage/products.jsonl`; каталог PostgreSQL | [шаг 2](#шаг-2-хранилище-файл-kafka-connect), [шаг 3](#шаг-3-client-api-поиск-и-запросы-клиентов) |
| Аналитическая обработка | HDFS + Spark, `analytics/RecommendationJob` | [шаг 5](#шаг-5-аналитика-hdfs-spark-рекомендации) |
| Рекомендации в отдельном топике | топик `recommendations` второго кластера | [шаг 5](#шаг-5-аналитика-hdfs-spark-рекомендации) |
| Метрики собираются через Prometheus и JMX Exporter | Java-агент JMX Exporter в каждом брокере, `config/prometheus` | [шаг 7](#шаг-7-мониторинг-и-отказоустойчивость) (цели Prometheus) |
| Дашборд Grafana отображает ключевые метрики | `config/grafana/dashboards/kafka-cluster.json` | [шаг 7](#шаг-7-мониторинг-и-отказоустойчивость) |
| Alertmanager отправляет оповещения при сбоях | алерты `config/prometheus/alert-rules.yml`, рассылка по почте `config/alertmanager/alertmanager.yml` | [шаг 7](#шаг-7-мониторинг-и-отказоустойчивость) (письма в Mailpit) |
| README: запуск, технологии, архитектура и логика | этот файл | разделы [Запуск](#запуск), [Использованные технологии](#использованные-технологии), [Архитектура и логика работы](#архитектура-и-логика-работы) |

## Использованные технологии

| Инструмент | Версия | Зачем |
|---|---|---|
| Apache Kafka (Confluent Platform `cp-kafka`, `cp-zookeeper`) | 7.4.4 (Kafka 3.4) | оба кластера; в том же образе — Mirror Maker 2, Kafka Connect и JMX Exporter |
| Java, Spring Boot | 17, 4.1.2 | консольное приложение платформы и `kafka-setup` |
| kafka-clients, Kafka Streams | 4.2 | продюсеры, консьюмеры, фильтр запрещённых товаров |
| Mirror Maker 2 | из Kafka 3.4 | дублирование данных во второй кластер |
| Kafka Connect, FileStreamSinkConnector | из Kafka 3.4 | запись отфильтрованных товаров в файл |
| PostgreSQL | 16 | каталог товаров для поиска, журнал запросов клиентов |
| Apache Hadoop HDFS (`apache/hadoop`) | 3.4.1 | Data Lake |
| Apache Spark (`apache/spark`), spark-sql-kafka | 3.5.3 | аналитика: Kafka → HDFS → рекомендации → Kafka |
| Prometheus, Alertmanager, Grafana | 2.47.1, 0.27.0, 9.5.12 | мониторинг и оповещения |
| Mailpit | 1.33.0 | локальный почтовый сервер: принимает письма Alertmanager и показывает их в веб-интерфейсе |
| OpenSSL, keytool | — | сертификаты ([COMMANDS.md](COMMANDS.md)) |

## Архитектура и логика работы

### Кластеры

**Основной кластер** — `zookeeper`, `kafka1`, `kafka2`, `kafka3`. У каждого брокера два SSL-порта:

| Порт | Адрес | Кто подключается |
|---|---|---|
| `SSL` | `kafkaN:9092` (внутри Docker-сети) | другие брокеры, `kafka-setup`, Mirror Maker 2, Kafka Connect, утилиты Kafka в контейнерах |
| `EXTERNAL` | `localhost:9093` / `9094` / `9095` | консольное приложение на хосте |

Брокеры требуют клиентский сертификат, подписанный собственным CA проекта (`KAFKA_SSL_CLIENT_AUTH: required`). Брокер берёт имя пользователя из CN сертификата: `CN=shop-api` → `User:shop-api`. Сертификаты и хранилища лежат в `certs`, команды их выпуска — в [COMMANDS.md](COMMANDS.md).

Автосоздание топиков выключено, по умолчанию у топиков 3 реплики и `min.insync.replicas=2`. Запись с `acks=all` подтверждается, когда данные есть минимум на двух брокерах, поэтому при остановке одного брокера запись и чтение продолжаются.

**Второй кластер** — `zookeeper-b`, `kafka-b`, без TLS: `kafka-b:9092` внутри Docker-сети, `localhost:19092` с хоста.

### Топики

| Кластер | Топик | Ключ | Значение | Пишет | Читают |
|---|---|---|---|---|---|
| основной | `shop-products` | `product_id` | JSON товара (формат из задания) | SHOP API | фильтр |
| основной | `banned-products` (compacted) | `product_id` | `{"product_id", "reason", "banned_at"}`, `null` — запрет снят | ban-manager | фильтр (GlobalKTable) |
| основной | `products` | `product_id` | JSON разрешённого товара | фильтр | Kafka Connect, загрузка каталога, Mirror Maker 2 |
| основной | `client-requests` | `user_id` | `{"user_id", "type": SEARCH\|RECOMMENDATIONS, "query", "product_ids", "timestamp"}` | CLIENT API | Mirror Maker 2 |
| второй | `primary.products`, `primary.client-requests` | | копии от Mirror Maker 2 | Mirror Maker 2 | аналитика |
| второй | `recommendations` (compacted) | `user_id` | `{"user_id", "generated_at", "products": [{"rank", "product_id", "name", "category", "price", "currency"}]}` | аналитика | CLIENT API |

### Пользователи и права (ACL)

Права выдаёт `kafka-setup`. Всё, что не разрешено явно, запрещено (`AclAuthorizer`). Суперпользователи — брокеры и `admin`.

| Пользователь | Ресурс | Права |
|---|---|---|
| `User:shop-api` | топик `shop-products` | Write, Describe |
| `User:ban-manager` | топик `banned-products` | Write, Describe |
| `User:stream-filter` | топики `shop-products`, `banned-products` | Read, Describe |
| `User:stream-filter` | топик `products` | Write, Describe |
| `User:stream-filter` | группа `stream-filter` | Read |
| `User:client-api` | топик `products` | Read, Describe |
| `User:client-api` | группа `client-api-catalog` | Read |
| `User:client-api` | топик `client-requests` | Write, Describe |
| `User:connect` | топик `products` | Read, Describe |
| `User:connect` | группа `connect-products-file-sink` | Read |
| `User:mirror-maker` | топики `products`, `client-requests` | Read, Describe, DescribeConfigs |

### Консольное приложение (Java)

Одно Spring Boot приложение эмулирует SHOP API, CLIENT API и управление списком запрещённых товаров, а в фоне запускает фильтр Kafka Streams и загрузку каталога в PostgreSQL. Каждый компонент подключается к Kafka со своим сертификатом, поэтому права у них разные, как у отдельных сервисов. Код — в `src/main/java/com/dmitrykoryanov/kafka/kafkafinal`:

| Класс | Что делает |
|---|---|
| `console/ConsoleRunner` | запускает фоновые компоненты, читает и выполняет команды |
| `shop/ShopApi` | SHOP API: читает `data/products.json` и отправляет товары в `shop-products` (`User:shop-api`) |
| `ban/BanManager` | пишет запрет (`ban-add`) или tombstone (`ban-remove`) в `banned-products` (`User:ban-manager`) |
| `stream/ProductFilterTopology` | топология фильтра: `shop-products` leftJoin GlobalKTable(`banned-products`) по `product_id` → только разрешённые → `products` |
| `stream/ProductFilter` | запускает Kafka Streams (`User:stream-filter`), отдаёт список запретов из хранилища GlobalKTable для `ban-list` |
| `client/CatalogLoader` | читает `products` и сохраняет товары в таблицу `products` PostgreSQL (`User:client-api`) |
| `client/ClientApi` | CLIENT API: поиск в PostgreSQL, чтение рекомендаций; каждый запрос — в `client_requests` и в топик `client-requests` |
| `client/RecommendationReader` | читает последние рекомендации пользователя из `recommendations` второго кластера |
| `console/AclCheck` | команда `acl-check`: попытки сделать то, что клиентам не разрешено |
| `setup/KafkaSetup` | профиль `setup` (контейнер `kafka-setup`): создаёт топики и ACL, затем приложение завершается |
| `config/KafkaClients` | настройки клиентов: SSL и сертификат `certs/<пользователь>/<пользователь>.p12` |

Таблицы PostgreSQL создаёт `src/main/resources/schema.sql` при запуске консоли.

### Mirror Maker 2

Работает в выделенном режиме (`connect-mirror-maker`), настройки — [config/mirror-maker/mm2.properties](config/mirror-maker/mm2.properties). Направление одно, `primary → secondary`, копируются топики `products` и `client-requests`. Mirror Maker только читает основной кластер: служебный топик синхронизации смещений хранится во втором кластере, ACL и смещения групп не копируются.

### Kafka Connect

Standalone-режим: один процесс, коннектор описан в файле, служебные топики не нужны. Настройки — [config/kafka-connect](config/kafka-connect). `FileStreamSinkConnector` читает топик `products` (только товары, прошедшие фильтр) и дописывает каждое сообщение строкой JSON в `/data/products.jsonl`. Этот файл смонтирован в `storage/products.jsonl` проекта.

### Аналитика

Задача Spark (`analytics/src/main/java/com/dmitrykoryanov/kafka/analytics/RecommendationJob.java`) работает в контейнере `analytics` в local-режиме и повторяется раз в 30 секунд:

1. Читает топики `primary.products` и `primary.client-requests` второго кластера и перезаписывает ими каталоги `/datalake/products` и `/datalake/client-requests` в HDFS.
2. Читает Data Lake и считает рекомендации Spark SQL. Товары, которые пользователь нашёл через `search`, определяют интересные ему категории. Ему рекомендуются до 5 товаров этих категорий в наличии, которых он ещё не находил: сначала из самых интересных категорий, внутри категории — более дешёвые.
3. Пишет рекомендации в топик `recommendations` второго кластера, ключ — `user_id`.

### Мониторинг

- JMX Exporter (`jmx_prometheus_javaagent`, входит в образ `cp-kafka`) подключён к каждому брокеру как Java-агент и отдаёт метрики на порту 7071. Правила — [config/jmx-exporter/kafka-broker.yml](config/jmx-exporter/kafka-broker.yml).
- Prometheus собирает метрики четырёх брокеров с меткой `cluster` (`primary` / `secondary`).
- Grafana: дашборд «Kafka — состояние кластеров» показывает брокеры в работе и недоступные, активный контроллер, недореплицированные и офлайн-партиции, партиции ниже `min.insync.replicas`, поток сообщений по топикам, трафик, изменения ISR, партиции и лидеров по брокерам, память JVM.
- Алерты ([config/prometheus/alert-rules.yml](config/prometheus/alert-rules.yml)): `KafkaBrokerDown` — метрики брокера недоступны больше 30 секунд; `KafkaUnderReplicatedPartitions` — в кластере есть недореплицированные партиции. Prometheus отправляет их в Alertmanager.
- Alertmanager ([config/alertmanager/alertmanager.yml](config/alertmanager/alertmanager.yml)) рассылает оповещения по почте: отдельное письмо на каждый алерт и брокер, а после устранения сбоя — письмо о восстановлении (`RESOLVED`). Письма принимает Mailpit — почтовый сервер для проверки, который показывает их в веб-интерфейсе. Для настоящей почты достаточно заменить `smtp_smarthost` и адрес получателя.

## Запуск

### Требования

- Docker Desktop: контейнеры занимают около 6 ГБ памяти, Docker лучше выделить не меньше 8 ГБ.
- Доступ в интернет: при первом запуске скачиваются образы и коннектор Spark для Kafka.
- JDK 17 (Gradle собирает проект на 17), запускать можно и более новой Java.
- Для проверки TLS — `openssl` (есть в Git Bash).

Команды ниже выполняются из корня проекта. В bash вместо `.\gradlew.bat` используйте `./gradlew`.

### 1. Сборка

```powershell
.\gradlew.bat build
```

Собираются `build/libs/kafka-final.jar` (консольное приложение и `kafka-setup`) и `analytics/build/libs/analytics.jar` (задача Spark), выполняются тесты. Контейнеры `kafka-setup` и `analytics` берут эти jar из каталогов проекта, поэтому сборка — до `docker compose up`.

### 2. Запуск контейнеров

```powershell
docker compose up -d
docker compose ps -a
```

Через 1–2 минуты все контейнеры должны быть в состоянии `Up`, а `kafka-setup` — `Exited (0)`: он создал топики и ACL и завершился. Его отчёт:

```powershell
docker logs kafka-setup
```

```
Кластер основной: брокеров 3
Создан топик shop-products: партиций 3, реплик 3, настройки {cleanup.policy=delete, min.insync.replicas=2}
...
Права доступа (ACL) выданы: 25
...
Создан топик recommendations: партиций 3, реплик 1, настройки {cleanup.policy=compact}
Настройка кластеров завершена
```

Kafka Connect, Mirror Maker 2 и аналитика стартуют только после успешного завершения `kafka-setup`.

### 3. Консольное приложение

```powershell
chcp 65001
java -jar build/libs/kafka-final.jar
```

`chcp 65001` переключает консоль Windows на UTF-8, иначе кириллица отображается и вводится как `?`. Приложение запускается из корня проекта: пути к сертификатам и файлу товаров относительные. После строки `Фильтр Kafka Streams: RUNNING` появляется список команд и приглашение `>`:

```
shop-load                        SHOP API: отправить товары из файла в Kafka (топик shop-products)
ban-add <product_id> [причина]   запретить товар
ban-remove <product_id>          снять запрет
ban-list                         список запрещённых товаров
search <user_id> <текст>         CLIENT API: поиск товара по названию
recommend <user_id>              CLIENT API: персональные рекомендации
status                           состояние фильтра и каталога
acl-check                        проверить, что ACL не дают клиентам лишних прав
help                             эта справка
exit                             выход
```

Сообщения фоновых компонентов (фильтра и загрузки каталога) выводятся в ту же консоль со временем в начале строки.

## Проверка по шагам

Шаги 1–6 выполняются по порядку в одной консоли приложения. Команды `docker ...` удобно запускать во втором окне терминала.

### Шаг 1. Список запрещённых товаров и потоковая фильтрация

Запретите два товара и отправьте товары магазинов:

```
> ban-add 12350 продажа электронных сигарет запрещена
> ban-add 12357 оружие
> ban-list
> shop-load
```

Ожидается:

- `ban-list` показывает оба товара с причиной и временем запрета.
- `shop-load` отправляет 14 товаров в `shop-products` и печатает партицию и смещение каждого.
- Фильтр пишет для 12350 и 12357 `Фильтр: товар ... в списке запрещённых, дальше не передаётся`, для остальных 12 — `Фильтр: товар ... разрешён -> products`.
- Загрузка каталога 12 раз пишет `Каталог: товар ... сохранён в PostgreSQL`, `status` показывает `Товаров в каталоге PostgreSQL: 12`.

Снимите один запрет и отправьте товары ещё раз:

```
> ban-remove 12357
> shop-load
```

Теперь отбрасывается только 12350, товар 12357 проходит фильтр. Запрет действует на товары, которые поступают после него.

### Шаг 2. Хранилище: файл Kafka Connect

В файл `storage/products.jsonl` попадают только товары из топика `products`, то есть прошедшие фильтр:

```powershell
docker exec kafka-connect bash -c 'wc -l /data/products.jsonl; grep -c 12350 /data/products.jsonl'
```

Ожидается 25 строк (12 после первой отправки и 13 после второй) и `0` строк с запрещённым товаром 12350. Сам файл можно открыть в редакторе: одна строка — один товар в JSON. Состояние коннектора — http://localhost:8083/connectors/products-file-sink/status (`RUNNING`).

### Шаг 3. CLIENT API: поиск и запросы клиентов

```
> search user-1 часы
> search user-1 XYZ
> search user-2 кофе
> search user-1 сигарета
```

Ожидается: «часы» — `12345 Умные часы XYZ`, «XYZ» — часы и `12349 Фитнес-браслет XYZ Band`, «кофе» — `12351 Кофемашина Barista One`, «сигарета» — ничего (запрещённый товар не попал в каталог).

Запросы записаны в PostgreSQL:

```powershell
docker exec postgres psql -U marketplace -d marketplace -c "SELECT user_id, request_type, query, results_count, created_at FROM client_requests ORDER BY id"
```

и в топик `client-requests` основного кластера (читаем от имени `User:admin`):

```powershell
docker exec kafka1 bash -c 'kafka-console-consumer --bootstrap-server kafka1:9092 --consumer.config /etc/kafka/secrets/admin/admin.properties --topic client-requests --from-beginning --timeout-ms 5000'
```

```
{"user_id":"user-1","type":"SEARCH","query":"часы","timestamp":"...","product_ids":["12345"]}
...
```

Здесь и дальше `kafka-console-consumer` ждёт новые сообщения 5 секунд (`--timeout-ms`) и завершается. Строки `TimeoutException` и `Processed a total of N messages` в конце вывода — это нормально.

### Шаг 4. Второй кластер: Mirror Maker 2

```powershell
docker exec kafka-b kafka-topics --bootstrap-server kafka-b:9092 --list
docker exec kafka-b kafka-console-consumer --bootstrap-server kafka-b:9092 --topic primary.client-requests --from-beginning --timeout-ms 5000
```

В списке есть `primary.products`, `primary.client-requests` (копии топиков основного кластера), `recommendations`, а также служебные топики Mirror Maker (`mm2-*`, `heartbeats`). В `primary.client-requests` — те же запросы, что в шаге 3.

### Шаг 5. Аналитика: HDFS, Spark, рекомендации

Не позже чем через 30 секунд задача Spark перенесёт данные в HDFS и посчитает рекомендации:

```powershell
docker logs analytics 2>&1 | Select-String "analytics\]|user_id|user-"
```

(в bash: `docker logs analytics 2>&1 | grep -E "analytics\]|user"`)

```
11:02:05 [analytics] Итерация 7: в HDFS перенесено товаров 25 (hdfs://namenode:8020/datalake/products), запросов клиентов 4 (hdfs://namenode:8020/datalake/client-requests)
11:02:11 [analytics] Итерация 7: рекомендации для пользователей (2) записаны в топик recommendations
|user_id|recommended                                                           |
|user-1 |[Беспроводные наушники SoundPro, Смартфон Nova 12, Ноутбук AirBook 14]|
|user-2 |[Электрочайник AquaHeat, Робот-пылесос CleanBot]                      |
```

Data Lake в HDFS:

```powershell
docker exec namenode bash -c 'hdfs dfs -ls -R /datalake'
docker exec namenode bash -c 'hdfs dfs -cat "/datalake/client-requests/part-*"'
```

Его же можно посмотреть в веб-интерфейсе NameNode: http://localhost:9870 → Utilities → Browse the file system → `/datalake`.

Рекомендации в консоли приложения:

```
> recommend user-1
Рекомендации для user-1 (рассчитаны 2026-11-05T11:02:11):
  1. 12347 Беспроводные наушники SoundPro — Электроника, 7490.00 RUB
  2. 12346 Смартфон Nova 12 — Электроника, 29990.00 RUB
  3. 12348 Ноутбук AirBook 14 — Электроника, 74990.00 RUB
> recommend user-2
> recommend user-3
```

`user-1` искал часы и браслет XYZ (Электроника), поэтому ему рекомендуются другие товары этой категории, кроме уже найденных и запрещённой электронной сигареты. `user-2` искал кофемашину (Бытовая техника). Для `user-3`, который ничего не искал, рекомендаций нет.

Сами рекомендации в топике второго кластера:

```powershell
docker exec kafka-b kafka-console-consumer --bootstrap-server kafka-b:9092 --topic recommendations --from-beginning --property print.key=true --timeout-ms 5000
```

### Шаг 6. TLS и ACL

Проверка прав из консоли приложения: клиенты пытаются сделать то, что им не разрешено.

```
> acl-check
User:shop-api -> запись в products (SHOP API пишет напрямую в products, минуя фильтр запрещённых товаров)
  отказано: TopicAuthorizationException: Not authorized to access topics: [products]
User:client-api -> запись в banned-products (CLIENT API меняет список запрещённых товаров)
  отказано: TopicAuthorizationException: Not authorized to access topics: [banned-products]
User:connect -> запись в shop-products (Kafka Connect пишет в топик магазинов)
  отказано: TopicAuthorizationException: Not authorized to access topics: [shop-products]
User:client-api -> чтение shop-products (CLIENT API читает неотфильтрованные товары магазинов)
  отказано: TopicAuthorizationException: Not authorized to access topics: [shop-products]
```

Выданные права, настройки топика и реплики (утилиты Kafka от имени `User:admin`):

```powershell
docker exec kafka1 bash -c 'kafka-acls --bootstrap-server kafka1:9092 --command-config /etc/kafka/secrets/admin/admin.properties --list'
docker exec kafka1 bash -c 'kafka-topics --bootstrap-server kafka1:9092 --command-config /etc/kafka/secrets/admin/admin.properties --describe --topic products'
```

```
Topic: products	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2,cleanup.policy=delete
	Topic: products	Partition: 0	Leader: 3	Replicas: 3,1,2	Isr: 1,2,3
	...
```

TLS (в Git Bash). С клиентским сертификатом соединение устанавливается, ожидается `Verify return code: 0 (ok)`:

```bash
openssl s_client -connect localhost:9093 -tls1_3 -CAfile certs/ca.crt \
  -cert certs/shop-api/shop-api.crt -key certs/shop-api/shop-api.key </dev/null
```

Без сертификата брокер разрывает соединение, ожидается `tls alert bad certificate` (`SSL alert number 42`):

```bash
openssl s_client -connect localhost:9093 -tls1_3 -CAfile certs/ca.crt </dev/null
```

### Шаг 7. Мониторинг и отказоустойчивость

1. Grafana: http://localhost:3000 — дашборд «Kafka — состояние кластеров» открывается сразу, без входа (для редактирования войдите: `admin` / `kafka`). Вверху переключатель кластера: `primary` или `secondary`. В основном кластере ожидается: брокеров в работе 3, недоступных 0, активный контроллер 1, недореплицированных, офлайн-партиций и партиций ниже `min.insync.replicas` — 0. Ниже — графики потока сообщений по топикам, трафика, изменений ISR, партиций и лидеров, памяти JVM.
2. Метрики собирает Prometheus с JMX Exporter каждого брокера: http://localhost:9090/targets — 4 цели задания `kafka` в состоянии `UP`.
3. Остановите брокер:

   ```powershell
   docker stop kafka3
   ```

4. Через 30–40 секунд:
   - на дашборде «Брокеры недоступны» = 1, «Недореплицированные партиции» > 0;
   - в Prometheus (http://localhost:9090/alerts) алерты `KafkaBrokerDown` (брокер `kafka3:7071`) и `KafkaUnderReplicatedPartitions` в состоянии `FIRING`;
   - они же в Alertmanager: http://localhost:9096;
   - Alertmanager отправил оповещения по почте: в Mailpit (http://localhost:8025) письма `[FIRING:1] KafkaBrokerDown kafka3:7071 (primary kafka critical)` и `[FIRING:1] KafkaUnderReplicatedPartitions (primary warning)` с описанием сбоя.
5. Кластер продолжает работать на двух брокерах (`min.insync.replicas=2`): команды `shop-load` и `search` в консоли выполняются без ошибок. Клиенты при этом предупреждают, что не могут подключиться к `localhost:9095`.
6. Верните брокер: `docker start kafka3`. Алерты погаснут, недореплицированных партиций снова станет 0. Примерно через минуту в Mailpit придут письма о восстановлении: `[RESOLVED] KafkaBrokerDown ...` и `[RESOLVED] KafkaUnderReplicatedPartitions ...`.

Если панели Grafana пишут `Unauthorized`, браузер, скорее всего, хранит cookie другой Grafana с `localhost:3000`. Проект использует своё имя cookie (`kafka_final_grafana_session`), поэтому обычно достаточно обновить страницу. Если не помогло, удалите cookie для `localhost:3000` или войдите как `admin`.

## Адреса и порты

| Что | Адрес на хосте |
|---|---|
| Основной кластер Kafka (SSL) | `localhost:9093`, `localhost:9094`, `localhost:9095` |
| Второй кластер Kafka | `localhost:19092` |
| PostgreSQL (`marketplace` / `marketplace`, база `marketplace`) | `localhost:15432` |
| Kafka Connect REST API | http://localhost:8083 |
| HDFS NameNode | http://localhost:9870 |
| Spark UI (во время расчёта) | http://localhost:4040 |
| Prometheus | http://localhost:9090 |
| Alertmanager | http://localhost:9096 |
| Mailpit (письма Alertmanager) | http://localhost:8025 |
| Grafana (просмотр без входа; `admin` / `kafka` для редактирования) | http://localhost:3000 |

PostgreSQL опубликован на порту 15432, потому что стандартный 5432 часто занят локально установленным PostgreSQL. Alertmanager — на 9096, потому что 9093 занят брокером `kafka1`.

## Остановка и сброс

```powershell
docker compose down      # остановить; топики, ACL, данные PostgreSQL сохраняются в томах Docker
docker compose down -v   # остановить и удалить все данные
```

После `docker compose down -v` при следующем запуске `kafka-setup` снова создаст топики и ACL. Файл `storage/products.jsonl` лежит в каталоге проекта и не удаляется: Kafka Connect будет дописывать в него. Для чистого запуска удалите его вручную.

## Упрощения

Это учебный стенд, поэтому:

- закрытые ключи и пароль хранилищ (`your-password`) лежат в репозитории;
- второй кластер — один брокер без TLS и ACL;
- SHOP API, CLIENT API, фильтр и управление запретами работают в одном процессе, хотя у каждого компонента свой пользователь Kafka;
- аналитика на каждой итерации перечитывает топики второго кластера целиком и перезаписывает Data Lake;
- запрет товара не удаляет его из `products` и каталога, если товар прошёл фильтр раньше;
- оповещения уходят на локальный почтовый сервер Mailpit, а не на настоящий почтовый ящик; Telegram не подключён.
