# Сертификаты и хранилища ключей

Здесь собраны команды, которыми выпущены сертификаты основного кластера Kafka. Описание проекта и порядок проверки — в [README.md](README.md).

Сертификаты уже выпущены и лежат в каталоге `certs`, команды нужны только для перевыпуска. Перевыпускайте их при остановленном стенде (`docker compose down`). Пересобирать приложение не нужно: оно читает хранилища из `certs` при запуске.

**Как пользоваться**

- Команды выполняются в bash (Git Bash, WSL, Linux, macOS) из корня проекта. Нужны `openssl` и `keytool` (входит в JDK).
- Порядок важен: сначала CA, затем брокеры и клиенты (их сертификаты подписывает CA).
- Пароль всех хранилищ — `your-password`.
- Сертификаты действуют 10 лет.

## Что лежит в `certs`

| Файл | Что это | Кто использует |
|---|---|---|
| `ca.cnf`, `broker.cnf`, `client.cnf` | параметры сертификатов CA, брокеров и клиентов. Имя (CN) брокера или клиента подставляется из переменной окружения `CN` | `openssl` |
| `ca.key`, `ca.crt` | закрытый ключ и сертификат собственного удостоверяющего центра (CA) | подписывает все сертификаты |
| `truststore.p12` | сертификат CA в формате PKCS12: по нему брокеры и клиенты проверяют друг друга | все участники |
| `password.txt` | пароль хранилищ для образа `cp-kafka` (`KAFKA_SSL_*_CREDENTIALS`). Перевод строки — только LF (см. `.gitattributes`) | брокеры |
| `<имя>/<имя>.key`, `.csr`, `.crt` | закрытый ключ, запрос на подпись и подписанный сертификат участника | промежуточные файлы |
| `<имя>/<имя>.p12` | keystore участника: ключ, сертификат и цепочка CA | брокер или клиент `<имя>` |
| `admin/admin.properties` | настройки подключения утилит Kafka (`kafka-topics`, `kafka-acls`) от имени `User:admin` внутри контейнеров брокеров | CLI |

Каталог `certs` смонтирован в брокеры, Kafka Connect и Mirror Maker как `/etc/kafka/secrets`, а в контейнер `kafka-setup` — как `/app/certs`. Консольное приложение читает его относительно рабочего каталога (корня проекта).

## 1. Удостоверяющий центр (CA)

```bash
# Закрытый ключ CA и самоподписанный сертификат CA (CN=kafka-final-ca)
openssl req -new -x509 -nodes -days 3650 -newkey rsa:2048 \
  -keyout certs/ca.key -out certs/ca.crt -config certs/ca.cnf

# Truststore с сертификатом CA — общий для брокеров и клиентов
keytool -import -noprompt -alias ca -file certs/ca.crt \
  -keystore certs/truststore.p12 -storetype PKCS12 -storepass your-password

# Пароль хранилищ для образа cp-kafka (printf пишет перевод строки LF)
printf 'your-password\n' > certs/password.txt
```

## 2. Брокеры основного кластера

Сертификат брокера годится и для сервера, и для клиента (`serverAuth`, `clientAuth`): при репликации брокеры подключаются друг к другу. Альтернативные имена (SAN) — `kafkaN` (адрес внутри Docker-сети) и `localhost` (адрес с хоста): по ним клиенты проверяют, что подключились к нужному брокеру.

Команды для `kafka1`. Для `kafka2` и `kafka3` повторите их, заменив значение `N`:

```bash
N=kafka1
mkdir -p certs/$N

# 1. Закрытый ключ и запрос на подпись. CN и SAN берутся из переменной CN
CN=$N openssl req -new -newkey rsa:2048 -nodes \
  -keyout certs/$N/$N.key -out certs/$N/$N.csr -config certs/broker.cnf

# 2. CA подписывает запрос, расширения (SAN, serverAuth, clientAuth) — из секции v3_req
CN=$N openssl x509 -req -days 3650 -in certs/$N/$N.csr \
  -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial \
  -out certs/$N/$N.crt -extfile certs/broker.cnf -extensions v3_req

# 3. Keystore брокера: ключ, сертификат и цепочка CA (KAFKA_SSL_KEYSTORE_FILENAME)
openssl pkcs12 -export -in certs/$N/$N.crt -inkey certs/$N/$N.key \
  -chain -CAfile certs/ca.crt -name $N \
  -out certs/$N/$N.p12 -password pass:your-password
```

## 3. Клиенты

У каждого компонента платформы свой сертификат. Брокер берёт имя пользователя из CN сертификата: `CN=shop-api` → `User:shop-api` (правило `KAFKA_SSL_PRINCIPAL_MAPPING_RULES` в `docker-compose.yml`). Права пользователей выдаёт `kafka-setup`, они перечислены в [README.md](README.md#пользователи-и-права-acl).

| Клиент (CN) | Кто подключается |
|---|---|
| `admin` | суперпользователь: `kafka-setup`, утилиты `kafka-topics` и `kafka-acls` |
| `shop-api` | SHOP API |
| `ban-manager` | управление списком запрещённых товаров |
| `stream-filter` | фильтр Kafka Streams |
| `client-api` | CLIENT API и загрузка каталога в PostgreSQL |
| `connect` | Kafka Connect |
| `mirror-maker` | Mirror Maker 2 |

Команды для `shop-api`. Для остальных клиентов повторите их, заменив значение `N`:

```bash
N=shop-api
mkdir -p certs/$N

# 1. Закрытый ключ и запрос на подпись
CN=$N openssl req -new -newkey rsa:2048 -nodes \
  -keyout certs/$N/$N.key -out certs/$N/$N.csr -config certs/client.cnf

# 2. CA подписывает запрос: сертификат только для аутентификации клиента (clientAuth)
CN=$N openssl x509 -req -days 3650 -in certs/$N/$N.csr \
  -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial \
  -out certs/$N/$N.crt -extfile certs/client.cnf -extensions v3_req

# 3. Keystore клиента
openssl pkcs12 -export -in certs/$N/$N.crt -inkey certs/$N/$N.key \
  -chain -CAfile certs/ca.crt -name $N \
  -out certs/$N/$N.p12 -password pass:your-password
```

## Проверка

```bash
# Все сертификаты подписаны нашим CA
openssl verify -CAfile certs/ca.crt certs/*/*.crt

# Имя и альтернативные имена брокера
openssl x509 -in certs/kafka1/kafka1.crt -noout -subject -ext subjectAltName

# Содержимое keystore
keytool -list -keystore certs/kafka1/kafka1.p12 -storepass your-password
```
