-- Каталог разрешённых товаров: его заполняет client/CatalogLoader из топика products, по нему ищет CLIENT API
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(64) PRIMARY KEY,
    name       TEXT        NOT NULL,
    description TEXT,
    category   TEXT,
    brand      TEXT,
    price      NUMERIC(12, 2),
    currency   VARCHAR(8),
    available  INTEGER,
    store_id   VARCHAR(64),
    data       JSONB       NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

-- Журнал запросов клиентов (поиск и рекомендации)
CREATE TABLE IF NOT EXISTS client_requests (
    id            BIGSERIAL PRIMARY KEY,
    user_id       VARCHAR(64) NOT NULL,
    request_type  VARCHAR(32) NOT NULL,
    query         TEXT,
    results_count INTEGER     NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
