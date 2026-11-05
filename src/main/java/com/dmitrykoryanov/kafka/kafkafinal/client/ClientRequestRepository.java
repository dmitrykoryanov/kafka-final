package com.dmitrykoryanov.kafka.kafkafinal.client;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

// Журнал запросов клиентов в PostgreSQL (таблица client_requests, см. schema.sql)
@Repository
@RequiredArgsConstructor
public class ClientRequestRepository {

    private final JdbcTemplate jdbc;

    public void save(String userId, String type, String query, int resultsCount) {
        jdbc.update("INSERT INTO client_requests (user_id, request_type, query, results_count) VALUES (?, ?, ?, ?)",
                userId, type, query, resultsCount);
    }
}
