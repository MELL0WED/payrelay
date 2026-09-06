package com.payrelay.payment_intake_service.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-21479f67-satyapnayak2022-455c.a.aivencloud.com:28847");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put("security.protocol", "SASL_SSL");
        config.put("sasl.mechanism", "PLAIN");
        config.put(
                "sasl.jaas.config",
                String.format(
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                        System.getenv("KAFKA_USERNAME"),
                        System.getenv("KAFKA_PASSWORD")
                )
        );
        config.put("ssl.truststore.type", "PEM");
        config.put("ssl.truststore.certificates", CA_CERT);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
    }

    private static final String CA_CERT = """
-----BEGIN CERTIFICATE-----
MIIERDCCAqygAwIBAgIUCnJQVQyfBK/noY9JqGx6csWYDn8wDQYJKoZIhvcNAQEM
BQAwOjE4MDYGA1UEAwwvNGUwZTA1YTUtODk5Zi00NGJiLTk0MDMtMmY2NDgzN2Ni
ZjFiIFByb2plY3QgQ0EwHhcNMjYwOTA2MTIxMjA1WhcNMzYwOTAzMTIxMjA1WjA6
MTgwNgYDVQQDDC80ZTBlMDVhNS04OTlmLTQ0YmItOTQwMy0yZjY0ODM3Y2JmMWIg
UHJvamVjdCBDQTCCAaIwDQYJKoZIhvcNAQEBBQADggGPADCCAYoCggGBANW01l5S
wzKlX5vHYpHpPjihXT9fyKn/TQnf/M5OvbGbuWgzB/uPSMNKhLdX5s+HToNlVsIy
hR0353b9nLIAuKLVxkvmmgBRvoX3zmYTZ2IS56dL85io8sA2L6rV1e52G0U1l00m
Rfj/3qRinW+2uCijolBL42NmsdI8TZZ1fP5sQHR4yx0OlK0IxTTdSrjcnntdLcbw
qAD1QpuAIbOa6OgpNPxJLa9QRMigY92Zh41L5mA869gO+uBCk8SX8Ltvs7wJxB0L
PqUMPYcvDCXJgXHzNJAgGwcwMGiYIM55+iUhazzIf34B1Cj1YO02DYSu4Pgya9q6
h92Q4QdlNy7spgulPBL/vJEr6ZF/lzAGu9C8AKjrIw6jJRFOIYIYNg4+UOcsdaOR
j7iHXN50q36u5z5Xo/5QJc0SZ0yTBbTJzfTzqG8Sw/ER367eMiGqtG+atqL6Hz6S
wo0ltqz/B61OtozfALSMQit6jqxYA45lm0EbcJFv6vLVUEE6ceyDUhssRwIDAQAB
o0IwQDAdBgNVHQ4EFgQUxQ98uYjO9y9+7Yl8k5yB9AsDrh8wEgYDVR0TAQH/BAgw
BgEB/wIBADALBgNVHQ8EBAMCAQYwDQYJKoZIhvcNAQEMBQADggGBAHubmP5ZU1ZY
/QQwL/4aMBX+EFhruhoTtw3h9xlwg96c+ho0ew4sOdf9p0Sz0ASrMF+AMT62jWi+
5sbCFZfMkNUEAKDqcdVpTbueo83aI/BXEKIoNsPVH1qXk1/QVOoKL5uPa32nBhcR
IxpwD8QgmEJfjsfBuXndJrcvS+3Y2Ei/E7CwtnKjW8QMJX0Qb3IEa3wTP3UR0ATT
Fkd3eFV9RmoO+fev5rYXoH1i9Y+Iryk/lYuIM0kheb8AdXHWlAEIrtV/b24eYkDp
ABrlStDyKduFDBAprp6SWBcNKxJ87+XaahKVI1ziu7m10Fl4j4zXrJiWA0sF97Ue
HiVe5HS6fuKZ6yDAM2k4Eb1KiYS+dDhOpOPxNtN2H90Dd3wtbqqFtq03Ln8NWJzh
lMOr72ge/FtfbsVJIzLR+6md4q68vgXppUMXnSsVbLLigAZpnsGznJsdRx89MWvh
3LhetqD5WDfklHFgg08FSzRMOjyLQLeGTokLTXfK5JvgrYvZY2YZsA==
-----END CERTIFICATE-----
""";
}