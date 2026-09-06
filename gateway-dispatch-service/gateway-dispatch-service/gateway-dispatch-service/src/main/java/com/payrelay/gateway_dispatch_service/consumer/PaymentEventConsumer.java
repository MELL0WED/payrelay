package com.payrelay.gateway_dispatch_service.consumer;

import com.payrelay.gateway_dispatch_service.gateway.GatewaySimulator;
import com.payrelay.gateway_dispatch_service.model.DispatchAttempt;
import com.payrelay.gateway_dispatch_service.repository.DispatchAttemptRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class PaymentEventConsumer {

    private final GatewaySimulator gatewaySimulator;
    private final DispatchAttemptRepository dispatchAttemptRepository;
    private final StringRedisTemplate redisTemplate;

    private static final Pattern PAYMENT_ID_PATTERN = Pattern.compile("\"paymentId\":(\\d+)");
    private static final Pattern IDEMPOTENCY_KEY_PATTERN = Pattern.compile("\"idempotencyKey\":\"([^\"]+)\"");
    private static final int MAX_RETRIES = 3;

    public PaymentEventConsumer(GatewaySimulator gatewaySimulator,
                                DispatchAttemptRepository dispatchAttemptRepository,
                                StringRedisTemplate redisTemplate) {
        this.gatewaySimulator = gatewaySimulator;
        this.dispatchAttemptRepository = dispatchAttemptRepository;
        this.redisTemplate = redisTemplate;
    }

    @KafkaListener(topics = "payment-created", groupId = "gateway-dispatch", containerFactory = "kafkaListenerContainerFactory")
    public void handle(String eventJson) {
        System.out.println("RAW MESSAGE RECEIVED: " + eventJson);
        Long paymentId = extract(PAYMENT_ID_PATTERN, eventJson).map(Long::parseLong).orElse(null);
        String idempotencyKey = extract(IDEMPOTENCY_KEY_PATTERN, eventJson).orElse(null);

        if (paymentId == null || idempotencyKey == null) {
            System.err.println("GATEWAY-DISPATCH: could not parse event: " + eventJson);
            return;
        }

        String statusKey = "dispatch:status:" + idempotencyKey;
        String existingStatus = redisTemplate.opsForValue().get(statusKey);
        if ("SUCCESS".equals(existingStatus)) {
            System.out.println("GATEWAY-DISPATCH: idempotencyKey=" + idempotencyKey + " already SUCCEEDED, skipping re-dispatch");
            return;
        }

        String retryCountKey = "dispatch:retries:" + idempotencyKey;
        Long currentRetry = redisTemplate.opsForValue().increment(retryCountKey);
        if (currentRetry != null && currentRetry == 1L) {
            redisTemplate.expire(retryCountKey, Duration.ofHours(1));
        }

        if (currentRetry != null && currentRetry > MAX_RETRIES) {
            recordAttempt(paymentId, idempotencyKey, "DEAD_LETTER");
            redisTemplate.opsForValue().set(statusKey, "DEAD_LETTER", Duration.ofHours(1));
            System.out.println("GATEWAY-DISPATCH: idempotencyKey=" + idempotencyKey + " exceeded max retries, moved to DEAD_LETTER");
            return;
        }

        String outcome = gatewaySimulator.call(idempotencyKey);
        recordAttempt(paymentId, idempotencyKey, outcome);

        if ("SUCCESS".equals(outcome)) {
            redisTemplate.opsForValue().set(statusKey, "SUCCESS", Duration.ofHours(1));
        }

        System.out.println("GATEWAY-DISPATCH: paymentId=" + paymentId + " idempotencyKey=" + idempotencyKey
                + " attempt=" + currentRetry + " outcome=" + outcome);
    }

    private void recordAttempt(Long paymentId, String idempotencyKey, String outcome) {
        dispatchAttemptRepository.save(new DispatchAttempt(null, paymentId, idempotencyKey, outcome, Instant.now()));
    }

    private Optional<String> extract(Pattern p, String s) {
        Matcher m = p.matcher(s);
        return m.find() ? Optional.of(m.group(1)) : Optional.empty();
    }
}