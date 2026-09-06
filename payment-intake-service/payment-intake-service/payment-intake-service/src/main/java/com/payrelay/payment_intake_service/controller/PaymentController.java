package com.payrelay.payment_intake_service.controller;

import com.payrelay.payment_intake_service.model.Payment;
import com.payrelay.payment_intake_service.repository.PaymentRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;

@RestController
@RequestMapping("/api/payments")
public class PaymentController {

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public PaymentController(PaymentRepository paymentRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.paymentRepository = paymentRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public Payment create(@RequestBody Payment payment) {
        payment.setStatus("PENDING");
        payment.setCreatedAt(Instant.now());
        Payment saved = paymentRepository.save(payment);

        String eventJson = String.format(
                "{\"paymentId\":%d,\"userId\":%d,\"amount\":%s,\"idempotencyKey\":\"%s\"}",
                saved.getId(), saved.getUserId(), saved.getAmount(), saved.getIdempotencyKey()
        );

        kafkaTemplate.send("payment-created", String.valueOf(saved.getId()), eventJson)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        System.err.println("KAFKA SEND FAILED: " + ex.getMessage());
                    } else {
                        System.out.println("KAFKA SEND OK: offset=" + result.getRecordMetadata().offset());
                    }
                });

        return saved;
    }
}