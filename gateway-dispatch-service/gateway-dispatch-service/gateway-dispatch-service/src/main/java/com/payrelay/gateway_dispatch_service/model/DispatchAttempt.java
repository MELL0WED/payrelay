package com.payrelay.gateway_dispatch_service.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "dispatch_attempts")
public class DispatchAttempt {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long paymentId;
    private String idempotencyKey;
    private String outcome;
    private Instant attemptedAt;

    public DispatchAttempt() {
    }

    public DispatchAttempt(Long id, Long paymentId, String idempotencyKey, String outcome, Instant attemptedAt) {
        this.id = id;
        this.paymentId = paymentId;
        this.idempotencyKey = idempotencyKey;
        this.outcome = outcome;
        this.attemptedAt = attemptedAt;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getPaymentId() {
        return paymentId;
    }

    public void setPaymentId(Long paymentId) {
        this.paymentId = paymentId;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public String getOutcome() {
        return outcome;
    }

    public void setOutcome(String outcome) {
        this.outcome = outcome;
    }

    public Instant getAttemptedAt() {
        return attemptedAt;
    }

    public void setAttemptedAt(Instant attemptedAt) {
        this.attemptedAt = attemptedAt;
    }
}