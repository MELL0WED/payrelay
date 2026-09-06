package com.payrelay.gateway_dispatch_service.gateway;

import org.springframework.stereotype.Component;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class GatewaySimulator {

    // Simulates calling a real payment gateway. Deliberately unreliable:
    // ~50% succeed, ~30% fail cleanly, ~20% "time out" (unknown outcome).
    public String call(String idempotencyKey) {
        int roll = ThreadLocalRandom.current().nextInt(100);
        if (roll < 50) return "SUCCESS";
        if (roll < 80) return "FAILED";
        return "TIMEOUT";
    }
}