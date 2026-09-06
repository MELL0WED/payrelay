package com.payrelay.payment_intake_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PaymentIntakeServiceApplication {

    public static void main(String[] args) {
        System.setProperty("user.timezone", "UTC");
        SpringApplication.run(PaymentIntakeServiceApplication.class, args);
    }

}