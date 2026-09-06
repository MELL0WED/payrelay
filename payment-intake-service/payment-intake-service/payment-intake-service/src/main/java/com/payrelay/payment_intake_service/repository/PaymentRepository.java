package com.payrelay.payment_intake_service.repository;

import com.payrelay.payment_intake_service.model.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<Payment, Long> {
}