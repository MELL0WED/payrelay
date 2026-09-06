package com.payrelay.gateway_dispatch_service.repository;

import com.payrelay.gateway_dispatch_service.model.DispatchAttempt;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DispatchAttemptRepository extends JpaRepository<DispatchAttempt, Long> {
}