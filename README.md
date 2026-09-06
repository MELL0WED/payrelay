# PayRelay — Payment Gateway Retry & Reconciliation Orchestrator

A two-service microservices system that safely handles payments through an unreliable external payment gateway — using asynchronous processing, idempotent event consumption, and automatic retry/dead-letter handling.

## The Problem

When a system calls an external payment gateway, that call can succeed, fail cleanly, or time out with no clear answer at all. The dangerous case is the timeout — you genuinely don't know if the charge went through. On top of that, message brokers like Kafka guarantee *at-least-once* delivery, meaning the same event can be delivered more than once. Naively reprocessing a duplicate event risks double-charging a customer.

PayRelay solves both problems: it retries failed/ambiguous gateway calls safely, caps retries to avoid infinite loops, and guarantees a payment is never processed twice, even if the same event arrives multiple times.

## Architecture

Two independently deployable Spring Boot services, communicating asynchronously via Apache Kafka:

**Payment-Intake Service** (`:8091`)
- Accepts a payment request via REST API
- Persists it to PostgreSQL with status `PENDING`
- Publishes an event to Kafka
- Responds to the caller immediately — the response never waits on downstream gateway processing

**Gateway-Dispatch Service**
- Consumes payment events from Kafka
- Calls a simulated external payment gateway (randomized SUCCESS / FAILED / TIMEOUT outcomes, standing in for a real provider)
- Uses Redis to guarantee idempotent processing: once a payment succeeds, any redelivered event for the same payment is skipped
- Tracks retry attempts in Redis, capping at a configurable threshold before marking a payment `DEAD_LETTER`
- Records every dispatch attempt (not just the final outcome) in its own PostgreSQL table

## Results

- **Reduced redundant payment-gateway calls under duplicate event delivery by 80%** (5 duplicate deliveries → 1 real gateway call, remaining 4 correctly skipped) by implementing a Redis-backed idempotency guard keyed on payment idempotency key, preventing duplicate processing under Kafka's at-least-once delivery semantics.
- **Achieved 28.77ms average response latency** on the payment-intake endpoint by decoupling user-facing request handling from downstream gateway processing via asynchronous Kafka publish, ensuring API responsiveness is independent of external gateway performance.
- **Sustained ~35 requests/sec throughput** on the payment-intake service under sequential load testing, verified via direct timed request batches against the live endpoint.
- **Implemented retry-and-dead-letter handling** for unreliable downstream payment gateway calls, capping failed/timeout attempts at a configurable threshold before marking a payment permanently failed, preventing infinite retry loops on a persistently failing dependency.
- **Built and deployed a two-service microservices architecture** using Java, Spring Boot, and Apache Kafka (Aiven managed cloud), demonstrating asynchronous inter-service communication, idempotent event consumption, and independent service deployability.

## Tech Stack

- **Java 17 + Spring Boot** — both services
- **Apache Kafka** (Aiven managed cloud) — event backbone connecting the two services, secured via SASL_SSL authentication
- **PostgreSQL** — persistent storage for payment records and dispatch-attempt history
- **Redis** — idempotency guard and retry-count tracking with automatic expiry
- **Docker Compose** — local infrastructure (Postgres, Redis)

## Design Decisions & Tradeoffs

- **StringSerializer/StringDeserializer with manual JSON construction**, rather than Spring Kafka's built-in JSON serializer, due to a compatibility conflict between Spring Kafka's serializer and Spring Boot 4's default Jackson version. A deliberate simplification: less type-safety on the producer side, in exchange for eliminating a serialization-library dependency risk.
- **Managed cloud Kafka (Aiven) rather than local Docker Kafka**, after extensive troubleshooting of a local Docker networking issue specific to the development machine. Documented transparently rather than hidden — the underlying application code is portable and would work identically against any standard Kafka broker.
- **Credentials externalized via environment variables**, never hardcoded, following secret-management best practice (also enforced by GitHub's push protection, which caught an early accidental credential commit).

## Bug Found & Fixed During Testing

While measuring idempotency-guard effectiveness, direct Redis inspection revealed the "already succeeded" status marker was never being persisted — a line writing the SUCCESS flag to Redis had been left commented out from an earlier debugging pass. Confirmed via `GET dispatch:status:<key>` returning `nil` despite successful processing. Fixed, then re-verified with a controlled before/after test, producing the measured 80% reduction figure above.