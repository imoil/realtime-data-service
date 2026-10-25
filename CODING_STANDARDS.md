# Coding Standards

This document defines repository-wide engineering standards enforced during review (`/code-review`).

## 1. Domain Modeling & Compatibility Wrappers

### Subclass Builder Method Hiding
When extending domain models or creating backward-compatibility wrappers (e.g. `com.imoil.formula.domain.*` wrapping `com.telemetry.engine.domain.*`):
- Static `builder()` methods in subclasses must not introduce return types incompatible with the parent class static `builder()` method.
- The subclass builder must explicitly extend the parent builder (`public static class Builder extends Parent.Builder`).
- If inheritance complicates builder return types, prefer composition or independent factory methods.

## 2. Module Boundary & Dependency Discipline

### Zero-Dependency Core Engine (`backend:engine`)
- `:backend:engine` is a pure Java 25 library consumed by both Flink streaming workers and the Spring Boot Gateway.
- Do not introduce Spring framework dependencies (`spring-boot-starter`, `spring-context`) or annotation processor plugins (`lombok`) into `:backend:engine`.
- Configuration and beans must be exposed as pure Java factory methods (e.g., `RuleEngineConfig.createEvaluatorInstance()`), allowing callers in Spring (`backend:gateway`) or Flink (`backend:pipeline`) to instantiate them cleanly without classloader conflicts.

### Boundary Validation
- Validate untrusted input at the system entry point (e.g., `ExpressionValidator.validate()`).
- Internal business logic should trust parsed, strongly typed domain models without redundant defensive null checks deep in execution loops.

## 3. High-Throughput State & Allocation Discipline

### Zero-Allocation Sliding State
- High-frequency streaming state paths (1 Hz ticks over 30-minute windows) must avoid Java object allocations (`Double` boxing, `ArrayList` resizing) inside tick processing loops.
- Use packed binary representations (`CompactCircularDoubleBuffer`) and lazy collection views (`AbstractList<Double>`) directly referencing backing buffers.

## 4. Testing Discipline

### Test Behavior, Not Implementation
- Assert observable behavior: mathematical calculation accuracy, edge-case returns, and security exception types.
- Avoid asserting against non-contract private helpers or volatile internal call sequences.

### Allocation Invariants vs Wall-Clock Timing
- Zero-allocation streaming contracts (e.g. steady-state lookups) are deterministic behavioral invariants and may be asserted directly via `ThreadMXBean.getCurrentThreadAllocatedBytes()` after warm-up passes.
- Wall-clock latency benchmarks (<2ns) are sensitive to CPU throttling and virtualization noise. In CI unit test suites, avoid fragile sub-nanosecond hard limits; use generous sanity margins or isolate them into dedicated benchmark executions per `benchmark-checklist`.
