# Agent work log

0. install skills
  - matt-pocock skills
  - diagram-design
  - pstack
1. initial skills
  - /setup-matt-pocock-skills
  - /setup-pstack
2. /grill-me-with-docs @file:ADR.md
3. /ask-matt
```mermaid
flowchart LR
    Grill["1. /grill-with-docs\n(완료: ADR-001 확정)"]
    Decision{"2. 세부 프로토타입\n검증 필요 여부"}
    Proto["/prototype\n(Flechette/LC JS 등 투기적 검증)"]
    Spec["3. /to-spec\n(대화 맥락을 스펙 문서로 합성)"]
    Tickets["4. /to-tickets\n(Tracer-Bullet 태스크 그래프 분할)"]
    Impl["5. /implement 또는 /implement-spec\n(TDD 기반 단계별 구현)"]

    Grill --> Decision
    Decision -->|"화면/스트림 검증 필요"| Proto
    Decision -->|"바로 전체 구축 진행"| Spec
    Proto --> Spec
    Spec --> Tickets
    Tickets --> Impl
```
4. /to-spec @file:ADR.md
```mermaid
flowchart LR
    subgraph Seam1["Seam 1: Flink MiniCluster Job Seam"]
        S1_In["Kafka Mock Source\n(Protobuf Records)"] --> FlinkJob["Flink Pipeline"]
        FlinkJob --> S1_Out1["Mock S3 Iceberg Sink"]
        FlinkJob --> S1_Out2["ClickHouse Rollup Sink"]
    end

    subgraph Seam2["Seam 2: Spring Boot API Gateway Seam (최상위 백엔드 심)"]
        S2_In["HTTP GET /api/v1/telemetry/am4-arrow\nWS /ws/v1/telemetry/live"] --> SpringBoot["Spring Boot Router"]
        SpringBoot --> S2_Out["Binary Arrow IPC Stream\n& WebSocket Ticks"]
    end

    subgraph Seam3["Seam 3: Frontend Composable Seam (최상위 프론트엔드 심)"]
        S3_In["Raw ArrayBuffer Stream\n& WebSocket Payload"] --> Composable["useTelemetryStream"]
        Composable --> S3_Out["TypedArray Memory View\n& WebGL Series Buffers"]
    end
```
5. /to-tickets #1
```mermaid
flowchart TD
    T1["#2 Ticket 1: Single-Sensor End-to-End Walking Skeleton\n(Frontier: 즉시 시작 가능)"]:::frontier
    T2["#3 Ticket 2: Multi-Sensor 30-Day Macro AM4 Visualization"]:::blocked
    T3["#5 Ticket 3: Cold Lakehouse Tiering & Micro-Zoom Parquet Scans"]:::blocked
    T4["#4 Ticket 4: Sub-Second Live Telemetry via WebSocket Tail"]:::blocked
    T5["#6 Ticket 5: Stream Resilience & Semantic Zoom Navigation"]:::blocked

    T1 -->|Blocks| T2
    T2 -->|Blocks| T3
    T1 -->|Blocks| T4
    T2 -->|Blocks| T5
    T3 -->|Blocks| T5
    T4 -->|Blocks| T5

    classDef frontier fill:#238636,stroke:#2ea043,color:#fff,font-weight:bold;
    classDef blocked fill:#21262d,stroke:#30363d,color:#8b949e;
```
6. /ask-matt 티켓을 모두 생성했다면, 컨텍스트를 clear 해도 되나?
7. /clear
8. /implement #2
9. /retro
10. gh issue close 2
11. /clear
12. /implement #3
13. gh issue close 3 --comment "Completed multi-sensor 30-day macro AM4 visualization (commit 2dc7ed1)"
14. /clear
15. /implement #4 진행 및 결과는 한글로 출력해
16. /retro
17. gh issue close 4 --comment "Completed sub-second live telemetry via WebSocket tail with full end-to-end verification (commit 4ddc668)"
18. /clear
19. /implement #5
20. /retro
21. /clear
22. /implement #6
23. /retro
24. /improve-codebase-architecture 회고 결과를 바탕으로 개선점을 도출해
25. /to-spec 위 내용을 스펙으로 전환해 (#7)
26. /to-tickets #7
```mermaid
flowchart TD
    T8["#8 Ticket 2.1: Unified Backend TelemetryStorageEngine Deep Module\n(Frontier: 즉시 시작 가능)"]:::frontier
    T9["#9 Ticket 2.2: Unified Flink Stream Processing Topology\n(Frontier: 즉시 시작 가능)"]:::frontier
    T10["#10 Ticket 2.3: Headless useTelemetryDashboard Deep Composable\n(Frontier: 즉시 시작 가능)"]:::frontier
    T11["#11 Ticket 2.4: Fixed-Budget Circular Ring Buffer Pooling\n(Blocked by #10)"]:::blocked

    T10 -->|Blocks| T11

    classDef frontier fill:#238636,stroke:#2ea043,color:#fff,font-weight:bold;
    classDef blocked fill:#21262d,stroke:#30363d,color:#8b949e;
```
27. /clear
28. /implement #8 진행 내용은 한글로 표시해
29. /retro
