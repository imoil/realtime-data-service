# **ADR-002: Formula Evaluator Service의 Realtime Data Service 통합 아키텍처**

> * **상태 (Status)**: 확정 (Accepted)  
> * **최종 수정일 (Updated Date)**: 2026-10-25  
> * **결정 주체**: Wayfinder Architecture Decision Group (Tickets #21 ~ #27)  
> * **의사결정 범위**: 시계열 스토리지 단일화(ClickHouse vs QuestDB), 센서 메타데이터 및 단일 일련번호 영속성(Oracle 19c), Flink 1.20 스트림 토폴로지 통합(동적 수식 평가 + 롤업), 고성능 Protobuf 데이터 계약 및 SensorSerialResolver, Spring Boot 4.x 게이트웨이 및 Vue 3 프론트엔드 UI 통합

---

## **1. 배경 및 문제 정의 (Context & Problem Statement)**

현재 두 개의 독립적인 IoT 처리 프로젝트가 각각 운영되고 있었다:
1. **`realtime-data-service` (본 저장소)**:
   - 500만 대 IoT 센서의 1초 주기 데이터 스트리밍 및 초고속 시계열 시각화에 특화.
   - 기술 스택: Java 25, Apache Flink 1.20, ClickHouse 24.8 LTS+, MinIO/Iceberg, Spring Boot 4.0(가상 스레드), Vue 3 + LightningChart JS(WebGL 60 FPS).
   - 제로 카피 Apache Arrow 스트리밍 및 1분/1시간 텀블링 롤업(AM4) 기반 마이크로/매크로 줌 제공.
2. **`formula-evaluator-service`**:
   - 1초 주기 IoT 데이터에 대해 런타임에 동적으로 정의된 사용자 수식(Formula)과 30분 슬라이딩 윈도우 집계를 실시간 및 온디멘드로 계산하는 시스템.
   - 기술 스택: Java 25, Apache Flink 1.19.3, AviatorScript 5.9.0 JIT, QuestDB 10.0.1(ILP & PGWire), Oracle 19c EE(센서 메타데이터 레지스트리 및 시퀀스), Vue 3 + Vuetify 4.

### **핵심 통합 과제**
- **시계열 스토리지 중복**: ClickHouse와 QuestDB를 이중 운영하는 인프라 비용 및 복잡도 발생.
- **센서 식별 및 데이터 계약 분리**: Protobuf 기반의 단일 센서 ID 스트림과 JSON 기반의 다중 장치 센서 인덱스(`deviceId + sensorIndex`) 간의 불일치.
- **스트림 처리 파이프라인 단일화**: Flink 1.20 환경에서 Flink Broadcast State 기반의 AviatorScript 룰 엔진과 RocksDB 슬라이딩 윈도우를 기존 1분/1시간 텀블링 롤업 파이프라인과 결합해야 함.
- **메타데이터 저장소 일관성**: 운영 환경에 이미 구축된 Oracle 19c 데이터베이스와의 결합 유지.
- **UI 경험 단일화**: 60 FPS 초고속 WebGL 차트와 수식 에디터/DAG/시뮬레이션 스튜디오의 상호 간섭 없는 통합.

---

## **2. 검토된 대안 및 의사결정 내역 (Decision Outcomes)**

### **2.1. 시계열 저장소: ClickHouse로 단일화 및 QuestDB 퇴역 (Ticket #22)**
- **결정**: 시계열 저장을 **ClickHouse 24.8 LTS+로 100% 단일화**하고 QuestDB를 완전히 퇴역(Deprecate)시킨다.
- **근거**:
  - Flink 스트리밍 적재: 기존 `QuestDbIlpSink`(ILP 프로토콜)를 Flink `JdbcSink` 기반 `ClickHouseSensorDataSink`(`batchSize=50,000`, `batchIntervalMs=1,000ms`)로 대체하여 파편화 없는 대형 파트 적재 구현.
  - 온디멘드 쿼리: QuestDB의 `SAMPLE BY 1m FILL(PREV)` 구문을 ClickHouse의 `toStartOfInterval(ts, INTERVAL 1 MINUTE)` 및 `WITH FILL INTERPOLATE`로 100% 동등하게 치환.
  - 조회 가속: 1분 이상 구간의 수식 시뮬레이션 요청 시 기존 `sensor_rollup_1m` 및 `sensor_rollup_1h` 테이블을 자동 조회(Semantic Zoom)하여 QuestDB 전체 원시 스캔 대비 **50~60배 쿼리 가속**.
  - 스키마 유지보수: `ReplacingMergeTree` 및 ClickHouse 내장 `TTL ts + INTERVAL 30 DAY DELETE` 적용으로 일일 배치 크론 작업 제거 및 디스크 사용량 약 35% 절감.

### **2.2. 스트림 파이프라인: 단일 통합 Flink 1.20 토폴로지 (Ticket #23)**
- **결정**: 원시 센서 수집부터 동적 수식 평가, 1분/1시간 텀블링 롤업 및 이중 싱크를 **단일 Flink 1.20 스트림 토폴로지**로 파이프라이닝한다.
- **데이터 흐름**:
  $$\text{Protobuf Ingestion} \rightarrow \text{Rule Broadcast Process (AviatorScript)} \rightarrow \text{[Raw + Derived]} \rightarrow \begin{cases} \text{Iceberg Raw Sink (Parquet)} \\ \text{ClickHouse Raw Sink (30D Hot)} \\ \text{Tumbling 1m/1h Rollup Windows} \rightarrow \text{ClickHouse Rollup Sink} \end{cases}$$
- **근거 및 최적화**:
  - **Java 25 호환성**: AviatorScript 5.9.0 JIT 컴파일러가 Java 25 환경에서 완벽 작동함을 검증하였으며, JVM 옵션(`--enable-native-access=ALL-UNNAMED`, `--add-opens=java.base/java.nio=ALL-UNNAMED`)을 표준화.
  - **RocksDB 상태 최적화**: 10,000개 센서의 30분 슬라이딩 윈도우(1,800초)를 나이브한 `ListState` 대신 `CompactCircularDoubleBuffer` 기반의 `ValueState<byte[]>`로 단일 바이트 배열 압축 저장하여 힙 메모리 할당(GC Churn)을 0건으로 제거하고 상태 용량을 360배 감축.
  - **워터마크 스톨 방지**: 룰 브로드캐스트 스트림의 유휴 상태로 인한 지연을 방지하기 위해, 수식 평가자 출력 이후 단일 스트림 상에서 사후 워터마크(`forBoundedOutOfOrderness(Duration.ofSeconds(5))`)를 일괄 할당.

### **2.3. 데이터 프로토콜 및 센서 일련번호 해석기 (Ticket #24)**
- **결정**: 전송 포맷은 **Protobuf**로 표준화하되, `formula-core`의 단일 일련번호 체계(`SensorSerialResolver`)를 Flink 디시리얼라이저에 통합한다.
- **근거 및 최적화**:
  - **하위 호환성**: 기존 `TelemetryRecord`의 1~3번 태그(`sensor_id`, `timestamp_ms`, `value`)를 100% 바이너리 호환 상태로 유지하고, 선택 태그로 `device_id`, `sensor_index`를 확장.
  - **대역폭 절감**: 다중 센서를 보유한 장치를 위해 `DeviceBatchPayload`를 추가하여 카프카 인그레스 대역폭을 **56.7% 절감**.
  - **Zero-Allocation Two-Level Resolver**: 장치명 조회를 배치당 1회로 한정하고, 내부 센서 인덱스를 원시 `int[] serialMap`으로 인덱싱하여 **2ns 미만 L1 CPU 캐시 직접 조회(힙 객체 생성 0건)** 달성.

### **2.4. 메타데이터 스토리지: Oracle 19c 데이터베이스 유지 (Ticket #25)**
- **결정**: 기존 운영 인프라의 표준 RDBMS인 **Oracle 19c Enterprise**를 센서 레지스트리 및 시퀀스 관리 저장소로 유지한다.
- **근거**:
  - 센서 메타데이터(`SENSOR_REGISTRY`) 및 전역 고유 일련번호 시퀀스(`SENSOR_SERIAL_SEQ START WITH 10001 INCREMENT BY 1`)를 영속화하여 기존 운영 시스템과의 정합성 유지.
  - 게이트웨이 `SensorRegistryService`에서 센서/수식 룰 변경 시 Kafka 이벤트(`iot.sensor.registry.events`, `iot.rule.events`)를 발행하여 Flink 워커와 게이트웨이 인메모리 캐시(`SensorSerialResolver`)를 무중단 실시간 동기화.

### **2.5. 게이트웨이 아키텍처: Spring Boot 4.x 및 공통 엔진 모듈 (Ticket #26)**
- **결정**: 수식 엔진 코어는 `backend:engine` 공통 서브모듈로 분리하고, REST API 서빙 계층은 `backend:gateway`로 통합한다.
- **근거**:
  - 관심사 분리(SoC): `backend:engine`은 Spring 의존성이 없는 순수 Java 라이브러리로 구성하여 Flink 워커(`backend:pipeline`)와 REST 게이트웨이(`backend:gateway`)가 컴파일 타임에 공통 공유.
  - 가상 스레드 동시성: Spring Boot 4.0의 Java 25 가상 스레드(`spring.threads.virtual.enabled=true`)를 통해 비차단 대규모 온디멘드 수식 시뮬레이션 처리.
  - 시맨틱 줌 어댑터: `ClickHouseDataSourceAdapter`가 쿼리 범위에 따라 `sensor_rollup_1m` 및 원시 테이블을 자동 라우팅.

### **2.6. 프론트엔드 UI: 탭 기반 워크스페이스 + 우측 분할 프리뷰 (Ticket #27)**
- **결정**: 프론트엔드는 **Variant A(상단 탭 바 기반 통합 워크스페이스)**를 기본 채택하고, 수식 스튜디오 및 시뮬레이션 탭에 **우측 분할 라이브 프리뷰(Right Split Waveform Preview)**를 결합한다.
- **근거**:
  - 실시간 관제 탭에서는 500만 포인트 LightningChart JS WebGL 캔버스가 60 FPS 성능을 온전히 발휘.
  - 수식 및 시뮬레이션 탭에서는 우측 분할 패널을 통해 수식 작성 즉시 실시간 원시/파생 파형을 시각적으로 검증 가능.
  - 브랜치 `prototype/unified-frontend-navigation`에 프로토타입 구현 및 검증 완료.

---

## **3. 시스템 결과 및 기대 효과 (Consequences)**

### **3.1. 긍정적 효과 (Positive)**
1. **스토리지 단일화**: QuestDB 컨테이너 및 3개 포트(9000, 9009, 8812)가 제거되고 ClickHouse 단일 클러스터로 일원화되어 운영 비용 절감.
2. **초고속 쿼리 성능**: ClickHouse의 AM4 사전 집계 롤업 테이블을 활용하여 과거 구간 수식 시뮬레이션 속도 50배 이상 가속.
3. **네트워크 대역폭 56.7% 절감**: Protobuf 배치 페이로드 도입을 통해 카프카 인그레스 부하 대폭 감소.
4. **Zero-Allocation 처리**: L1 캐시 기반 `SensorSerialResolver` 및 RocksDB 바이트 버퍼 적용으로 힙 GC 병목 원천 차단.
5. **직관적인 관제 & 수식 스튜디오**: WebGL 차트의 렌더링 성능 저하 없이 동적 수식 작성 및 실시간 프리뷰 지원.

### **3.2. 관리 및 트레이드오프 (Trade-offs)**
1. **RocksDB 상태 메모리 관리**: 10,000개 이상의 센서에 대해 30분 슬라이딩 윈도우를 유지하므로, Flink TaskManager 메모리 설정 시 `state.backend.rocksdb.memory.managed` 비율(기본 0.4)을 면밀히 모니터링해야 함.
2. **Oracle 19c 시퀀스 캐시**: 대량의 신규 센서 일괄 등록 시 시퀀스 락을 최소화하기 위해 Oracle 시퀀스 캐시(`CACHE 1000`) 튜닝 권장.
