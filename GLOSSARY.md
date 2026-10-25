# Glossary

### Physical Sensor (물리 센서)
An IoT sensing element deployed in the field (e.g., thermocouple, pressure transducer, accelerometer) that measures real-world physical quantities. It is uniquely addressable within its host device by a local sensor index.

### Formula Sensor / Derived Sensor (수식 센서 / 파생 센서)
A virtual sensor whose value is computed dynamically at runtime using mathematical or statistical expressions over one or more physical or derived sensors (e.g., thermal efficiency, moving average, synthetic power output). Emits 1-second interval time-series data identical in structure to physical sensor data.

### Global Serial Number (전역 일련번호)
A monotonically increasing, system-wide unique integer identifier (`10001+`) that unifies all physical sensors and formula sensors into a single flat namespace, preventing index collisions across multiple multi-sensor IoT devices.

### Sensor Serial Resolver (센서 일련번호 해석기)
The routing component responsible for mapping a composite device key `(device_id, device_sensor_index)` to its canonical Global Serial Number.

### Tumbling Rollup (텀블링 롤업)
Fixed, non-overlapping time windows (1-minute and 1-hour) that compute pre-aggregated scalar statistics (first, last, min, max, avg, count) for time-series compression and accelerated macro-scale dashboard queries.

### Sliding Window (슬라이딩 윈도우)
An overlapping historical time buffer (e.g., 30-minute retention) maintained per sensor to compute moving aggregates such as window average (`window_avg`) and window maximum (`window_max`) on continuous 1-second ticks.

### On-Demand Evaluation (온디멘드 수식 평가)
An ad-hoc computation path where an operator applies a dynamic formula retrospectively over a designated historical time window, pulling raw or pre-aggregated data from time-series storage and calculating results on demand without deploying a persistent streaming job.

### Live Tail Bridge (실시간 라이브 테일 브릿지)
A high-throughput, low-latency bridge connecting streaming message queues to active WebSocket clients for live sub-second monitoring of active sensors.
