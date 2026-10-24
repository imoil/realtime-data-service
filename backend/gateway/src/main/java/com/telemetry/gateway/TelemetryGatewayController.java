package com.telemetry.gateway;

import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@RestController
@RequestMapping("/api/v1/telemetry")
public class TelemetryGatewayController {

    public static final String ARROW_STREAM_MEDIA_TYPE = "application/vnd.apache.arrow.stream";

    private final TelemetryStorageEngine storageEngine;

    public TelemetryGatewayController(TelemetryStorageEngine storageEngine) {
        this.storageEngine = storageEngine;
    }

    @GetMapping(value = "/am4-arrow", produces = ARROW_STREAM_MEDIA_TYPE)
    public ResponseEntity<StreamingResponseBody> getAm4ArrowStream(
            @RequestParam(name = "sensor_ids", required = false) String sensorIdsParam,
            @RequestParam(name = "sensor_id", defaultValue = "1") int singleSensorId,
            @RequestParam(name = "lb_sec", defaultValue = "1729728000") long lbSec,
            @RequestParam(name = "ub_sec", defaultValue = "1729728060") long ubSec,
            @RequestParam(name = "width", defaultValue = "1920") int width
    ) {
        TelemetryQueryRequest request = TelemetryQueryRequest.of(sensorIdsParam, singleSensorId, lbSec, ubSec, width);
        TelemetryStreamResult result = storageEngine.streamTelemetry(request);

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, ARROW_STREAM_MEDIA_TYPE)
                .header("X-Accel-Buffering", "no")
                .header(HttpHeaders.CACHE_CONTROL, "no-transform")
                .header("X-Target-Table", result.targetTable())
                .header("X-Query-Mode", result.queryMode())
                .body(result.responseBody());
    }
}
