package com.telemetry.gateway;

import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.util.List;

/**
 * ClickHouse & Arrow IPC implementation of TelemetryStorageEngine.
 *
 * Coordinates query routing, AM4 / raw data retrieval, and binary Arrow IPC streaming.
 */
@Service
public class ClickHouseArrowStorageEngine implements TelemetryStorageEngine {

    private final TelemetryQueryRouter queryRouter;
    private final ClickHouseTelemetryService telemetryService;
    private final ArrowStreamWriterService arrowWriterService;

    public ClickHouseArrowStorageEngine(
            TelemetryQueryRouter queryRouter,
            ClickHouseTelemetryService telemetryService,
            ArrowStreamWriterService arrowWriterService
    ) {
        this.queryRouter = queryRouter;
        this.telemetryService = telemetryService;
        this.arrowWriterService = arrowWriterService;
    }

    @Override
    public TelemetryStreamResult streamTelemetry(TelemetryQueryRequest request) {
        QueryRoutingDecision routing = queryRouter.resolve(
                request.timeRange(),
                request.sensorIds(),
                request.width()
        );

        List<Am4DataPoint> points = telemetryService.getAm4Telemetry(
                routing.sensorIds(),
                routing.timeRange(),
                routing.width()
        );

        StreamingResponseBody responseBody = outputStream -> {
            arrowWriterService.writeDataPoints(points, outputStream);
            outputStream.flush();
        };

        String queryMode = routing.isRaw() ? "raw" : "am4";
        return new TelemetryStreamResult(responseBody, routing.targetTable(), queryMode, points.size());
    }
}
