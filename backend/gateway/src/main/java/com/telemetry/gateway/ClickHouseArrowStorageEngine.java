package com.telemetry.gateway;

import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.util.List;

/**
 * ClickHouse & Arrow IPC implementation of TelemetryStorageEngine.
 *
 * Coordinates query routing, live ClickHouse binary pass-through streaming,
 * and fallback mock generation for isolated testing.
 */
@Service
public class ClickHouseArrowStorageEngine implements TelemetryStorageEngine {

    private final TelemetryQueryRouter queryRouter;
    private final ClickHouseTelemetryService telemetryService;
    private final ClickHouseClientService clickHouseClientService;
    private final ArrowStreamWriterService arrowWriterService;

    public ClickHouseArrowStorageEngine(
            TelemetryQueryRouter queryRouter,
            ClickHouseTelemetryService telemetryService,
            ClickHouseClientService clickHouseClientService,
            ArrowStreamWriterService arrowWriterService
    ) {
        this.queryRouter = queryRouter;
        this.telemetryService = telemetryService;
        this.clickHouseClientService = clickHouseClientService;
        this.arrowWriterService = arrowWriterService;
    }

    @Override
    public TelemetryStreamResult streamTelemetry(TelemetryQueryRequest request) {
        QueryRoutingDecision routing = queryRouter.resolve(
                request.timeRange(),
                request.sensorIds(),
                request.width()
        );

        String queryMode = routing.isRaw() ? "raw" : "am4";

        StreamingResponseBody responseBody = outputStream -> {
            boolean streamed = false;
            if (clickHouseClientService.isEnabled()) {
                streamed = clickHouseClientService.streamDirectArrow(routing.sqlQuery(), outputStream);
            }

            if (!streamed) {
                // Fallback / mock mode (unit tests & local mode without ClickHouse daemon)
                List<Am4DataPoint> points = telemetryService.getAm4Telemetry(
                        routing.sensorIds(),
                        routing.timeRange(),
                        routing.width()
                );
                arrowWriterService.writeDataPoints(points, outputStream);
                outputStream.flush();
            }
        };

        int estimatedRows = routing.isRaw()
                ? (int) (routing.timeRange().durationSec() + 1) * routing.sensorIds().size()
                : routing.width() * 4 * routing.sensorIds().size();

        return new TelemetryStreamResult(responseBody, routing.targetTable(), queryMode, estimatedRows);
    }
}
