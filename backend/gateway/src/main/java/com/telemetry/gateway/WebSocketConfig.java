package com.telemetry.gateway;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final LiveTelemetryWebSocketHandler liveHandler;

    public WebSocketConfig(LiveTelemetryWebSocketHandler liveHandler) {
        this.liveHandler = liveHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(liveHandler, "/ws/v1/telemetry/live")
                .setAllowedOrigins("*");
    }
}
