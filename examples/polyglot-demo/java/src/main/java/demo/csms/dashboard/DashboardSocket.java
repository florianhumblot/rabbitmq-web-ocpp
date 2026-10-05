package demo.csms.dashboard;

import demo.csms.config.CsmsProperties;
import demo.csms.monitor.BrokerStats;
import demo.csms.monitor.ChargerRegistry;
import demo.csms.monitor.TrafficStats;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.databind.json.JsonMapper;

/** Pushes one JSON snapshot per second to every dashboard on {@code /ws}. */
@Configuration
@EnableWebSocket
public class DashboardSocket extends TextWebSocketHandler implements WebSocketConfigurer {

    public record Chargers(long known, long online, Map<String, Long> byVersion,
                           Map<String, Long> bySecurityProfile) {
    }

    public record Snapshot(String implementation, String instance, long uptimeSeconds, long timestamp,
                           Chargers chargers, Map<String, Long> connectors, TrafficStats.Traffic traffic,
                           TrafficStats.Latency latency, TrafficStats.Commands commands,
                           BrokerStats.View broker, List<TrafficStats.RecentFrame> recent) {
    }

    private final Set<WebSocketSession> sessions = ConcurrentHashMap.newKeySet();
    private final JsonMapper json;
    private final ChargerRegistry registry;
    private final TrafficStats stats;
    private final BrokerStats broker;
    private final String implementation;
    private final String instance;

    public DashboardSocket(JsonMapper json, ChargerRegistry registry, TrafficStats stats, BrokerStats broker,
                           CsmsProperties properties) throws IOException {
        this.json = json;
        this.registry = registry;
        this.stats = stats;
        this.broker = broker;
        this.implementation = properties.implementation();
        this.instance = InetAddress.getLocalHost().getHostName();
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry handlers) {
        handlers.addHandler(this, "/ws").setAllowedOriginPatterns("*");
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        sessions.add(new ConcurrentWebSocketSessionDecorator(session, 1000, 1 << 20));
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.removeIf(s -> s.getId().equals(session.getId()));
    }

    @Scheduled(fixedRate = 1000)
    void broadcast() {
        // Close the statistics window even without viewers, so rates stay per second.
        TrafficStats.Window window = stats.window();
        if (sessions.isEmpty()) {
            return;
        }
        ChargerRegistry.Summary summary = registry.summary();
        Snapshot snapshot = new Snapshot(implementation, instance,
                ManagementFactory.getRuntimeMXBean().getUptime() / 1000, System.currentTimeMillis(),
                new Chargers(summary.known(), summary.online(), summary.byVersion(), summary.bySecurityProfile()),
                summary.connectors(), window.traffic(), window.latency(), window.commands(),
                broker.current(), window.recent());
        TextMessage message = new TextMessage(json.writeValueAsString(snapshot));
        for (WebSocketSession session : sessions) {
            try {
                session.sendMessage(message);
            } catch (IOException | IllegalStateException e) {
                sessions.remove(session);
            }
        }
    }
}
