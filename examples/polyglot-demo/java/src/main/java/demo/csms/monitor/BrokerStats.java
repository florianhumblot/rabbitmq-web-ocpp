package demo.csms.monitor;

import demo.csms.config.AmqpConfig;
import demo.csms.config.ConditionalOnRole;
import demo.csms.config.CsmsProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

/** Polls the RabbitMQ management API for broker-wide figures. */
@Component
@ConditionalOnRole(ConditionalOnRole.DASHBOARD)
public class BrokerStats {

    private static final Logger log = LoggerFactory.getLogger(BrokerStats.class);

    public record RequestQueue(long messages, long consumers, double ackRate) {
    }

    public record View(boolean available, long connections, long queues, double publishRate,
                       double deliverRate, RequestQueue requestQueue, long memoryBytes,
                       long memoryLimitBytes, long fdUsed, long fdTotal, long erlangProcesses) {
        static View unavailable() {
            return new View(false, 0, 0, 0, 0, new RequestQueue(0, 0, 0), 0, 0, 0, 0, 0);
        }
    }

    private final RestClient client;
    private final String vhost;
    private volatile View current = View.unavailable();

    public BrokerStats(RestClient.Builder builder, CsmsProperties properties) {
        this.client = builder
                .baseUrl(properties.managementUrl())
                .defaultHeaders(h -> h.setBasicAuth(properties.managementUser(), properties.managementPassword()))
                .defaultHeader(HttpHeaders.ACCEPT, "application/json")
                .build();
        this.vhost = properties.vhost();
    }

    public View current() {
        return current;
    }

    @Scheduled(fixedDelay = 2000)
    void poll() {
        try {
            JsonNode overview = client.get().uri("/api/overview").retrieve().body(JsonNode.class);
            JsonNode queue = client.get().uri("/api/queues/{vhost}/{queue}", vhost, AmqpConfig.REQUESTS_QUEUE)
                    .retrieve().body(JsonNode.class);
            JsonNode nodes = client.get().uri("/api/nodes").retrieve().body(JsonNode.class);

            long mem = 0, memLimit = 0, fdUsed = 0, fdTotal = 0, procs = 0;
            for (JsonNode node : nodes) {
                mem += node.path("mem_used").asLong(0);
                memLimit += node.path("mem_limit").asLong(0);
                fdUsed += node.path("fd_used").asLong(0);
                fdTotal += node.path("fd_total").asLong(0);
                procs += node.path("proc_used").asLong(0);
            }
            JsonNode stats = overview.path("message_stats");
            current = new View(true,
                    overview.path("object_totals").path("connections").asLong(0),
                    overview.path("object_totals").path("queues").asLong(0),
                    stats.path("publish_details").path("rate").asDouble(0),
                    stats.path("deliver_get_details").path("rate").asDouble(0),
                    new RequestQueue(queue.path("messages").asLong(0), queue.path("consumers").asLong(0),
                            queue.path("message_stats").path("ack_details").path("rate").asDouble(0)),
                    mem, memLimit, fdUsed, fdTotal, procs);
        } catch (RuntimeException e) {
            log.debug("Management API poll failed: {}", e.getMessage());
            current = View.unavailable();
        }
    }
}
