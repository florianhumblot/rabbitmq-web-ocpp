package demo.csms.monitor;

import demo.csms.config.ConditionalOnRole;
import demo.csms.ocpp.Ocpp;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Consumes this dashboard instance's copy of all OCPP traffic and turns it into traffic
 * statistics. Charger state comes from the shared registry instead.
 *
 * <p>Two kinds of frames cross {@code amq.topic}:
 * <ul>
 *   <li>charger → CSMS, routing key {@code ocpp16.Heartbeat.req}, reply_to = charge point id,
 *       correlation_id = OCPP message id</li>
 *   <li>CSMS → charger, routing key = charge point id</li>
 * </ul>
 */
@Component
@ConditionalOnRole(ConditionalOnRole.DASHBOARD)
public class OcppTap {

    private final JsonMapper json;
    private final TrafficStats stats;

    public OcppTap(JsonMapper json, TrafficStats stats) {
        this.json = json;
        this.stats = stats;
    }

    @RabbitListener(queues = "#{tapQueue.name}", containerFactory = "tapContainerFactory")
    public void onFrame(Message message) {
        MessageProperties props = message.getMessageProperties();
        String routingKey = props.getReceivedRoutingKey();
        JsonNode frame;
        try {
            frame = json.readTree(message.getBody());
        } catch (JacksonException e) {
            return;
        }
        if (!frame.isArray() || frame.size() < 3) {
            return;
        }
        long now = System.currentTimeMillis();
        int type = frame.get(0).asInt();
        String messageId = frame.get(1).asString();
        String kind = Ocpp.kindName(type);

        Ocpp.ChargerKey key = Ocpp.parseRoutingKey(routingKey);
        if (key != null) {
            String chargerId = props.getReplyTo();
            if (chargerId == null) {
                return;
            }
            if ("req".equals(key.direction())) {
                JsonNode payload = frame.size() > 3 ? frame.get(3) : null;
                boolean expectsReply = type == Ocpp.CALL && !Ocpp.isSyntheticOffline(key.action(), payload);
                stats.chargerRequest(chargerId, messageId, key.action(), kind, expectsReply, now);
            } else {
                String status = type == Ocpp.CALLRESULT ? frame.get(2).path("status").asString(null) : null;
                stats.chargerResponse(chargerId, messageId, kind, status, now);
            }
        } else if (type == Ocpp.CALL) {
            stats.csmsCall(routingKey, messageId, frame.get(2).asString(), now);
        } else {
            stats.csmsReply(routingKey, messageId, kind, now);
        }
    }
}
