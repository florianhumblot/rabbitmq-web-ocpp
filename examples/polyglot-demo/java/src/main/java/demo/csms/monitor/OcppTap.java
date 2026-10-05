package demo.csms.monitor;

import demo.csms.command.CommandGateway;
import demo.csms.ocpp.Ocpp;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Consumes this instance's copy of all OCPP traffic and dispatches it to the registry, the
 * statistics and the command gateway (which waits for charger answers).
 *
 * <p>Two kinds of frames cross {@code amq.topic}:
 * <ul>
 *   <li>charger → CSMS, routing key {@code ocpp16.Heartbeat.req}, reply_to = charge point id,
 *       correlation_id = OCPP message id</li>
 *   <li>CSMS → charger, routing key = charge point id</li>
 * </ul>
 */
@Component
public class OcppTap {

    private final JsonMapper json;
    private final ChargerRegistry registry;
    private final TrafficStats stats;
    private final CommandGateway commands;

    public OcppTap(JsonMapper json, ChargerRegistry registry, TrafficStats stats, CommandGateway commands) {
        this.json = json;
        this.registry = registry;
        this.stats = stats;
        this.commands = commands;
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
                registry.onChargerRequest(chargerId, key.version(), key.action(), payload, now);
                boolean expectsReply = type == Ocpp.CALL && !Ocpp.isSyntheticOffline(key.action(), payload);
                stats.chargerRequest(chargerId, messageId, key.action(), kind, expectsReply, now);
            } else {
                registry.onChargerResponse(chargerId, key.version(), now);
                String status = type == Ocpp.CALLRESULT ? frame.get(2).path("status").asString(null) : null;
                stats.chargerResponse(chargerId, messageId, kind, status, now);
                commands.onChargerResponse(chargerId, messageId, frame);
            }
        } else if (type == Ocpp.CALL) {
            stats.csmsCall(routingKey, messageId, frame.get(2).asString(), now);
        } else {
            stats.csmsReply(routingKey, messageId, kind, now);
        }
    }
}
