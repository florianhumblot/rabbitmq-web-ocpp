package demo.csms.ocpp;

import demo.csms.config.AmqpConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Stateless OCPP worker. Consumes charger CALLs from the shared {@code csms.requests} queue and
 * publishes the answer back to {@code amq.topic} with the charge point id as routing key, which
 * the plugin delivers down the charger's WebSocket. Any number of instances can compete for the
 * queue. The message is acknowledged when this method returns (AUTO ack mode).
 */
@Component
public class OcppWorker {

    private static final Logger log = LoggerFactory.getLogger(OcppWorker.class);
    /** Replies to a charger that went away are useless after a while. */
    private static final String REPLY_TTL_MS = "60000";

    private final RabbitTemplate rabbit;
    private final JsonMapper json;
    private final OcppResponder responder;

    public OcppWorker(RabbitTemplate rabbit, JsonMapper json, OcppResponder responder) {
        this.rabbit = rabbit;
        this.json = json;
        this.responder = responder;
    }

    @RabbitListener(queues = AmqpConfig.REQUESTS_QUEUE)
    public void onChargerRequest(Message message) {
        MessageProperties props = message.getMessageProperties();
        String chargerId = props.getReplyTo();
        Ocpp.ChargerKey key = Ocpp.parseRoutingKey(props.getReceivedRoutingKey());
        if (chargerId == null || key == null) {
            return;
        }
        JsonNode frame;
        try {
            frame = json.readTree(message.getBody());
        } catch (JacksonException e) {
            log.warn("Dropping malformed frame from {}: {}", chargerId, e.getOriginalMessage());
            return;
        }
        // SEND (OCPP 2.1) expects no answer; anything else here is not a CALL.
        if (!frame.isArray() || frame.size() < 4 || frame.get(0).asInt() != Ocpp.CALL) {
            return;
        }
        String messageId = frame.get(1).asString();
        String action = frame.get(2).asString();
        JsonNode payload = frame.get(3);
        if (Ocpp.isSyntheticOffline(action, payload)) {
            return;
        }

        MessageProperties out = new MessageProperties();
        out.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        out.setCorrelationId(messageId);
        out.setExpiration(REPLY_TTL_MS);
        out.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
        byte[] body = json.writeValueAsBytes(responder.reply(key.version(), messageId, action, payload));
        rabbit.send(AmqpConfig.EXCHANGE, chargerId, new Message(body, out));
    }
}
