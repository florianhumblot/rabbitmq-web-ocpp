package demo.csms.ocpp;

import demo.csms.config.AmqpConfig;
import demo.csms.config.ConditionalOnRole;
import demo.csms.registry.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
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
 * Stateless OCPP worker. Consumes charger CALLs from the shared {@code csms.requests} queue,
 * publishes the answer back to {@code amq.topic} with the charge point id as routing key (the
 * plugin delivers it down the charger's WebSocket) and records the charger's state in the shared
 * registry. Any number of instances can compete for the queue. The message is acknowledged when
 * this method returns (AUTO ack mode); on shutdown the container stops consuming and finishes the
 * messages it holds.
 */
@Component
@ConditionalOnRole(ConditionalOnRole.WORKER)
public class OcppWorker {

    private static final Logger log = LoggerFactory.getLogger(OcppWorker.class);
    /** Replies to a charger that went away are useless after a while. */
    private static final String REPLY_TTL_MS = "60000";

    private final RabbitTemplate rabbit;
    private final JsonMapper json;
    private final OcppResponder responder;
    private final Registry registry;

    public OcppWorker(RabbitTemplate rabbit, JsonMapper json, OcppResponder responder, Registry registry) {
        this.rabbit = rabbit;
        this.json = json;
        this.responder = responder;
        this.registry = registry;
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
        if (!frame.isArray() || frame.size() < 4) {
            return;
        }
        int type = frame.get(0).asInt();
        if (type != Ocpp.CALL && type != Ocpp.SEND) {
            return;
        }
        String messageId = frame.get(1).asString();
        String action = frame.get(2).asString();
        JsonNode payload = frame.get(3);

        // SEND (OCPP 2.1) expects no answer, nor does the synthetic offline notification.
        if (type == Ocpp.CALL && !Ocpp.isSyntheticOffline(action, payload)) {
            MessageProperties out = new MessageProperties();
            out.setContentType(MessageProperties.CONTENT_TYPE_JSON);
            out.setCorrelationId(messageId);
            out.setExpiration(REPLY_TTL_MS);
            out.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
            byte[] body = json.writeValueAsBytes(responder.reply(key.version(), messageId, action, payload));
            rabbit.send(AmqpConfig.EXCHANGE, chargerId, new Message(body, out));
        }

        // The registry is a projection: if Valkey is unavailable the charger still gets its
        // answer, and the state converges with its next frames.
        try {
            registry.observe(chargerId, key.version(), action, payload, this::connected);
        } catch (RuntimeException e) {
            log.warn("Registry update failed for {}: {}", chargerId, e.getMessage());
        }
    }

    /**
     * Whether a charger is connected right now: the plugin consumes the charger's queue
     * ({@code ocpp.<id>}) for as long as its WebSocket is open, on whichever broker node it is
     * connected to. A passive declare returns the consumer count without changing anything.
     */
    private boolean connected(String chargerId) {
        try {
            Integer consumers = rabbit.execute(channel ->
                    channel.queueDeclarePassive("ocpp." + chargerId).getConsumerCount());
            return consumers != null && consumers > 0;
        } catch (AmqpException e) {
            // The broker answers 404 (and closes the channel) for a queue that does not exist.
            if (String.valueOf(e.getMessage()).contains("NOT_FOUND")
                    || (e.getCause() != null && String.valueOf(e.getCause()).contains("NOT_FOUND"))) {
                return false;
            }
            throw e;
        }
    }
}
