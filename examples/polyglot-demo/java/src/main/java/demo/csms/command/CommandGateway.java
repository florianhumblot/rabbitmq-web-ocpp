package demo.csms.command;

import demo.csms.config.AmqpConfig;
import demo.csms.config.CsmsProperties;
import demo.csms.ocpp.Ocpp;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Sends CSMS-initiated CALLs and waits for the charger's CALLRESULT/CALLERROR.
 *
 * <p>The CALL is published to {@code amq.topic} with the charge point id as routing key; the
 * plugin queues it in the charger's own queue and pushes it down the WebSocket. The answer comes
 * back as {@code <protocol>.response.conf|error} with the OCPP message id as correlation id and
 * is handed over by the tap. Every instance taps all answers, so this works with any number of
 * replicas: only the instance holding the pending call completes it.
 */
@Component
public class CommandGateway {

    /** The charger's answer frame and the round trip time. */
    public record Answer(String messageId, JsonNode frame, long latencyMs) {
    }

    private final RabbitTemplate rabbit;
    private final JsonMapper json;
    private final Duration timeout;
    private final Map<String, CompletableFuture<JsonNode>> pending = new ConcurrentHashMap<>();

    public CommandGateway(RabbitTemplate rabbit, JsonMapper json, CsmsProperties properties) {
        this.rabbit = rabbit;
        this.json = json;
        this.timeout = properties.commandTimeout();
    }

    /** Blocks (on a virtual thread) until the charger answers or the timeout expires. */
    public Answer call(String chargerId, String action, Map<String, Object> payload)
            throws TimeoutException, InterruptedException {
        String messageId = UUID.randomUUID().toString();
        String key = chargerId + '|' + messageId;
        CompletableFuture<JsonNode> answer = new CompletableFuture<>();
        pending.put(key, answer);

        MessageProperties props = new MessageProperties();
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        props.setCorrelationId(messageId);
        // A command nobody picked up in time must not reach the charger later.
        props.setExpiration(Long.toString(timeout.toMillis()));
        props.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
        byte[] body = json.writeValueAsBytes(List.of(Ocpp.CALL, messageId, action, payload));

        long start = System.nanoTime();
        try {
            rabbit.send(AmqpConfig.EXCHANGE, chargerId, new Message(body, props));
            JsonNode frame = answer.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return new Answer(messageId, frame, (System.nanoTime() - start) / 1_000_000);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        } finally {
            pending.remove(key);
        }
    }

    /** Called by the tap for every CALLRESULT/CALLERROR sent by a charger. */
    public void onChargerResponse(String chargerId, String messageId, JsonNode frame) {
        CompletableFuture<JsonNode> answer = pending.remove(chargerId + '|' + messageId);
        if (answer != null) {
            answer.complete(frame);
        }
    }
}
