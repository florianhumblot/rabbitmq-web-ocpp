package demo.csms.command;

import demo.csms.config.AmqpConfig;
import demo.csms.config.ConditionalOnRole;
import demo.csms.config.CsmsProperties;
import demo.csms.ocpp.Ocpp;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Sends CSMS-initiated CALLs and waits for the charger's CALLRESULT/CALLERROR.
 *
 * <p>The CALL is published to {@code amq.topic} with the charge point id as routing key; the
 * plugin queues it in the charger's own queue and pushes it down the WebSocket. Any number of
 * instances can run behind a load balancer: the answer ({@code <protocol>.response.conf|error})
 * lands in the shared {@code csms.responses} queue and may be consumed by any instance. The OCPP
 * message id starts with the id of the instance that sent the command, so an instance receiving
 * somebody else's answer forwards it to that instance's private queue: at most one extra hop, and
 * no instance sees all answers.
 */
@Component
@ConditionalOnRole(ConditionalOnRole.API)
public class CommandGateway {

    private static final String REPLY_QUEUE_PREFIX = "csms.replies.";

    /** The charger's answer frame and the round trip time. */
    public record Answer(String messageId, JsonNode frame, long latencyMs) {
    }

    private final RabbitTemplate rabbit;
    private final JsonMapper json;
    private final Duration timeout;
    /** Random id of this process; prefixes the message ids of its commands. */
    private final String instance = HexFormat.of().toHexDigits(ThreadLocalRandom.current().nextInt());
    private final Map<String, CompletableFuture<JsonNode>> pending = new ConcurrentHashMap<>();

    public CommandGateway(RabbitTemplate rabbit, JsonMapper json, CsmsProperties properties) {
        this.rabbit = rabbit;
        this.json = json;
        this.timeout = properties.commandTimeout();
    }

    public String replyQueue() {
        return REPLY_QUEUE_PREFIX + instance;
    }

    /** {@code <instance>-<random>}: 36 characters, the maximum length of an OCPP message id. */
    private String newMessageId() {
        return instance + "-" + UUID.randomUUID().toString().replace("-", "").substring(0, 27);
    }

    /** The instance id at the start of a message id made by {@link #newMessageId()}. */
    static String ownerOf(String messageId) {
        return messageId != null && messageId.length() == 36 && messageId.charAt(8) == '-'
                ? messageId.substring(0, 8) : null;
    }

    /** Blocks (on a virtual thread) until the charger answers or the timeout expires. */
    public Answer call(String chargerId, String action, Map<String, Object> payload)
            throws TimeoutException, InterruptedException {
        String messageId = newMessageId();
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

    /**
     * The shared queue of answers. Ours complete a pending call; the others are forwarded to
     * their owner's private queue. If that instance is gone the forward is unroutable and
     * dropped, like its HTTP request. On shutdown this listener stops first, leaving the queue
     * to the other instances, which then forward our answers to {@link #complete}.
     */
    @RabbitListener(queues = AmqpConfig.RESPONSES_QUEUE, concurrency = "1")
    public void onSharedAnswer(Message message) {
        String owner = ownerOf(message.getMessageProperties().getCorrelationId());
        if (instance.equals(owner)) {
            complete(message);
        } else if (owner != null) {
            rabbit.send("", REPLY_QUEUE_PREFIX + owner, message);
        }
    }

    /** Completes a pending call with the charger's answer, if we still wait for it. */
    public void complete(Message message) {
        MessageProperties props = message.getMessageProperties();
        CompletableFuture<JsonNode> answer = pending.remove(props.getReplyTo() + '|' + props.getCorrelationId());
        if (answer != null) {
            answer.complete(json.readTree(message.getBody()));
        }
    }
}
