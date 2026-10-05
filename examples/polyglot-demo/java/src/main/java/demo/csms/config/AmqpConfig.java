package demo.csms.config;

import demo.csms.command.CommandGateway;
import java.util.Map;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Base64UrlNamingStrategy;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.boot.web.server.context.WebServerGracefulShutdownLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * AMQP topology. The plugin publishes every charger frame to {@code amq.topic} with the routing
 * key {@code <protocol>.<Action>.<req|conf|error>} and consumes, per charger, a queue bound with
 * the charge point id as routing key.
 *
 * <p>The shared queues are durable classic queues: the plugin publishes into them directly and
 * does not yet keep the client state quorum queues need.
 */
@Configuration
public class AmqpConfig {

    public static final String EXCHANGE = "amq.topic";
    /** Shared work queue: every charger-initiated CALL, consumed by competing workers. */
    public static final String REQUESTS_QUEUE = "csms.requests";
    /** Charger answers to CSMS commands, consumed by competing command API instances. */
    public static final String RESPONSES_QUEUE = "csms.responses";

    @Bean
    TopicExchange ocppExchange() {
        // amq.topic always exists; declaring it lets bindings reference it.
        return new TopicExchange(EXCHANGE, true, false);
    }

    @Bean
    @ConditionalOnRole(ConditionalOnRole.WORKER)
    Declarables requestsTopology(TopicExchange ocppExchange) {
        Queue queue = QueueBuilder.durable(REQUESTS_QUEUE).build();
        return new Declarables(queue, BindingBuilder.bind(queue).to(ocppExchange).with("*.*.req"));
    }

    @Bean
    @ConditionalOnRole(ConditionalOnRole.API)
    Declarables responsesTopology(TopicExchange ocppExchange, CommandGateway gateway) {
        Queue responses = QueueBuilder.durable(RESPONSES_QUEUE).build();
        // This instance's private queue, for answers other instances forward to it.
        Queue replies = new Queue(gateway.replyQueue(), false, true, true);
        return new Declarables(responses, replies,
                BindingBuilder.bind(responses).to(ocppExchange).with("*.response.conf"),
                BindingBuilder.bind(responses).to(ocppExchange).with("*.response.error"));
    }

    /**
     * Consumes this instance's private reply queue. Its lifecycle phase is below the web
     * server's graceful shutdown, so it keeps completing the commands in flight while the HTTP
     * requests drain, and stops after them.
     */
    @Bean
    @ConditionalOnRole(ConditionalOnRole.API)
    SimpleMessageListenerContainer replyContainer(ConnectionFactory connectionFactory, CommandGateway gateway) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames(gateway.replyQueue());
        container.setAcknowledgeMode(AcknowledgeMode.NONE);
        container.setExclusive(true);
        container.setMessageListener(gateway::complete);
        container.setPhase(WebServerGracefulShutdownLifecycle.SMART_LIFECYCLE_PHASE - 1);
        return container;
    }

    /**
     * Per-instance monitoring tap of the dashboard: an exclusive, auto-deleted copy of all OCPP
     * traffic in this vhost (both directions). Bounded so a slow dashboard can never hurt the
     * broker.
     */
    @Bean
    @ConditionalOnRole(ConditionalOnRole.DASHBOARD)
    Queue tapQueue() {
        return new AnonymousQueue(new Base64UrlNamingStrategy("csms.tap.java."),
                Map.of("x-max-length", 50_000, "x-overflow", "drop-head"));
    }

    @Bean
    @ConditionalOnRole(ConditionalOnRole.DASHBOARD)
    Binding tapBinding(Queue tapQueue, TopicExchange ocppExchange) {
        return BindingBuilder.bind(tapQueue).to(ocppExchange).with("#");
    }

    /** The tap is consumed by a single thread without acknowledgements. */
    @Bean
    @ConditionalOnRole(ConditionalOnRole.DASHBOARD)
    SimpleRabbitListenerContainerFactory tapContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setAcknowledgeMode(AcknowledgeMode.NONE);
        factory.setConcurrentConsumers(1);
        factory.setPrefetchCount(1000);
        return factory;
    }
}
