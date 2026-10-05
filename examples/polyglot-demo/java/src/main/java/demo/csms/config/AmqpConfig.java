package demo.csms.config;

import java.util.Map;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Base64UrlNamingStrategy;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * AMQP topology. The plugin publishes every charger frame to {@code amq.topic} with the routing
 * key {@code <protocol>.<Action>.<req|conf|error>} and consumes, per charger, a queue bound with
 * the charge point id as routing key.
 */
@Configuration
public class AmqpConfig {

    public static final String EXCHANGE = "amq.topic";
    /** Shared work queue: every charger-initiated CALL, consumed by competing workers. */
    public static final String REQUESTS_QUEUE = "csms.requests";

    @Bean
    TopicExchange ocppExchange() {
        // amq.topic always exists; declaring it lets bindings reference it.
        return new TopicExchange(EXCHANGE, true, false);
    }

    @Bean
    Queue requestsQueue() {
        return QueueBuilder.durable(REQUESTS_QUEUE).build();
    }

    @Bean
    Binding requestsBinding(Queue requestsQueue, TopicExchange ocppExchange) {
        return BindingBuilder.bind(requestsQueue).to(ocppExchange).with("*.*.req");
    }

    /**
     * Per-instance monitoring tap: an exclusive, auto-deleted copy of all OCPP traffic in this
     * vhost (both directions). Bounded so a slow dashboard can never hurt the broker.
     */
    @Bean
    Queue tapQueue() {
        return new AnonymousQueue(new Base64UrlNamingStrategy("csms.tap.java."),
                Map.of("x-max-length", 50_000, "x-overflow", "drop-head"));
    }

    @Bean
    Binding tapBinding(Queue tapQueue, TopicExchange ocppExchange) {
        return BindingBuilder.bind(tapQueue).to(ocppExchange).with("#");
    }

    /** The tap is consumed by a single thread without acknowledgements. */
    @Bean
    SimpleRabbitListenerContainerFactory tapContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setAcknowledgeMode(AcknowledgeMode.NONE);
        factory.setConcurrentConsumers(1);
        factory.setPrefetchCount(1000);
        return factory;
    }
}
