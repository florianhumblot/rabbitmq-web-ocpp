package demo.csms;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * OCPP CSMS on top of rabbitmq-web-ocpp, as a Spring Boot modular monolith.
 *
 * <ul>
 *   <li>{@code ocpp} - stateless workers answering charger requests from the shared queue</li>
 *   <li>{@code command} - REST API sending CSMS-initiated calls to chargers</li>
 *   <li>{@code monitor} - a tap on the OCPP exchange feeding the charger registry and traffic
 *       statistics, plus the management API poller</li>
 *   <li>{@code dashboard} - WebSocket pushing a snapshot to the browser every second</li>
 * </ul>
 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class CsmsApplication {

    public static void main(String[] args) {
        SpringApplication.run(CsmsApplication.class, args);
    }
}
