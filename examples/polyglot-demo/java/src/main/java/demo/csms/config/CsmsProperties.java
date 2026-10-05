package demo.csms.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("csms")
public record CsmsProperties(
        String implementation,
        int heartbeatInterval,
        Duration commandTimeout,
        String commandApiUrl,
        String managementUrl,
        String managementUser,
        String managementPassword,
        String vhost) {
}
