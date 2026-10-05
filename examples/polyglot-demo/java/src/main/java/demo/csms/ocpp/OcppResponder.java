package demo.csms.ocpp;

import demo.csms.config.CsmsProperties;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

/** Builds the CSMS answer to a charger-initiated CALL, for OCPP 1.6 and 2.x. */
@Component
public class OcppResponder {

    private static final Map<String, Object> ACCEPTED = Map.of("status", "Accepted");

    private final int heartbeatInterval;
    private final AtomicInteger transactionIds = new AtomicInteger();

    public OcppResponder(CsmsProperties properties) {
        this.heartbeatInterval = properties.heartbeatInterval();
    }

    /** Returns the complete CALLRESULT or CALLERROR frame. */
    public List<Object> reply(String version, String messageId, String action, JsonNode payload) {
        boolean v2 = Ocpp.isV2(version);
        Map<String, Object> result = switch (action) {
            case "BootNotification" -> Map.of(
                    "status", "Accepted", "currentTime", Ocpp.now(), "interval", heartbeatInterval);
            case "Heartbeat" -> Map.of("currentTime", Ocpp.now());
            case "Authorize" -> v2 ? Map.of("idTokenInfo", ACCEPTED) : Map.of("idTagInfo", ACCEPTED);
            case "StartTransaction" -> Map.of(
                    "transactionId", transactionIds.incrementAndGet(), "idTagInfo", ACCEPTED);
            case "StopTransaction" -> Map.of("idTagInfo", ACCEPTED);
            case "TransactionEvent" -> payload != null && payload.has("idToken")
                    ? Map.of("idTokenInfo", ACCEPTED) : Map.of();
            case "DataTransfer" -> ACCEPTED;
            case "StatusNotification", "MeterValues", "DiagnosticsStatusNotification",
                 "FirmwareStatusNotification", "SecurityEventNotification", "LogStatusNotification",
                 "NotifyReport", "NotifyEvent", "NotifyMonitoringReport" -> Map.of();
            default -> null;
        };
        if (result == null) {
            return List.of(Ocpp.CALLERROR, messageId, "NotImplemented",
                    "Action " + action + " is not supported by this CSMS", Map.of());
        }
        return List.of(Ocpp.CALLRESULT, messageId, result);
    }
}
