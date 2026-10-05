package demo.csms.command;

import demo.csms.config.ConditionalOnRole;
import demo.csms.registry.Registry;
import demo.csms.ocpp.Ocpp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;

/**
 * Version-neutral command API. The request is translated to the OCPP 1.6 or 2.x payload based
 * on the protocol recorded in the shared registry, or on an explicit {@code ocppVersion} field in
 * the body. Stateless: any instance can serve any request.
 */
@RestController
@ConditionalOnRole(ConditionalOnRole.API)
@RequestMapping("/api/chargers")
public class CommandController {

    private static final Set<String> VERSIONS = Set.of("ocpp16", "ocpp201", "ocpp21");
    private static final Set<String> TRIGGERABLE =
            Set.of("BootNotification", "Heartbeat", "MeterValues", "StatusNotification");

    private final Registry registry;
    private final CommandGateway gateway;

    public CommandController(Registry registry, CommandGateway gateway) {
        this.registry = registry;
        this.gateway = gateway;
    }

    @GetMapping
    public List<Registry.ChargerView> list(@RequestParam(defaultValue = "100") int limit) {
        return registry.list(Math.max(0, limit));
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> get(@PathVariable String id) {
        return registry.get(id).<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> error(HttpStatus.NOT_FOUND, "unknown charge point " + id));
    }

    @PostMapping("/{id}/reset")
    public ResponseEntity<?> reset(@PathVariable String id, @RequestBody(required = false) JsonNode body) {
        JsonNode b = orEmpty(body);
        String type = b.path("type").asString("Soft");
        if (!type.equals("Soft") && !type.equals("Hard")) {
            return error(HttpStatus.BAD_REQUEST, "type must be Soft or Hard");
        }
        return send(id, b, "Reset", v2 -> v2
                ? Map.of("type", type.equals("Hard") ? "Immediate" : "OnIdle")
                : Map.of("type", type));
    }

    @PostMapping("/{id}/change-availability")
    public ResponseEntity<?> changeAvailability(@PathVariable String id, @RequestBody(required = false) JsonNode body) {
        JsonNode b = orEmpty(body);
        String type = b.path("type").asString("Inoperative");
        int connector = b.path("connectorId").asInt(0);
        if (!type.equals("Operative") && !type.equals("Inoperative") || connector < 0) {
            return error(HttpStatus.BAD_REQUEST, "type must be Operative or Inoperative, connectorId >= 0");
        }
        return send(id, b, "ChangeAvailability", v2 -> {
            Map<String, Object> p = new LinkedHashMap<>();
            if (v2) {
                p.put("operationalStatus", type);
                if (connector > 0) {
                    p.put("evse", Map.of("id", connector));
                }
            } else {
                p.put("connectorId", connector);
                p.put("type", type);
            }
            return p;
        });
    }

    @PostMapping("/{id}/trigger-message")
    public ResponseEntity<?> triggerMessage(@PathVariable String id, @RequestBody(required = false) JsonNode body) {
        JsonNode b = orEmpty(body);
        String requested = b.path("requestedMessage").asString("StatusNotification");
        int connector = b.path("connectorId").asInt(0);
        if (!TRIGGERABLE.contains(requested) || connector < 0) {
            return error(HttpStatus.BAD_REQUEST, "requestedMessage must be one of " + TRIGGERABLE);
        }
        return send(id, b, "TriggerMessage", v2 -> {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("requestedMessage", requested);
            if (connector > 0) {
                p.put(v2 ? "evse" : "connectorId", v2 ? Map.of("id", connector) : connector);
            }
            return p;
        });
    }

    private interface PayloadBuilder {
        Map<String, Object> build(boolean v2);
    }

    private ResponseEntity<?> send(String id, JsonNode body, String action, PayloadBuilder builder) {
        String version = body.path("ocppVersion").asString(null);
        if (version != null && !VERSIONS.contains(version)) {
            return error(HttpStatus.BAD_REQUEST, "ocppVersion must be one of " + VERSIONS);
        }
        if (version == null) {
            Registry.Lookup charger = registry.lookup(id).orElse(null);
            if (charger == null) {
                return error(HttpStatus.NOT_FOUND, "unknown charge point " + id + " (pass ocppVersion to force)");
            }
            if (!charger.online()) {
                return error(HttpStatus.CONFLICT, "charge point " + id + " is offline");
            }
            version = charger.version();
        }
        Map<String, Object> payload = builder.build(Ocpp.isV2(version));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("chargerId", id);
        result.put("action", action);
        result.put("ocppVersion", version);
        result.put("request", payload);
        try {
            CommandGateway.Answer answer = gateway.call(id, action, payload);
            JsonNode frame = answer.frame();
            result.put("messageId", answer.messageId());
            result.put("latencyMs", answer.latencyMs());
            if (frame.get(0).asInt() == Ocpp.CALLRESULT) {
                result.put("status", frame.get(2).path("status").asString(null));
                result.put("response", frame.get(2));
                return ResponseEntity.ok(result);
            }
            result.put("error", Map.of("code", frame.path(2).asString(""), "description", frame.path(3).asString("")));
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(result);
        } catch (TimeoutException e) {
            result.put("error", "no answer from the charge point in time");
            return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT).body(result);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return error(HttpStatus.SERVICE_UNAVAILABLE, "interrupted");
        }
    }

    private static JsonNode orEmpty(JsonNode body) {
        return body == null ? JsonNodeFactory.instance.objectNode() : body;
    }

    private static ResponseEntity<Map<String, String>> error(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(Map.of("error", message));
    }
}
