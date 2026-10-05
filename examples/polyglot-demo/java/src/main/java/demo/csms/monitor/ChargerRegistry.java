package demo.csms.monitor;

import demo.csms.ocpp.Ocpp;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

/**
 * What the CSMS knows about every charge point, learned purely from the OCPP traffic seen on
 * the broker: version from the routing key, online state from any frame (and the plugin's
 * synthetic offline StatusNotification), connector state from StatusNotification.
 */
@Component
public class ChargerRegistry {

    public static final class Charger {
        final String id;
        final String securityProfile;
        volatile String version;
        volatile boolean online;
        volatile long lastSeen;
        final Map<String, String> connectors = new ConcurrentHashMap<>();

        Charger(String id) {
            this.id = id;
            this.securityProfile = Ocpp.securityProfile(id);
        }

        public String version() {
            return version;
        }

        public boolean online() {
            return online;
        }

        public ChargerView view() {
            return new ChargerView(id, version, securityProfile, online, lastSeen, new TreeMap<>(connectors));
        }
    }

    public record ChargerView(String id, String ocppVersion, String securityProfile, boolean online,
                              long lastSeen, Map<String, String> connectors) {
    }

    public record Summary(long known, long online, Map<String, Long> byVersion,
                          Map<String, Long> bySecurityProfile, Map<String, Long> connectors) {
    }

    private final Map<String, Charger> chargers = new ConcurrentHashMap<>();

    /** A charger-initiated CALL or SEND. */
    public void onChargerRequest(String chargerId, String version, String action, JsonNode payload, long now) {
        Charger c = chargers.computeIfAbsent(chargerId, Charger::new);
        c.version = version;
        c.lastSeen = now;
        if (Ocpp.isSyntheticOffline(action, payload)) {
            c.online = false;
            return;
        }
        c.online = true;
        if ("StatusNotification".equals(action) && payload != null) {
            // 1.6: connectorId + status. 2.x: evseId + connectorStatus (the simulator has one
            // connector per EVSE, so the EVSE id identifies the connector).
            boolean v2 = Ocpp.isV2(version);
            int connector = (v2 ? payload.path("evseId") : payload.path("connectorId")).asInt(0);
            String status = (v2 ? payload.path("connectorStatus") : payload.path("status")).asString("");
            if (connector > 0 && !status.isEmpty()) {
                c.connectors.put(Integer.toString(connector), status);
            }
        }
    }

    /** A CALLRESULT/CALLERROR from the charger proves it is connected. */
    public void onChargerResponse(String chargerId, String version, long now) {
        Charger c = chargers.computeIfAbsent(chargerId, Charger::new);
        c.version = version;
        c.lastSeen = now;
        c.online = true;
    }

    public Charger get(String chargerId) {
        return chargers.get(chargerId);
    }

    public List<ChargerView> list(int limit) {
        return chargers.values().stream()
                .sorted(Comparator.comparing((Charger c) -> !c.online).thenComparing(c -> c.id))
                .limit(limit)
                .map(Charger::view)
                .toList();
    }

    public Summary summary() {
        long online = 0;
        Map<String, Long> byVersion = new TreeMap<>();
        Map<String, Long> byProfile = new TreeMap<>();
        Map<String, Long> connectors = new TreeMap<>();
        for (Charger c : chargers.values()) {
            if (!c.online) {
                continue;
            }
            online++;
            byVersion.merge(c.version, 1L, Long::sum);
            if (c.securityProfile != null) {
                byProfile.merge(c.securityProfile, 1L, Long::sum);
            }
            for (String status : c.connectors.values()) {
                connectors.merge(status, 1L, Long::sum);
            }
        }
        return new Summary(chargers.size(), online, byVersion, byProfile, connectors);
    }
}
