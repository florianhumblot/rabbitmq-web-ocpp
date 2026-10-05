package demo.csms.registry;

import demo.csms.config.CsmsProperties;
import demo.csms.ocpp.Ocpp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Predicate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

/**
 * The charger registry shared by every instance, kept in Valkey. Workers record what chargers
 * send; the command API and the dashboard read it. The update logic is {@code
 * valkey/registry.lua}, shared with the Rust and Go implementations.
 */
@Component
public class Registry {

    private static final List<String> VERSIONS = List.of("ocpp16", "ocpp201", "ocpp21");
    private static final List<String> PROFILES = List.of("1", "2", "3");

    public record ChargerView(String id, String ocppVersion, String securityProfile, boolean online,
                              long lastSeen, Map<String, String> connectors) {
    }

    public record Summary(long known, long online, Map<String, Long> byVersion,
                          Map<String, Long> bySecurityProfile) {
    }

    public record Lookup(String version, boolean online) {
    }

    private final StringRedisTemplate redis;
    private final RedisScript<String> script =
            RedisScript.of(new ClassPathResource("registry.lua"), String.class);
    /** One hash tag per vhost: everything of an implementation on one shard. */
    private final String prefix;

    public Registry(StringRedisTemplate redis, CsmsProperties properties) {
        this.redis = redis;
        this.prefix = "csms:{" + properties.vhost() + "}:";
    }

    private String key(String id) {
        return prefix + "c:" + id;
    }

    private String run(String id, String op, String... args) {
        List<String> all = new ArrayList<>(List.of(prefix, op, id, Long.toString(System.currentTimeMillis())));
        all.addAll(List.of(args));
        return redis.execute(script, List.of(key(id)), all.toArray());
    }

    /**
     * Records a CALL or SEND a worker processed. When an online/offline transition looks due,
     * {@code connected} asks the broker whether the charger really is connected, as competing
     * workers may process frames out of order.
     */
    public void observe(String id, String version, String action, JsonNode payload, Predicate<String> connected) {
        String result;
        if (Ocpp.isSyntheticOffline(action, payload)) {
            result = run(id, "offline");
        } else {
            String[] status = statusOf(version, action, payload);
            String sp = Optional.ofNullable(Ocpp.securityProfile(id)).orElse("");
            result = run(id, "event", version, sp, status[0], status[1], status[2]);
        }
        if ("check".equals(result)) {
            run(id, "presence", connected.test(id) ? "1" : "0");
        }
    }

    /** Connector status of a StatusNotification: connector, status, timestamp (or blanks). */
    private static String[] statusOf(String version, String action, JsonNode payload) {
        if (!"StatusNotification".equals(action) || payload == null) {
            return new String[] {"", "", ""};
        }
        // 1.6: connectorId + status. 2.x: evseId + connectorStatus (one connector per EVSE).
        boolean v2 = Ocpp.isV2(version);
        int connector = (v2 ? payload.path("evseId") : payload.path("connectorId")).asInt(0);
        String status = (v2 ? payload.path("connectorStatus") : payload.path("status")).asString("");
        if (connector <= 0 || status.isEmpty()) {
            return new String[] {"", "", ""};
        }
        return new String[] {Integer.toString(connector), status, payload.path("timestamp").asString("")};
    }

    public Optional<Lookup> lookup(String id) {
        List<Object> values = redis.opsForHash().multiGet(key(id), List.of("v", "on"));
        return values.get(0) == null ? Optional.empty()
                : Optional.of(new Lookup((String) values.get(0), "1".equals(values.get(1))));
    }

    private static ChargerView view(String id, Map<?, ?> hash) {
        Map<String, String> connectors = new TreeMap<>();
        hash.forEach((k, v) -> {
            if (k.toString().startsWith("c:")) {
                connectors.put(k.toString().substring(2), v.toString());
            }
        });
        Object sp = hash.get("sp");
        Object seen = hash.get("seen");
        return new ChargerView(id, (String) hash.get("v"), sp == null || sp.toString().isEmpty() ? null : sp.toString(),
                "1".equals(hash.get("on")), seen == null ? 0 : Long.parseLong(seen.toString()), connectors);
    }

    public Optional<ChargerView> get(String id) {
        Map<Object, Object> hash = redis.opsForHash().entries(key(id));
        return hash.isEmpty() ? Optional.empty() : Optional.of(view(id, hash));
    }

    /** Up to {@code limit} connected chargers. */
    public List<ChargerView> list(int limit) {
        List<String> ids = new ArrayList<>(redis.opsForSet().distinctRandomMembers(prefix + "online", limit));
        ids.sort(Comparator.naturalOrder());
        List<Object> hashes = redis.executePipelined((RedisCallback<Object>) connection -> {
            ids.forEach(id -> connection.hashCommands().hGetAll(bytes(key(id))));
            return null;
        });
        List<ChargerView> views = new ArrayList<>(ids.size());
        for (int i = 0; i < ids.size(); i++) {
            views.add(view(ids.get(i), (Map<?, ?>) hashes.get(i)));
        }
        return views;
    }

    /**
     * Connected chargers by protocol and security profile, and their connectors by status, in
     * one round trip.
     */
    public Map.Entry<Summary, Map<String, Long>> summary() {
        List<Object> results = redis.executePipelined((RedisCallback<Object>) connection -> {
            scard(connection, "known");
            scard(connection, "online");
            VERSIONS.forEach(v -> scard(connection, "online:v:" + v));
            PROFILES.forEach(sp -> scard(connection, "online:sp:" + sp));
            connection.hashCommands().hGetAll(bytes(prefix + "status"));
            return null;
        });
        Map<String, Long> byVersion = new TreeMap<>();
        for (int i = 0; i < VERSIONS.size(); i++) {
            putPositive(byVersion, VERSIONS.get(i), (Long) results.get(2 + i));
        }
        Map<String, Long> byProfile = new TreeMap<>();
        for (int i = 0; i < PROFILES.size(); i++) {
            putPositive(byProfile, PROFILES.get(i), (Long) results.get(2 + VERSIONS.size() + i));
        }
        Map<String, Long> connectors = new LinkedHashMap<>();
        ((Map<?, ?>) results.getLast()).forEach((status, count) ->
                putPositive(connectors, status.toString(), Long.parseLong(count.toString())));
        return Map.entry(new Summary((Long) results.get(0), (Long) results.get(1), byVersion, byProfile), connectors);
    }

    private void scard(RedisConnection connection, String name) {
        connection.setCommands().sCard(bytes(prefix + name));
    }

    private static void putPositive(Map<String, Long> map, String key, Long value) {
        if (value != null && value > 0) {
            map.put(key, value);
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
