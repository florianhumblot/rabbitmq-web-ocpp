package ocpp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The behaviour of one simulated charge point, as plain Java: which CALLs it sends and when, and
 * how it reacts to CSMS commands. The Gatling scenario only moves the frames: it drains
 * {@link #poll()} one CALL at a time, waits for the matching CALLRESULT, and hands it back
 * through {@link #onReply}.
 *
 * <p>An instance lives in the virtual user's session and is only touched by that user.
 */
final class ChargerModel {

    record Call(String action, String payload) {
    }

    static final ObjectMapper JSON = new ObjectMapper();

    private final Fleet.Charger charger;
    private final Config cfg;
    private final boolean v2;
    private final ArrayDeque<Call> outbox = new ArrayDeque<>();
    private final boolean[] inoperative;

    private int heartbeatInterval = 60;
    private long nextHeartbeat = Long.MAX_VALUE;
    private boolean rebootRequested;

    // At most one charging session at a time, on activeConnector (0 = idle).
    private int activeConnector;
    private long sessionEnd;
    private long nextMeter;
    private long nextSession;
    private String transactionId;
    private int seqNo;
    private double meterWh = ThreadLocalRandom.current().nextDouble(0, 1_000_000);

    ChargerModel(Fleet.Charger charger, Config cfg) {
        this.charger = charger;
        this.cfg = cfg;
        this.v2 = charger.ocppVersion().startsWith("2");
        this.inoperative = new boolean[cfg.connectors + 1];
    }

    // ---- What the scenario asks ---------------------------------------------------------

    Call poll() {
        return outbox.poll();
    }

    boolean hasOutbox() {
        return !outbox.isEmpty();
    }

    /** A Reset was accepted: reboot once the pending frames are out. */
    boolean rebootDue() {
        return rebootRequested && outbox.isEmpty();
    }

    // ---- Lifecycle ----------------------------------------------------------------------

    /** Freshly (re)connected: boot, then report every connector. */
    void onConnected(long now) {
        outbox.clear();
        rebootRequested = false;
        activeConnector = 0;
        nextHeartbeat = Long.MAX_VALUE;
        nextSession = now + random(cfg.idleMinSeconds / 10, cfg.idleMaxSeconds / 2);
        outbox.add(boot());
        for (int c = 1; c <= cfg.connectors; c++) {
            outbox.add(status(c, idleStatus(c)));
        }
    }

    void onReply(String action, String reply, long now) {
        JsonNode result = parse(reply).path(2);
        switch (action) {
            case "BootNotification" -> {
                heartbeatInterval = Math.max(1, result.path("interval").asInt(heartbeatInterval));
                nextHeartbeat = now + heartbeatInterval * 1000L;
            }
            case "StartTransaction" -> transactionId = result.path("transactionId").asText(null);
            case "Heartbeat" -> nextHeartbeat = now + heartbeatInterval * 1000L;
            default -> {
            }
        }
    }

    /** Called every tick: schedules heartbeats and drives charging sessions. */
    void tick(long now) {
        if (now >= nextHeartbeat) {
            nextHeartbeat = Long.MAX_VALUE; // re-armed by the Heartbeat reply
            outbox.add(new Call("Heartbeat", "{}"));
        }
        if (activeConnector == 0 && now >= nextSession) {
            startSession(now);
        } else if (activeConnector != 0 && now >= sessionEnd) {
            stopSession(now, "Local");
        } else if (activeConnector != 0 && now >= nextMeter) {
            meterWh += ThreadLocalRandom.current().nextDouble(7_000, 22_000) * cfg.meterIntervalSeconds / 3600;
            nextMeter = now + cfg.meterIntervalSeconds * 1000L;
            outbox.add(v2 ? transactionEvent("Updated", "MeterValuePeriodic", null) : meterValues(activeConnector));
        }
    }

    // ---- Charging sessions ----------------------------------------------------------------

    private void startSession(long now) {
        int[] free = java.util.stream.IntStream.rangeClosed(1, cfg.connectors).filter(c -> !inoperative[c]).toArray();
        nextSession = now + random(cfg.idleMinSeconds, cfg.idleMaxSeconds);
        if (free.length == 0) {
            return;
        }
        activeConnector = free[ThreadLocalRandom.current().nextInt(free.length)];
        sessionEnd = now + random(cfg.sessionMinSeconds, cfg.sessionMaxSeconds);
        nextMeter = now + cfg.meterIntervalSeconds * 1000L;
        String idTag = "TAG-" + Integer.toHexString(ThreadLocalRandom.current().nextInt()).toUpperCase();
        if (v2) {
            transactionId = UUID.randomUUID().toString();
            seqNo = 0;
            outbox.add(new Call("Authorize", obj().set("idToken", idToken(idTag)).toString()));
            outbox.add(transactionEvent("Started", "Authorized", idTag));
            outbox.add(status(activeConnector, "Occupied"));
        } else {
            transactionId = null; // assigned by the CSMS in StartTransaction.conf
            outbox.add(new Call("Authorize", obj().put("idTag", idTag).toString()));
            outbox.add(new Call("StartTransaction", obj().put("connectorId", activeConnector).put("idTag", idTag)
                    .put("meterStart", (long) meterWh).put("timestamp", now()).toString()));
            outbox.add(status(activeConnector, "Charging"));
        }
    }

    private void stopSession(long now, String reason) {
        int connector = activeConnector;
        if (v2) {
            outbox.add(transactionEvent("Ended", "StopAuthorized", null));
        } else {
            ObjectNode stop = obj().put("meterStop", (long) meterWh).put("timestamp", now()).put("reason", reason);
            stop.put("transactionId", transactionId == null ? 0 : Integer.parseInt(transactionId));
            outbox.add(new Call("StopTransaction", stop.toString()));
        }
        activeConnector = 0;
        nextSession = now + random(cfg.idleMinSeconds, cfg.idleMaxSeconds);
        outbox.add(status(connector, idleStatus(connector)));
    }

    // ---- CSMS-initiated CALLs ---------------------------------------------------------------

    /**
     * The CALLRESULT was already sent by {@link #autoReply} the moment the frame arrived; this
     * applies the side effects, from frames buffered since the previous tick.
     */
    void onCsmsFrames(Iterable<String> frames, long now) {
        for (String text : frames) {
            JsonNode frame = parse(text);
            if (frame.path(0).asInt() != 2) {
                continue; // late CALLRESULTs etc.
            }
            JsonNode payload = frame.path(3);
            switch (frame.path(2).asText()) {
                case "Reset" -> {
                    if (activeConnector != 0) {
                        stopSession(now, v2 ? "ImmediateReset" : (payload.path("type").asText().equals("Hard") ? "HardReset" : "SoftReset"));
                    }
                    rebootRequested = true;
                }
                case "ChangeAvailability" -> {
                    boolean down = (v2 ? payload.path("operationalStatus") : payload.path("type")).asText().equals("Inoperative");
                    int target = v2 ? payload.path("evse").path("id").asInt(0) : payload.path("connectorId").asInt(0);
                    for (int c = 1; c <= cfg.connectors; c++) {
                        if (target == 0 || target == c) {
                            inoperative[c] = down;
                            if (c == activeConnector && down) {
                                stopSession(now, "Other");
                            } else if (c != activeConnector) {
                                outbox.add(status(c, idleStatus(c)));
                            }
                        }
                    }
                }
                case "TriggerMessage" -> {
                    int target = v2 ? payload.path("evse").path("id").asInt(0) : payload.path("connectorId").asInt(0);
                    switch (payload.path("requestedMessage").asText()) {
                        case "BootNotification" -> outbox.add(boot());
                        case "Heartbeat" -> outbox.add(new Call("Heartbeat", "{}"));
                        case "MeterValues" -> outbox.add(meterValues(target == 0 ? 1 : target));
                        case "StatusNotification" -> {
                            for (int c = 1; c <= cfg.connectors; c++) {
                                if (target == 0 || target == c) {
                                    outbox.add(status(c, c == activeConnector ? (v2 ? "Occupied" : "Charging") : idleStatus(c)));
                                }
                            }
                        }
                        default -> {
                        }
                    }
                }
                default -> {
                }
            }
        }
    }

    /**
     * Gatling hook answering CSMS CALLs immediately, from the network thread, in any state of the
     * WebSocket (even while the user awaits the answer to its own CALL). Returns null for frames
     * that are not CALLs.
     */
    static String autoReply(String text) {
        if (!text.startsWith("[2")) {
            return null;
        }
        JsonNode frame = parse(text);
        if (frame.path(0).asInt() != 2) {
            return null;
        }
        String messageId = frame.path(1).asText();
        return switch (frame.path(2).asText()) {
            case "Reset", "ChangeAvailability", "TriggerMessage" ->
                    "[3," + JSON.valueToTree(messageId) + ",{\"status\":\"Accepted\"}]";
            default -> "[4," + JSON.valueToTree(messageId) + ",\"NotImplemented\",\"\",{}]";
        };
    }

    // ---- Payloads -----------------------------------------------------------------------------

    private Call boot() {
        ObjectNode p = obj();
        if (v2) {
            p.put("reason", "PowerUp").putObject("chargingStation").put("vendorName", "Gatling")
                    .put("model", "SimCharger-21").put("serialNumber", charger.id()).put("firmwareVersion", "1.0.0");
        } else {
            p.put("chargePointVendor", "Gatling").put("chargePointModel", "SimCharger-16")
                    .put("chargePointSerialNumber", charger.id()).put("firmwareVersion", "1.0.0");
        }
        return new Call("BootNotification", p.toString());
    }

    private String idleStatus(int connector) {
        return inoperative[connector] ? "Unavailable" : "Available";
    }

    private Call status(int connector, String status) {
        ObjectNode p = obj();
        if (v2) {
            p.put("timestamp", now()).put("connectorStatus", status).put("evseId", connector).put("connectorId", 1);
        } else {
            p.put("connectorId", connector).put("errorCode", "NoError").put("status", status).put("timestamp", now());
        }
        return new Call("StatusNotification", p.toString());
    }

    private Call meterValues(int connector) {
        ObjectNode p = obj();
        ArrayNode values = (v2 ? p.put("evseId", connector) : p.put("connectorId", connector)).putArray("meterValue");
        values.add(meterValue());
        if (!v2 && transactionId != null && connector == activeConnector) {
            p.put("transactionId", Integer.parseInt(transactionId));
        }
        return new Call("MeterValues", p.toString());
    }

    private ObjectNode meterValue() {
        ObjectNode sample = obj().put("measurand", "Energy.Active.Import.Register");
        if (v2) {
            sample.put("value", Math.round(meterWh)).putObject("unitOfMeasure").put("unit", "Wh");
        } else {
            sample.put("value", Long.toString(Math.round(meterWh))).put("unit", "Wh");
        }
        ObjectNode mv = obj().put("timestamp", now());
        mv.putArray("sampledValue").add(sample);
        return mv;
    }

    private Call transactionEvent(String eventType, String trigger, String idTag) {
        ObjectNode p = obj().put("eventType", eventType).put("timestamp", now()).put("triggerReason", trigger)
                .put("seqNo", seqNo++);
        ObjectNode info = p.putObject("transactionInfo").put("transactionId", transactionId);
        if (eventType.equals("Ended")) {
            info.put("stoppedReason", "Local");
        }
        if (idTag != null) {
            p.set("idToken", idToken(idTag));
            p.putObject("evse").put("id", activeConnector).put("connectorId", 1);
        }
        if (!eventType.equals("Started")) {
            p.putArray("meterValue").add(meterValue());
        }
        return new Call("TransactionEvent", p.toString());
    }

    private static ObjectNode idToken(String idTag) {
        return obj().put("idToken", idTag).put("type", "ISO14443");
    }

    // ---- Helpers --------------------------------------------------------------------------------

    private static ObjectNode obj() {
        return JSON.createObjectNode();
    }

    static JsonNode parse(String text) {
        try {
            return text == null ? JSON.missingNode() : JSON.readTree(text);
        } catch (Exception e) {
            return JSON.missingNode();
        }
    }

    private static String now() {
        return Instant.now().truncatedTo(ChronoUnit.MILLIS).toString();
    }

    private static long random(int minSeconds, int maxSeconds) {
        return ThreadLocalRandom.current().nextLong(minSeconds * 1000L, Math.max(minSeconds + 1, maxSeconds) * 1000L);
    }

    @Override
    public String toString() {
        return charger.id() + " outbox=" + outbox.size() + " active=" + activeConnector
                + " inoperative=" + Arrays.toString(inoperative);
    }
}
