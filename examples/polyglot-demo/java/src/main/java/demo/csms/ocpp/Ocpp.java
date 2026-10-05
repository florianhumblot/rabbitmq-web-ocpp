package demo.csms.ocpp;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import tools.jackson.databind.JsonNode;

/** OCPP-J framing helpers shared by the worker, the tap and the command API. */
public final class Ocpp {

    public static final int CALL = 2;
    public static final int CALLRESULT = 3;
    public static final int CALLERROR = 4;
    public static final int CALLRESULTERROR = 5;
    public static final int SEND = 6;

    /** Routing keys of frames sent by chargers: {@code ocpp16.BootNotification.req}. */
    private static final Pattern CHARGER_ROUTING_KEY =
            Pattern.compile("^(ocpp\\d+)\\.([A-Za-z0-9]+)\\.(req|conf|error)$");
    private static final Pattern SECURITY_PROFILE = Pattern.compile("-sp(\\d)$");

    private Ocpp() {
    }

    /** Parsed routing key of a charger-originated frame. */
    public record ChargerKey(String version, String action, String direction) {
    }

    /** Returns the parsed key, or null when the frame was published by the CSMS. */
    public static ChargerKey parseRoutingKey(String routingKey) {
        Matcher m = CHARGER_ROUTING_KEY.matcher(routingKey);
        return m.matches() ? new ChargerKey(m.group(1), m.group(2), m.group(3)) : null;
    }

    /** OCPP 2.x (ocpp20, ocpp201, ocpp21) versus 1.x payload family. */
    public static boolean isV2(String version) {
        return version != null && version.startsWith("ocpp2");
    }

    /** The demo encodes the security profile in the charge point id ({@code ...-sp3}). */
    public static String securityProfile(String chargerId) {
        Matcher m = SECURITY_PROFILE.matcher(chargerId);
        return m.find() ? m.group(1) : null;
    }

    public static String kindName(int type) {
        return switch (type) {
            case CALL -> "CALL";
            case CALLRESULT -> "CALLRESULT";
            case CALLERROR -> "CALLERROR";
            case CALLRESULTERROR -> "CALLRESULTERROR";
            case SEND -> "SEND";
            default -> "UNKNOWN";
        };
    }

    /**
     * The plugin publishes one synthetic StatusNotification when a charger disconnects. It must
     * not be answered: the charger is gone.
     */
    public static boolean isSyntheticOffline(String action, JsonNode payload) {
        if (!"StatusNotification".equals(action) || payload == null) {
            return false;
        }
        JsonNode source = payload.has("customData") ? payload.get("customData") : payload;
        return "rabbitmq".equals(source.path("vendorId").asString(""))
                && "Offline".equals(source.path("vendorErrorCode").asString(""));
    }

    public static String now() {
        return Instant.now().truncatedTo(ChronoUnit.MILLIS).toString();
    }
}
