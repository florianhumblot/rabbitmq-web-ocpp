package ocpp;

import java.nio.file.Path;

/** Simulation parameters, from environment variables (or -D system properties). */
final class Config {

    final Path dataDir = Path.of(str("DATA_DIR", "../generated"));
    final String host = str("TARGET_HOST", "localhost");
    final int wsPort = num("WS_PORT", 19520);
    final int wssPort = num("WSS_PORT", 19521);
    final String vhost = str("VHOST", "csms-java");
    /** 0 = every charger in chargers.csv. */
    final int chargers = num("CHARGERS", 0);
    final int rampSeconds = num("RAMP_SECONDS", 120);
    /** Steady state after the ramp; the chargers disconnect when it is over. */
    final int durationSeconds = num("DURATION_SECONDS", 600);
    final String csmsApi = str("CSMS_API", "http://localhost:8080");
    final double commandsPerSecond = Double.parseDouble(str("COMMANDS_PER_SEC", "2"));
    final int connectors = num("CONNECTORS", 2);
    final int meterIntervalSeconds = num("METER_INTERVAL_SECONDS", 60);
    final int callTimeoutSeconds = num("CALL_TIMEOUT_SECONDS", 60);
    /** Charging sessions: idle gap and duration, in seconds. */
    final int idleMinSeconds = num("IDLE_MIN_SECONDS", 300);
    final int idleMaxSeconds = num("IDLE_MAX_SECONDS", 1800);
    final int sessionMinSeconds = num("SESSION_MIN_SECONDS", 300);
    final int sessionMaxSeconds = num("SESSION_MAX_SECONDS", 1200);

    private static String str(String key, String def) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            v = System.getProperty(key);
        }
        return v == null || v.isBlank() ? def : v;
    }

    private static int num(String key, int def) {
        return Integer.parseInt(str(key, Integer.toString(def)));
    }

    @Override
    public String toString() {
        return "host=" + host + " vhost=" + vhost + " chargers=" + (chargers == 0 ? "all" : chargers)
                + " ramp=" + rampSeconds + "s duration=" + durationSeconds + "s api=" + csmsApi
                + " commands/s=" + commandsPerSecond;
    }
}
