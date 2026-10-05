package demo.csms.monitor;

import demo.csms.config.ConditionalOnRole;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.springframework.stereotype.Component;

/**
 * Traffic counters fed by the tap: message rates, CSMS reply latency (charger request seen on
 * the broker until the CSMS answer is seen on the broker) and command outcomes.
 */
@Component
@ConditionalOnRole(ConditionalOnRole.DASHBOARD)
public class TrafficStats {

    public record RecentFrame(long ts, String direction, String chargerId, String kind, String action,
                              String messageId) {
    }

    public record ActionRate(String action, String direction, double perSec) {
    }

    public record Traffic(double inPerSec, double outPerSec, List<ActionRate> byAction) {
    }

    public record Latency(int samples, double p50, double p95, double p99, double max) {
    }

    public record Commands(long sent, long accepted, long rejected, long failed, long pending) {
    }

    public record Window(Traffic traffic, Latency latency, Commands commands, List<RecentFrame> recent) {
    }

    private record Pending(long ts, String action) {
    }

    private static final int RECENT_SIZE = 30;
    private static final long PENDING_EXPIRY_MS = 60_000;

    private final LongAdder inbound = new LongAdder();
    private final LongAdder outbound = new LongAdder();
    private final Map<String, LongAdder> actions = new ConcurrentHashMap<>();
    private final Map<String, Long> awaitingReply = new ConcurrentHashMap<>();
    private final Map<String, Pending> commandsInFlight = new ConcurrentHashMap<>();
    private final LongAdder sent = new LongAdder();
    private final LongAdder accepted = new LongAdder();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final List<Double> latencies = new ArrayList<>();
    private final ArrayDeque<RecentFrame> recent = new ArrayDeque<>(RECENT_SIZE);

    // Values at the previous window, to turn cumulative counters into rates.
    private long lastWindowAt = System.nanoTime();
    private long lastIn;
    private long lastOut;
    private final Map<String, Long> lastActions = new ConcurrentHashMap<>();

    private static String key(String chargerId, String messageId) {
        return chargerId + '|' + messageId;
    }

    /** CALL or SEND from a charger. CALLs are timed until the CSMS answers. */
    public void chargerRequest(String chargerId, String messageId, String action, String kind,
                               boolean expectsReply, long now) {
        inbound.increment();
        actions.computeIfAbsent("in|" + action, k -> new LongAdder()).increment();
        if (expectsReply) {
            awaitingReply.put(key(chargerId, messageId), now);
        }
        remember(new RecentFrame(now, "in", chargerId, kind, action, messageId));
    }

    /** CALLRESULT/CALLERROR from a charger, answering a CSMS command. */
    public void chargerResponse(String chargerId, String messageId, String kind, String status, long now) {
        inbound.increment();
        Pending command = commandsInFlight.remove(key(chargerId, messageId));
        if (command != null) {
            if (!"CALLRESULT".equals(kind)) {
                failed.increment();
            } else if (status == null || "Accepted".equals(status) || "Scheduled".equals(status)) {
                accepted.increment();
            } else {
                rejected.increment();
            }
        }
        remember(new RecentFrame(now, "in", chargerId, kind, command == null ? null : command.action(), messageId));
    }

    /** CALL from the CSMS to a charger (a command). */
    public void csmsCall(String chargerId, String messageId, String action, long now) {
        outbound.increment();
        sent.increment();
        actions.computeIfAbsent("out|" + action, k -> new LongAdder()).increment();
        commandsInFlight.put(key(chargerId, messageId), new Pending(now, action));
        remember(new RecentFrame(now, "out", chargerId, "CALL", action, messageId));
    }

    /** CALLRESULT/CALLERROR from the CSMS, answering a charger request. */
    public void csmsReply(String chargerId, String messageId, String kind, long now) {
        outbound.increment();
        Long requestedAt = awaitingReply.remove(key(chargerId, messageId));
        if (requestedAt != null) {
            synchronized (latencies) {
                latencies.add((double) (now - requestedAt));
            }
        }
        remember(new RecentFrame(now, "out", chargerId, kind, null, messageId));
    }

    private void remember(RecentFrame frame) {
        synchronized (recent) {
            if (recent.size() == RECENT_SIZE) {
                recent.removeLast();
            }
            recent.addFirst(frame);
        }
    }

    /** Closes the current window: rates since the previous call and its latency distribution. */
    public synchronized Window window() {
        long nowNanos = System.nanoTime();
        double seconds = Math.max(0.001, (nowNanos - lastWindowAt) / 1e9);
        lastWindowAt = nowNanos;

        long in = inbound.sum();
        long out = outbound.sum();
        double inRate = (in - lastIn) / seconds;
        double outRate = (out - lastOut) / seconds;
        lastIn = in;
        lastOut = out;

        List<ActionRate> byAction = new ArrayList<>();
        actions.forEach((k, adder) -> {
            long total = adder.sum();
            long previous = lastActions.getOrDefault(k, 0L);
            lastActions.put(k, total);
            double rate = (total - previous) / seconds;
            if (rate > 0) {
                int sep = k.indexOf('|');
                byAction.add(new ActionRate(k.substring(sep + 1), k.substring(0, sep), rate));
            }
        });

        List<Double> samples;
        synchronized (latencies) {
            samples = new ArrayList<>(latencies);
            latencies.clear();
        }
        Collections.sort(samples);

        long cutoff = System.currentTimeMillis() - PENDING_EXPIRY_MS;
        awaitingReply.values().removeIf(ts -> ts < cutoff);
        commandsInFlight.values().removeIf(p -> p.ts() < cutoff);

        List<RecentFrame> recentCopy;
        synchronized (recent) {
            recentCopy = List.copyOf(recent);
        }
        return new Window(
                new Traffic(inRate, outRate, byAction),
                new Latency(samples.size(), percentile(samples, 0.50), percentile(samples, 0.95),
                        percentile(samples, 0.99), samples.isEmpty() ? 0 : samples.getLast()),
                new Commands(sent.sum(), accepted.sum(), rejected.sum(), failed.sum(), commandsInFlight.size()),
                recentCopy);
    }

    private static double percentile(List<Double> sorted, double p) {
        if (sorted.isEmpty()) {
            return 0;
        }
        int index = (int) Math.ceil(p * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }
}
