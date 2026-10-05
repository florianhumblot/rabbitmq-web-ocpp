package ocpp;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.*;

import io.gatling.http.action.ws.WsInboundMessage;
import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Session;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A fleet of OCPP 1.6 and 2.1 charge points on Security Profiles 1, 2 and 3 connecting to
 * rabbitmq-web-ocpp, plus an operator driving the CSMS command API.
 *
 * <p>Every CALL a charger sends is a Gatling request named {@code "<version> <Action>"}, timed
 * from the frame leaving the charger to the CSMS answer arriving through the broker. The
 * operator's commands are named {@code "API <Action>"} and include the charger round trip.
 */
public class ChargePointSimulation extends Simulation {

    private static final String WS_NAME = "ocpp";
    private static final String MESSAGE_TYPE = "^\\[\\s*(\\d)";

    private final Config cfg = new Config();
    private final Fleet fleet;
    private final long endAt;

    {
        try {
            fleet = new Fleet(cfg.dataDir, cfg.chargers);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read the fleet, run the provision service first", e);
        }
        System.out.println("OCPP fleet: " + fleet.size() + " chargers, " + cfg);
        endAt = System.currentTimeMillis() + (cfg.rampSeconds + cfg.durationSeconds) * 1000L;
    }

    private boolean running() {
        return System.currentTimeMillis() < endAt;
    }

    private static ChargerModel model(Session s) {
        return (ChargerModel) s.get("model");
    }

    // ---- Charge points ------------------------------------------------------------------------

    private final HttpProtocolBuilder chargerProtocol = http
            .disableWarmUp()
            // Frames received while no CALL is awaiting an answer are kept for the next tick.
            .wsUnmatchedInboundMessageBufferSize(64)
            // Security Profile 3: each virtual user presents its own charger's certificate.
            .perUserKeyManagerFactory(fleet::keyManagers);

    /** Sends the next CALL of the outbox and waits for the CALLRESULT with the same message id. */
    private final ChainBuilder sendNextCall = exec(s -> {
        ChargerModel.Call call = model(s).poll();
        return s.set("mid", UUID.randomUUID().toString())
                .set("action", call.action())
                .set("payload", call.payload());
    })
            // Gatling logs the send and the check separately: the check, named after the
            // version and action, carries the round trip time through broker and CSMS.
            .exec(ws("send #{action}", WS_NAME)
                    .sendText("[2,\"#{mid}\",\"#{action}\",#{payload}]")
                    .await(Duration.ofSeconds(cfg.callTimeoutSeconds)).on(
                            ws.checkTextMessage("#{version} #{action}")
                                    // OCPP frames are root-level arrays: [3,"<message id>",{...}]
                                    .matching(substring("\"#{mid}\""))
                                    .check(regex(MESSAGE_TYPE).is("3"), bodyString().saveAs("reply"))))
            .exec(s -> {
                model(s).onReply(s.getString("action"), s.getString("reply"), System.currentTimeMillis());
                return s;
            });

    private final ChainBuilder drainOutbox = asLongAs(s -> model(s).hasOutbox()).on(sendNextCall);

    private final ChainBuilder connect = doIfOrElse(s -> s.getInt("sp") == 3)
            .then(exec(ws("#{version} connect SP#{sp}", WS_NAME).connect("#{url}")
                    .subprotocol("#{subprotocol}")
                    .autoReplyTextFrame(ChargerModel::autoReply)))
            .orElse(exec(ws("#{version} connect SP#{sp}", WS_NAME).connect("#{url}")
                    .subprotocol("#{subprotocol}")
                    .header("Authorization", "#{authorization}")
                    .autoReplyTextFrame(ChargerModel::autoReply)));

    /** One second of a connected charger: apply buffered CSMS commands, then send what is due. */
    private final ChainBuilder tick = pause(Duration.ofSeconds(1))
            .exec(ws.processUnmatchedMessages(WS_NAME, (messages, s) -> {
                List<String> frames = new ArrayList<>(messages.size());
                for (WsInboundMessage m : messages) {
                    if (m instanceof WsInboundMessage.Text text) {
                        frames.add(text.message());
                    }
                }
                ChargerModel model = model(s);
                long now = System.currentTimeMillis();
                model.onCsmsFrames(frames, now);
                model.tick(now);
                return s;
            }))
            .exec(drainOutbox);

    private final ScenarioBuilder chargePoints = scenario("Charge points")
            .exec(s -> {
                Fleet.Charger c = fleet.forUser(s.userId());
                return s.set("id", c.id())
                        .set("version", c.ocppVersion())
                        .set("subprotocol", c.subprotocol())
                        .set("sp", c.securityProfile())
                        .set("url", c.url(cfg))
                        .set("authorization", c.authorization())
                        .set("model", new ChargerModel(c, cfg));
            })
            // Power cycles: connect, boot, run until a Reset or the end of the test.
            .asLongAs(s -> running()).on(
                    exitBlockOnFail().on(
                            exec(connect),
                            exec(s -> {
                                model(s).onConnected(System.currentTimeMillis());
                                return s;
                            }),
                            exec(drainOutbox),
                            asLongAs(s -> running() && !model(s).rebootDue()).on(tick)),
                    // Leaving the block on a failure (rejected connect, closed socket, CALL
                    // timeout) behaves like real firmware: drop the connection and retry.
                    doIf(s -> s.contains(WS_NAME)).then(exec(ws("close", WS_NAME).close())),
                    exec(Session::markAsSucceeded),
                    doIf(s -> running()).then(pause(Duration.ofSeconds(5), Duration.ofSeconds(15))));

    // ---- Operator -------------------------------------------------------------------------------

    private final HttpProtocolBuilder apiProtocol = http
            .baseUrl(cfg.csmsApi)
            .disableWarmUp()
            .contentTypeHeader("application/json")
            .acceptHeader("application/json");

    private String randomCharger() {
        return fleet.get(ThreadLocalRandom.current().nextInt(fleet.size())).id();
    }

    /** One command to a random charger; 409 (charger offline, e.g. rebooting) is a valid answer. */
    private final ScenarioBuilder operator = scenario("Operator")
            .exec(s -> {
                ThreadLocalRandom r = ThreadLocalRandom.current();
                double dice = r.nextDouble();
                String command;
                String body;
                if (dice < 0.6) {
                    String[] messages = {"StatusNotification", "StatusNotification", "Heartbeat", "MeterValues"};
                    command = "trigger-message";
                    body = "{\"requestedMessage\":\"" + messages[r.nextInt(messages.length)]
                            + "\",\"connectorId\":" + r.nextInt(cfg.connectors + 1) + "}";
                } else if (dice < 0.9) {
                    command = "change-availability";
                    body = "{\"type\":\"" + (r.nextBoolean() ? "Inoperative" : "Operative")
                            + "\",\"connectorId\":" + r.nextInt(cfg.connectors + 1) + "}";
                } else {
                    command = "reset";
                    body = "{\"type\":\"" + (r.nextDouble() < 0.7 ? "Soft" : "Hard") + "\"}";
                }
                return s.set("charger", randomCharger()).set("command", command).set("body", body);
            })
            .exec(http("API #{command}")
                    .post("/api/chargers/#{charger}/#{command}")
                    .body(StringBody("#{body}"))
                    .check(status().in(200, 409)));

    {
        int ramp = cfg.rampSeconds;
        setUp(
                chargePoints.injectOpen(rampUsers(fleet.size()).during(ramp)).protocols(chargerProtocol),
                // Starts after the ramp, so charge points own user ids 1..N (see Fleet#forUser).
                operator.injectOpen(
                        nothingFor(Duration.ofSeconds(ramp + 15)),
                        constantUsersPerSec(cfg.commandsPerSecond).during(Duration.ofSeconds(Math.max(1, cfg.durationSeconds - 30))))
                        .protocols(apiProtocol))
                .maxDuration(Duration.ofSeconds(ramp + cfg.durationSeconds + 120L));
    }
}
