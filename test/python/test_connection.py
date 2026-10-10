"""Connections: findings 12, 13, 16 and 17."""

import shlex
import subprocess
import time

import paho.mqtt.publish
import pytest

import ocpp_client as oc


@pytest.mark.slow
def test_f12_quiet_charge_point_stays_connected(res):
    """12: the server never pings and the idle timeout is 60 s: a charge point
    that does not ping itself and sends Heartbeats less often than every
    minute is disconnected every minute (and announced offline each time).
    The client answers pings, as RFC 6455 requires."""
    cid = res.cid()
    cp = res.connect(cid)
    started = time.time()
    code = cp.wait_closed(timeout=75)
    assert code == "open", (
        f"the connection was closed after {time.time() - started:.0f} s of silence "
        f"(close code {code})")


def test_f13_oversized_frame_is_rejected(res):
    """13: there is no frame size limit by default: a charge point can make the
    broker buffer frames of any size (here 4 MiB)."""
    cid = res.cid()
    cp = res.connect(cid)
    cp.call("DataTransfer", {"vendorId": "v", "data": "a" * (4 * 1024 * 1024)})
    code = cp.wait_closed(timeout=10)
    # 1009 (message too big), or None if the close frame got lost while the
    # client was still sending.
    assert code in (1009, None), f"the 4 MiB frame was accepted (connection {code!r})"


@pytest.mark.broker_shell
def test_f13_invalid_json_is_not_logged_in_full(res, broker):
    """13: invalid JSON is logged in full at error level: one charge point can
    flood the broker's logs."""
    def log_size():
        return len(subprocess.run(shlex.split(oc.BROKER_LOGS), capture_output=True,
                                  timeout=120).stdout)
    cid = res.cid()
    cp = res.connect(cid)
    before = log_size()
    cp.send("[2, \"1\", \"Heartbeat\", " + "x" * 200_000)
    cp.wait_closed(timeout=60)
    # Wait until the logs stop growing.
    sizes = [log_size()]
    while True:
        time.sleep(2)
        sizes.append(log_size())
        if sizes[-1] == sizes[-2]:
            break
    logged = sizes[-1] - before
    assert logged < 20_000, f"200 kB of invalid JSON produced {logged} bytes of logs"


def test_f16_mqtt_client_cannot_reach_charge_points(res):
    """16: charge point queues are bound to amq.topic, which MQTT (and STOMP)
    clients publish to: an MQTT client publishing to topic <client ID> sends
    frames to that charge point."""
    cid = res.cid()
    cp = res.connect(cid)
    mqtt_user = res.cid("mqtt")
    oc.ensure_user(mqtt_user)
    try:
        paho.mqtt.publish.single(cid, '[2,"mqtt-1","Reset",{"type":"Hard"}]',
                                 hostname=oc.AMQP_HOST, port=oc.MQTT_PORT,
                                 auth={"username": mqtt_user, "password": oc.PASSWORD})
    except (ConnectionRefusedError, OSError) as e:
        pytest.skip(f"MQTT not reachable: {e}")
    got = cp.recv(timeout=3)
    assert got is None, f"an MQTT client sent the charge point {got!r}"


def test_f16_amq_topic_does_not_reach_charge_points(res, worker):
    """16: the same as above, with any client publishing to amq.topic."""
    cid = res.cid()
    cp = res.connect(cid)
    worker.publish_to_cp(cid, [2, "x-1", "Reset", {"type": "Hard"}], exchange="amq.topic")
    got = cp.recv(timeout=3)
    assert got is None, f"a message published to amq.topic reached the charge point: {got!r}"


@pytest.mark.slow
def test_f17_revoked_permissions_take_effect(res, worker):
    """17: permission checks are cached for the lifetime of the connection:
    revoking a charge point's permissions has no effect while it stays
    connected. (Waits 65 s: a cache may hold results for up to a minute.)"""
    cid = res.cid()
    queue = res.queue()
    worker.declare_queue(queue, "ocpp16.Heartbeat.req")
    cp = res.connect(cid)
    cp.call("Heartbeat")
    assert worker.from_cp(queue, cid, expected=1)
    oc.set_permissions(cid, ".*", "^$", ".*")  # No write permission anymore.
    time.sleep(65)
    cp.call("Heartbeat")
    code = cp.wait_closed(timeout=5)
    published = worker.from_cp(queue, cid, expected=1, timeout=2)
    assert not published, (
        f"65 s after its write permission was revoked, the charge point still publishes "
        f"(connection {code})")


@pytest.mark.parametrize("protocol", ["ocpp1.2", "ocpp1.5"])
def test_f17_soap_only_versions_are_rejected(res, protocol):
    """17: OCPP 1.2 and 1.5 only exist as SOAP, there is no OCPP-J flavour of
    them, but the plugin accepts these subprotocols."""
    cid = res.cid()
    cp, status = res.try_connect(cid, protocols=(protocol,))
    assert status == 400, f"subprotocol {protocol} accepted (HTTP {status})"


@pytest.mark.broker_shell
def test_f17_disabling_the_plugin_stops_its_listener(res, broker):
    """17: the plugin stops its listeners with a function that RabbitMQ 4.2
    renamed: stopping or disabling the plugin crashes and the listener keeps
    accepting charge points (with the old configuration if it is enabled
    again)."""
    cid = res.cid()
    oc.ensure_user(cid)
    broker.run("rabbitmq-plugins", "disable", "rabbitmq_web_ocpp")
    try:
        time.sleep(2)
        cp, status = res.try_connect(cid, create_user=False)
    finally:
        if cp:
            cp.close()
        broker.run("rabbitmq-plugins", "enable", "rabbitmq_web_ocpp")
        oc.wait_for_broker()
    assert status != 101, "a charge point connected after the plugin was disabled"
