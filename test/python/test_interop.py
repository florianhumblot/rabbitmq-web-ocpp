"""Interoperability with the rest of RabbitMQ: AMQP 1.0, streams, connection
limits, CLI tools and metrics."""

import re
import socket
import threading
import time

import pika
import pytest
import requests
from proton.utils import BlockingConnection
from proton import Timeout

import ocpp_client as oc


def test_amqp10_worker_receives_charge_point_messages(res, worker):
    """AMQP 1.0 is RabbitMQ's main protocol since 4.0. Messages of charge
    points are converted to AMQP 1.0 with untagged property values, which
    cannot be encoded: AMQP 1.0 consumers never receive them."""
    cid = res.cid()
    queue = res.queue()
    worker.declare_queue(queue, "ocpp16.Heartbeat.req")
    cp = res.connect(cid)
    msg_id = cp.call("Heartbeat")
    assert oc.wait_until(lambda: worker.message_count(queue) == 1, timeout=5)
    conn = BlockingConnection(f"amqp://{oc.AMQP_HOST}:{oc.AMQP_PORT}", user=oc.ADMIN_USER,
                              password=oc.ADMIN_PASS, allowed_mechs="PLAIN")
    try:
        receiver = conn.create_receiver(f"/queues/{queue}")
        try:
            msg = receiver.receive(timeout=5)
        except Timeout:
            pytest.fail("the AMQP 1.0 consumer did not receive the charge point's message")
        receiver.accept()
    finally:
        conn.close()
    assert msg.correlation_id == msg_id and msg.reply_to == cid


def test_stream_bound_to_the_exchange_does_not_break_charge_points(res):
    """Binding a stream (e.g. for auditing) stores the charge points' messages
    in AMQP 1.0 format. Its untagged values crash the connection of every
    charge point that publishes to the stream."""
    cid = res.cid()
    stream = res.queue("audit")
    params = pika.ConnectionParameters(host=oc.AMQP_HOST, port=oc.AMQP_PORT,
                                       credentials=pika.PlainCredentials(oc.ADMIN_USER,
                                                                         oc.ADMIN_PASS))
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(stream, durable=True, arguments={"x-queue-type": "stream"})
    ch.queue_bind(stream, oc.EXCHANGE, routing_key="ocpp16.Heartbeat.req")
    cp = res.connect(cid)
    msg_id = cp.call("Heartbeat")
    code = cp.wait_closed(timeout=2)
    ch.basic_qos(prefetch_count=10)
    stored = []
    for method, props, body in ch.consume(stream, arguments={"x-stream-offset": "first"},
                                          inactivity_timeout=5):
        if method is None:
            break
        stored.append(props.correlation_id)
        ch.basic_ack(method.delivery_tag)
    conn.close()
    assert code == "open", f"the charge point's connection was closed (code {code})"
    assert msg_id in stored, "the Heartbeat was not stored in the stream"


def test_vhost_connection_limit_applies(res):
    """The connection limit of a vhost (rabbitmqctl set_vhost_limits,
    max-connections) does not apply to OCPP connections."""
    vhost = oc.unique_id("vhost")
    oc.mgmt("PUT", f"/vhosts/{oc.q(vhost)}")
    try:
        oc.mgmt("PUT", f"/vhost-limits/{oc.q(vhost)}/max-connections", json={"value": 1})
        first = res.cid()
        res.connect(first, vhost=vhost)
        second = res.cid()
        cp, status = res.try_connect(second, vhost=vhost)
        assert status != 101, "a second connection was accepted in a vhost limited to one"
    finally:
        for cp in res.cps:
            cp.close()
        oc.mgmt("DELETE", f"/vhosts/{oc.q(vhost)}")


def test_user_connection_limit_applies(res):
    """The connection limit of a user (rabbitmqctl set_user_limits,
    max-connections) does not apply to OCPP connections."""
    cid = res.cid()
    oc.ensure_user(cid)
    oc.mgmt("PUT", f"/user-limits/{oc.q(cid)}/max-connections", json={"value": 0})
    cp, status = res.try_connect(cid, create_user=False)
    assert status != 101, "the connection was accepted although the user's limit is 0"


@pytest.mark.broker_shell
def test_cli_lists_connections_while_http_clients_are_connected(res, broker):
    """`rabbitmqctl list_web_ocpp_connections` asks every connection of the
    listener for its details, including plain HTTP connections that are not
    (yet) OCPP sessions, e.g. clients still sending their request, as slow
    clients on a mobile network do. Those never answer: each stalls the
    command for 5 s, after which it prints an error (with exit code 0)
    instead of, or in addition to, the charge points."""
    cid = res.cid()
    host, port = oc.ocpp_host_port()
    res.connect(cid)
    slow = [socket.create_connection((host, port)) for _ in range(3)]
    for sock in slow:
        sock.sendall(b"GET /ocpp/%2F/slow-client HTTP/1.1\r\n")
    stop = threading.Event()

    def trickle_headers():
        while not stop.is_set():
            for sock in slow:
                sock.sendall(b"X-Slow: a\r\n")
            time.sleep(1)
    threading.Thread(target=trickle_headers, daemon=True).start()
    try:
        time.sleep(1)
        started = time.time()
        proc = broker.run("rabbitmqctl", "list_web_ocpp_connections", "client_id",
                          timeout=120, check=False)
        took = time.time() - started
    finally:
        stop.set()
        for sock in slow:
            sock.close()
    output = proc.stdout + proc.stderr
    assert proc.returncode == 0 and cid in output and "badrpc" not in output, (
        f"list_web_ocpp_connections (exit code {proc.returncode}, {took:.1f} s) printed: "
        f"{output[-300:]!r}")


def prometheus(metric, **labels):
    text = requests.get(oc.PROMETHEUS_URL + "/metrics", timeout=10).text
    selector = ",".join(f'{k}="{v}"' for k, v in labels.items())
    m = re.search(rf"^{metric}{{{re.escape(selector)}}} (\S+)$", text, re.M)
    assert m, f"metric {metric}{{{selector}}} not found"
    return float(m.group(1))


def test_metrics_count_ocpp_connections_and_messages(res, worker):
    """The protocol metrics (Prometheus, global counters) never decrement the
    consumers gauge, and never count received messages: the gauge grows with
    every reconnect."""
    queue = res.queue()
    worker.declare_queue(queue, "ocpp16.Heartbeat.req")
    consumers = prometheus("rabbitmq_global_consumers", protocol="ocpp16")
    received = prometheus("rabbitmq_global_messages_received_total", protocol="ocpp16")
    for _ in range(3):
        cid = res.cid()
        cp = res.connect(cid)
        cp.call("Heartbeat")
        worker.from_cp(queue, cid, expected=1)
        cp.close()
    time.sleep(2)
    consumers_after = prometheus("rabbitmq_global_consumers", protocol="ocpp16")
    received_after = prometheus("rabbitmq_global_messages_received_total", protocol="ocpp16")
    assert (consumers_after, received_after - received) == (consumers, 3), (
        f"after 3 charge points connected, sent a Heartbeat and disconnected: consumers "
        f"{consumers:.0f} -> {consumers_after:.0f}, messages received +{received_after - received:.0f}")
