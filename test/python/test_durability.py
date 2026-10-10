"""Durability, expiry and offline notifications: findings 8 and 15."""

import shlex
import subprocess
import time

import pytest

import ocpp_client as oc


def test_f8_charge_point_messages_are_persistent(res, worker):
    """8: messages of charge points are published as transient (delivery
    mode 1): a broker restart loses them even in durable queues."""
    cid = res.cid()
    queue = res.queue()
    worker.declare_queue(queue, "ocpp16.Heartbeat.req")
    cp = res.connect(cid)
    cp.call("Heartbeat")
    [(_, props, _)] = worker.from_cp(queue, cid, expected=1)
    assert props.delivery_mode == 2, f"delivery mode is {props.delivery_mode}"


def test_f8_charge_point_queue_expires_commands_and_itself(res):
    """8: charge point queues have no message TTL and no expiry: a charge point
    that is offline for days runs days old RemoteStartTransaction and Reset
    commands when it comes back, and the queues of decommissioned charge
    points stay forever."""
    cid = res.cid()
    res.connect(cid)
    args = oc.get_queue("ocpp." + cid)["arguments"]
    assert "x-message-ttl" in args and "x-expires" in args, f"queue arguments: {args}"


def test_f15_reconnect_does_not_announce_offline(res, worker):
    """15: when a charge point reconnects, its old connection is kicked and
    publishes the synthetic offline StatusNotification, although the charge
    point is online. If the kick takes long, it even arrives after the new
    connection's own status."""
    cid = res.cid()
    queue = res.queue()
    worker.declare_queue(queue, "ocpp16.StatusNotification.req")
    old = res.connect(cid)
    new = res.connect(cid)
    old.wait_closed(timeout=5)
    time.sleep(1)
    offline = worker.from_cp(queue, cid, expected=1, timeout=1)
    assert not offline, (
        "the charge point reconnected and is online, but backends were told it is offline: "
        f"{offline[0][2]}")


@pytest.mark.broker_shell
def test_f15_stuck_old_connection_does_not_block_reconnect(res, worker, broker):
    """15: a reconnecting charge point waits 3 s for its old connection to go
    away (e.g. one stuck writing to a dead TCP connection), then tries to
    consume its queue anyway. The old connection still holds the exclusive
    consumer: the new connection fails, and the charge point cannot connect
    until the old one dies."""
    cid = res.cid()
    res.connect(cid)
    broker.eval(f"sys:suspend({broker.ocpp_connection_pid(cid)}).")
    new, status = res.try_connect(cid)
    assert new is not None, f"reconnect rejected with HTTP {status}"
    code = new.wait_closed(timeout=6)
    assert code == "open", f"the new connection was closed (code {code})"
    worker.publish_to_cp(cid, [2, "csms-1", "Reset", {"type": "Soft"}])
    assert new.recv(timeout=5) is not None, "the new connection does not receive commands"


@pytest.mark.broker_shell
def test_f15_newer_connection_survives_join_of_older(res, broker):
    """15: after a network partition heals, two connections with the same
    client ID (one on each side) see each other's join of the client ID
    group. Each one assumes the other is newer and closes: the charge point
    loses both. Simulated by telling a connection about another member."""
    cid = res.cid()
    cp = res.connect(cid)
    broker.eval(
        f"P = {broker.ocpp_connection_pid(cid)}, "
        "Older = spawn(fun() -> receive _ -> ok after 60000 -> ok end end), "
        f"P ! {{make_ref(), join, {{<<\"/\">>, <<\"{cid}\">>}}, [Older]}}, ok.")
    code = cp.wait_closed(timeout=3)
    assert code == "open", (
        "the connection closed on seeing another connection's join, without knowing "
        "which of the two is newer")


@pytest.mark.broker_shell
def test_f8_offline_status_survives_broker_restart(res, worker, broker):
    """8, 15: when the broker stops (e.g. docker stop), connections publish the
    offline StatusNotification while the vhost is already stopping, and as a
    transient message: it does not survive the restart."""
    cid = res.cid()
    queue = res.queue()
    worker.declare_queue(queue, "ocpp16.StatusNotification.req")
    res.connect(cid)
    subprocess.run(shlex.split(oc.BROKER_RESTART), check=True, capture_output=True, timeout=300)
    oc.wait_for_broker()
    offline = worker.from_cp(queue, cid, expected=1, timeout=10)
    assert offline, "the offline StatusNotification of the charge point was lost in the restart"
