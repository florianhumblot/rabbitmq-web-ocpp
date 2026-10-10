"""Handling of OCPP-J messages: findings 6, 7, 11 and 14."""

import pytest

import ocpp_client as oc


def test_f6_one_outstanding_call_at_a_time(res, worker):
    """6: OCPP-J does not allow sending a CALL before the previous one was
    answered or timed out. The plugin sends queued commands back to back."""
    cid = res.cid()
    cp = res.connect(cid)
    worker.publish_to_cp(cid, [2, "csms-1", "GetConfiguration", {}])
    worker.publish_to_cp(cid, [2, "csms-2", "GetConfiguration", {}])
    assert cp.recv()[1] == "csms-1"
    early = cp.recv(timeout=1)
    assert early is None, (
        f"the charge point received {early!r} before it answered CALL csms-1")
    cp.result("csms-1")
    assert cp.recv()[1] == "csms-2"


def test_f6_unanswered_call_survives_disconnect(res, worker):
    """6: commands are acknowledged when they are handed to the WebSocket,
    not when the charge point answers. A command whose answer never came,
    because the connection dropped, is lost."""
    cid = res.cid()
    cp = res.connect(cid)
    worker.publish_to_cp(cid, [2, "csms-1", "RemoteStartTransaction",
                               {"idTag": "0123456789", "connectorId": 1}])
    assert cp.recv()[1] == "csms-1"
    cp.close()  # The connection drops before the charge point answers.
    cp2 = res.connect(cid)
    again = cp2.recv(timeout=5)
    assert again is not None and again[1] == "csms-1", (
        "the unanswered RemoteStartTransaction was lost when the connection dropped")


def test_f7_answer_routing_key_carries_action(res, worker):
    """7: answers of the charge point to CSMS commands are routed as
    ocpp16.response.conf: workers cannot bind to the answers of a particular
    command, and cannot tell which command an answer belongs to without
    keeping state."""
    cid = res.cid()
    queue = res.queue()
    worker.declare_queue(queue, "ocpp16.GetConfiguration.conf")
    cp = res.connect(cid)
    worker.publish_to_cp(cid, [2, "csms-7", "GetConfiguration", {}])
    assert cp.recv()[1] == "csms-7"
    cp.result("csms-7", {"configurationKey": []})
    got = worker.from_cp(queue, cid, expected=1, timeout=5)
    assert got, "the answer to GetConfiguration was not routed as ocpp16.GetConfiguration.conf"


def test_f11_unroutable_call_gets_callerror(res):
    """11: a CALL no worker is bound for is dropped (with a log line); the
    charge point waits for its timeout, then retries or reboots. It should
    get a CALLERROR."""
    cid = res.cid()
    cp = res.connect(cid)
    msg_id = cp.call("UnboundAction" + cid.replace("-", ""))
    got = cp.recv(timeout=5)
    assert got is not None and got[0] == 4 and got[1] == msg_id, (
        "the unroutable CALL got no answer")


@pytest.mark.parametrize("frame", [
    [2, {"a": 1}, "Heartbeat", {}],
    [2, [1, 2], "Heartbeat", {}],
    [2, "1" * 37, "Heartbeat", {}],
], ids=["map-id", "list-id", "37-char-id"])
def test_f14_invalid_message_id_is_a_protocol_error(res, frame):
    """14: message IDs that are not strings, or longer than the 36 characters
    OCPP-J allows, are accepted and published. They should close the
    connection with 1002 (protocol error)."""
    cid = res.cid()
    cp = res.connect(cid)
    cp.send(frame)
    code = cp.wait_closed(timeout=5)
    assert code == 1002, f"got close code {code!r} for {frame!r}"


def test_f14_non_string_action_is_a_protocol_error(res, worker):
    """14: a CALL whose action is not a string is published as
    ocpp16.response.req instead of being rejected."""
    cid = res.cid()
    queue = res.queue()
    worker.declare_queue(queue, "ocpp16.response.req")
    cp = res.connect(cid)
    cp.send([2, "1", 123, {}])
    code = cp.wait_closed(timeout=3)
    published = worker.from_cp(queue, cid, expected=1, timeout=1)
    assert code == 1002 and not published, (
        f"close code {code!r}; published as ocpp16.response.req: {bool(published)}")


def test_f14_non_string_message_id_does_not_break_workers(res, worker):
    """14: a message ID that is not a string is published as correlation_id.
    Such a message cannot be encoded for AMQP 0-9-1: every worker that reads
    it loses its connection (INTERNAL_ERROR), and the message stays in the
    queue for the next worker: a poison message one charge point can inject."""
    cid = res.cid()
    queue = res.queue()
    worker.declare_queue(queue, "ocpp16.Heartbeat.req")
    cp = res.connect(cid)
    cp.send([2, {"a": 1}, "Heartbeat", {}])
    cp.call("Heartbeat")
    try:
        worker.from_cp(queue, cid, expected=2, timeout=3)
    except Exception as e:
        pytest.fail(f"reading the charge point's messages from {queue} failed: {e!r}")
