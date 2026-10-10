"""Back pressure: finding 10."""

import time

import pytest

import ocpp_client as oc

pytestmark = pytest.mark.broker_shell


def test_f10_memory_alarm_stops_charge_points_publishing(res, worker, broker):
    """10: the connections do not register for resource alarms: charge points
    keep publishing while the broker is out of memory or disk (e.g. during a
    reconnection storm), when all other protocols are blocked."""
    cid = res.cid()
    queue = res.queue()
    worker.declare_queue(queue, "ocpp16.Heartbeat.req")
    cp = res.connect(cid)
    broker.set_memory_alarm()
    try:
        time.sleep(1)
        cp.call("Heartbeat")
        during_alarm = worker.from_cp(queue, cid, expected=1, timeout=2)
    finally:
        broker.clear_memory_alarm()
    assert not during_alarm, "the charge point published while the memory alarm was set"
    assert worker.from_cp(queue, cid, expected=1, timeout=10), \
        "the Heartbeat was not published after the alarm cleared"


def test_f10_slow_queue_throttles_charge_point(res, worker, broker):
    """10: publishes do not use credit flow: a charge point can fill the
    mailbox of a queue that cannot keep up without bound."""
    cid = res.cid()
    queue = res.queue()
    worker.declare_queue(queue, "ocpp16.DataTransfer.req")
    cp = res.connect(cid)
    qpid = broker.queue_pid(queue)
    broker.eval(f"sys:suspend({qpid}).")
    try:
        data = "x" * 1000
        for _ in range(2000):
            cp.call("DataTransfer", {"vendorId": "v", "data": data})
        time.sleep(3)
        mailbox = broker.eval_int(
            f"{{message_queue_len, N}} = erlang:process_info({qpid}, message_queue_len), N.")
    finally:
        broker.eval(f"sys:resume({qpid}).")
    assert mailbox < 1000, (
        f"the stalled queue's mailbox holds {mailbox} of the 2000 messages the charge point sent")
