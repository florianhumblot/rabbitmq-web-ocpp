"""Critical flaws C1-C5.

Every test asserts the correct behaviour: it fails while the flaw exists."""

import random
import string
import time

import pytest

import ocpp_client as oc


def test_c1_quorum_queue_receives_every_message(res, worker):
    """C1: the plugin drops the queue client state after every publish. A
    quorum queue then sees every message as the first one of a new publisher
    with sequence number 1 and drops all but the first as duplicates, e.g.
    all Heartbeats after the first one of a connection."""
    cid = res.cid()
    queue = res.queue()
    worker.declare_queue(queue, "ocpp16.Heartbeat.req", {"x-queue-type": "quorum"})
    cp = res.connect(cid)
    for _ in range(5):
        cp.call("Heartbeat")
    received = worker.from_cp(queue, cid, expected=5, timeout=10)
    assert len(received) == 5, (
        f"the charge point sent 5 Heartbeats, the quorum queue received {len(received)}")


def test_c2_deleted_queue_disconnects_charge_point(res, worker):
    """C2: when the charge point's queue goes away (deleted, or its node
    restarts), the connection stays open but the charge point never receives
    another command. Closing the connection makes the charge point reconnect,
    which declares and consumes the queue again."""
    cid = res.cid()
    cp = res.connect(cid)
    oc.delete_queue("ocpp." + cid)
    code = cp.wait_closed(timeout=5)
    if code == "open":
        worker.publish_to_cp(cid, [2, "csms-1", "Reset", {"type": "Soft"}])
        got = cp.recv(timeout=3)
        pytest.fail("the charge point's queue was deleted, but its connection stays open "
                    f"and it does not receive commands anymore (received: {got!r})")


@pytest.mark.broker_shell
def test_c2_crashed_queue_disconnects_charge_point(res, worker, broker):
    """C2: the 'DOWN' of the charge point's queue process (e.g. its node
    restarted) is swallowed. The restarted queue has no consumer: the charge
    point looks online but is deaf."""
    cid = res.cid()
    cp = res.connect(cid)
    broker.eval(f"exit({broker.queue_pid('ocpp.' + cid)}, kill).")
    if cp.wait_closed(timeout=5) == "open":
        worker.publish_to_cp(cid, [2, "csms-1", "Reset", {"type": "Soft"}])
        got = cp.recv(timeout=3)
        assert got is not None, (
            "the charge point's queue process crashed and restarted; the connection "
            "stays open but the command sent to the charge point never arrives")


def junk_keys(n):
    return {"".join(random.choices(string.ascii_lowercase, k=12)): "v" for _ in range(n)}


@pytest.mark.broker_shell
def test_c3_client_properties_are_bounded(res, worker, broker):
    """C3: every key of a BootNotification payload and every StatusNotification
    connectorId is stored as a client property of the connection (and turned
    into an atom, see the next test). The charge point decides how many."""
    cid = res.cid()
    queue = res.queue()
    worker.declare_queue(queue, "ocpp16.*.req")
    cp = res.connect(cid)
    cp.call("BootNotification", {"chargePointVendor": "V", "chargePointModel": "M",
                                 **junk_keys(300)})
    for connector in range(1000, 1100):
        cp.call("StatusNotification", {"connectorId": connector, "status": "Available",
                                       "errorCode": "NoError"})
    worker.from_cp(queue, cid, expected=101, timeout=15)
    count = broker.eval_int(
        f"[{{user_property, P}}] = rabbit_web_ocpp_handler:info({broker.ocpp_connection_pid(cid)}, "
        "[user_property]), length(P).")
    assert count < 50, f"the charge point's connection stores {count} client properties"


@pytest.mark.broker_shell
def test_c3_client_input_does_not_create_atoms(res, worker, broker):
    """C3: client property keys are converted with binary_to_atom/2. Atoms are
    never garbage collected and the atom table is limited (1,048,576 by
    default): one authenticated charge point sending random keys can crash
    the node."""
    cid = res.cid()
    queue = res.queue()
    worker.declare_queue(queue, "ocpp16.BootNotification.req")
    cp = res.connect(cid)
    before = broker.eval_int("erlang:system_info(atom_count).")
    cp.call("BootNotification", {"chargePointVendor": "V", "chargePointModel": "M",
                                 **junk_keys(2000)})
    worker.from_cp(queue, cid, expected=1)
    time.sleep(1)
    created = broker.eval_int("erlang:system_info(atom_count).") - before
    assert created < 100, f"one BootNotification created {created} atoms on the broker"


@pytest.mark.parametrize("client_id", ["#", "*", "ocpp16.Heartbeat.req"])
def test_c4_wildcard_client_ids_are_rejected(res, client_id):
    """C4: the client ID is the binding key of the charge point queue on a
    topic exchange. Wildcards and dots must be rejected."""
    res.users.append(client_id)
    res.queues.append("ocpp." + client_id)
    cp, status = res.try_connect(client_id)
    assert status != 101, f"a charge point could connect as {client_id!r}"


def test_c4_hash_client_id_receives_all_traffic(res, worker):
    """C4: a charge point connecting as '#' binds its queue with '#' and
    receives everything published to the exchange: the requests of all other
    charge points and every command the CSMS sends."""
    res.users.append("#")
    res.queues.append("ocpp.#")
    spy, status = res.try_connect("#")
    if spy is None:
        return  # Rejected: not vulnerable.
    victim = res.cid("victim")
    victim_cp = res.connect(victim)
    worker.publish_to_cp(victim, [2, "csms-secret", "ChangeConfiguration",
                                  {"key": "AuthorizationKey", "value": "s3cr3t"}])
    victim_cp.call("Authorize", {"idTag": "0123456789"})
    stolen = []
    while True:
        frame = spy.recv(timeout=2)
        if frame is None:
            break
        stolen.append(frame)
    assert not stolen, (f"charge point '#' received traffic of charge point {victim}: {stolen}")


def test_c4_client_id_length_is_limited(res):
    """C4: OCPP 2.0.1 limits the identity to 48 characters, the plugin
    accepts any length."""
    cid = "x" * 200
    res.users.append(cid)
    res.queues.append("ocpp." + cid)
    cp, status = res.try_connect(cid)
    assert status != 101, "a charge point could connect with a 200 character client ID"


def test_c5_username_must_be_the_client_id(res):
    """C5: OCPP security profiles 1 and 2 require the Basic auth username to
    be the charge point identity. Any valid credentials connect as any
    charge point."""
    cid = res.cid()
    fleet = res.cid("fleet")
    oc.ensure_user(fleet)
    cp, status = res.try_connect(cid, user=fleet, create_user=False)
    assert status in (401, 403), (
        f"user {fleet!r} could connect as charge point {cid!r} (HTTP {status})")


def test_c5_other_credentials_cannot_take_over_a_session(res, worker):
    """C5: with "last connection wins", credentials of any user (e.g. a leaked
    password of another charge point) disconnect a charge point and receive
    its commands."""
    cid = res.cid()
    victim = res.connect(cid)
    attacker = res.cid("attacker")
    oc.ensure_user(attacker)
    hijacker, status = res.try_connect(cid, user=attacker, create_user=False)
    if hijacker is None:
        return  # Rejected: not vulnerable.
    victim_code = victim.wait_closed(timeout=5)
    oc.wait_until(lambda: oc.cp_queue_bound(cid), timeout=5)
    worker.publish_to_cp(cid, [2, "csms-1", "UpdateFirmware",
                               {"location": "https://example.com/fw.bin",
                                "retrieveDate": "2026-01-01T00:00:00Z"}])
    stolen = hijacker.recv(timeout=3)
    pytest.fail(f"user {attacker!r} took over charge point {cid!r}: the charge point was "
                f"disconnected (close code {victim_code}) and the attacker received its "
                f"command {stolen!r}")
