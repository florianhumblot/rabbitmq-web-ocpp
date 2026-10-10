"""High availability (finding 9). Needs the 3 node cluster of
docker-compose.cluster.yml, see the README."""

import os
import shlex
import subprocess
import time

import pytest

import ocpp_client as oc

pytestmark = pytest.mark.skipif(os.environ.get("OCPP_CLUSTER") != "1",
                                reason="needs the 3 node cluster (docker-compose.cluster.yml)")

COMPOSE = f"docker compose -f {oc.HERE}/docker-compose.cluster.yml"
NODE2_URL = os.environ.get("OCPP_NODE2_URL", "ws://localhost:19522/ocpp")
NODE3_URL = os.environ.get("OCPP_NODE3_URL", "ws://localhost:19523/ocpp")


@pytest.fixture(scope="module", autouse=True)
def cluster_up():
    def running():
        nodes = oc.mgmt("GET", "/nodes").json()
        return len([n for n in nodes if n.get("running")]) == 3
    assert oc.wait_until(running, timeout=180, interval=2), "the cluster did not form"
    for url in (NODE2_URL, NODE3_URL):
        assert oc.wait_until(lambda: oc.ocpp_listening(url), timeout=120, interval=2), \
            f"{url} does not listen"


def compose(*args):
    subprocess.run(shlex.split(COMPOSE) + list(args), check=True, capture_output=True,
                   timeout=300)


def test_f9_charge_point_survives_the_loss_of_a_node(res, worker):
    """9: a charge point queue is a classic queue on the node the charge point
    first connected to, and cannot be made replicated (the cluster asks for
    quorum queues in advanced.config). While that node is down, the charge
    point cannot connect to any other node: its connection fails because
    its queue is unavailable."""
    cid = res.cid()
    cp = res.connect(cid, url=NODE2_URL)
    compose("stop", "rabbitmq2")
    try:
        assert cp.wait_closed(timeout=30) != "open"
        # The charge point reconnects to another node.
        deadline = time.time() + 60
        reconnected = None
        while time.time() < deadline and reconnected is None:
            new, status = oc.try_connect(cid, create_user=False, url=NODE3_URL)
            if new is not None and new.wait_closed(timeout=2) == "open":
                reconnected = new
                res.cps.append(new)
            else:
                time.sleep(2)
        assert reconnected, "the charge point could not connect to another node for 60 s"
        worker.publish_to_cp(cid, [2, "csms-1", "Reset", {"type": "Soft"}])
        assert reconnected.recv(timeout=10) is not None, \
            "the charge point did not receive commands on the other node"
    finally:
        compose("start", "rabbitmq2")
