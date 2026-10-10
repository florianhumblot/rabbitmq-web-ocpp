import pytest

import ocpp_client as oc


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: waits for a broker timeout (about a minute)")
    config.addinivalue_line("markers", "broker_shell: needs rabbitmqctl in the broker container")


@pytest.fixture(scope="session", autouse=True)
def broker_up():
    oc.wait_for_broker()


@pytest.fixture
def worker():
    return oc.Worker()


@pytest.fixture(scope="session")
def _broker_shell():
    return oc.broker_available(oc.BROKER_EXEC)


@pytest.fixture
def broker(_broker_shell):
    """rabbitmqctl in the broker container, for flaws that can only be
    provoked or observed from inside the broker."""
    if not _broker_shell:
        pytest.skip("needs the broker shell (BROKER_EXEC, see README)")
    return oc.Broker(oc.BROKER_EXEC)


class Resources:
    """Tracks what a test creates, so it can be cleaned up."""

    def __init__(self):
        self.cps = []
        self.queues = []
        self.users = []

    def cid(self, prefix="cp"):
        cid = oc.unique_id(prefix)
        self.users.append(cid)
        self.queues.append("ocpp." + cid)
        return cid

    def queue(self, prefix="worker"):
        name = oc.unique_id(prefix)
        self.queues.append(name)
        return name

    def connect(self, client_id, **kwargs):
        cp = oc.connect(client_id, **kwargs)
        self.cps.append(cp)
        return cp

    def try_connect(self, client_id, **kwargs):
        cp, status = oc.try_connect(client_id, **kwargs)
        if cp:
            self.cps.append(cp)
        return cp, status


@pytest.fixture
def res():
    r = Resources()
    yield r
    for cp in r.cps:
        cp.close()
    for name in r.queues:
        try:
            oc.delete_queue(name)
        except Exception:
            pass
    for user in r.users:
        try:
            oc.delete_user(user)
        except Exception:
            pass
