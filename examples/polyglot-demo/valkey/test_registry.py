"""Tests for registry.lua against a live Valkey (or Redis).

    docker run --rm -d -p 6379:6379 valkey/valkey:8-alpine
    pip install redis && python valkey/test_registry.py
"""
import os
import unittest

import redis

SCRIPT = open(os.path.join(os.path.dirname(__file__), "registry.lua")).read()
P = "csms:{test}:"


class RegistryTest(unittest.TestCase):
    def setUp(self):
        self.r = redis.Redis(host=os.environ.get("VALKEY_HOST", "localhost"), decode_responses=True)
        for key in self.r.scan_iter(P + "*"):
            self.r.delete(key)
        self.script = self.r.register_script(SCRIPT)

    def call(self, op, cid, *args, now=1):
        return self.script(keys=[P + "c:" + cid], args=[P, op, cid, now, *args])

    def event(self, cid, version="ocpp16", sp="1", connector="", status="", ts=""):
        return self.call("event", cid, version, sp, connector, status, ts)

    def counts(self):
        return {k: int(v) for k, v in self.r.hgetall(P + "status").items() if int(v)}

    def test_first_event_needs_presence_check(self):
        self.assertEqual(self.event("a", connector="1", status="Available"), "check")
        self.assertEqual(self.r.scard(P + "online"), 0)
        self.assertEqual(self.call("presence", "a", "1"), "ok")
        self.assertEqual(self.r.smembers(P + "online"), {"a"})
        self.assertEqual(self.r.smembers(P + "online:v:ocpp16"), {"a"})
        self.assertEqual(self.r.smembers(P + "online:sp:1"), {"a"})
        self.assertEqual(self.counts(), {"Available": 1})
        # Online now: further events need no check.
        self.assertEqual(self.event("a", connector="1", status="Charging"), "ok")
        self.assertEqual(self.counts(), {"Charging": 1})

    def test_offline_and_stale_events(self):
        self.event("a", connector="1", status="Available")
        self.call("presence", "a", "1")
        self.assertEqual(self.call("offline", "a"), "check")
        # Broker says a newer connection exists: the offline frame is stale.
        self.call("presence", "a", "1")
        self.assertEqual(self.r.scard(P + "online"), 1)
        # Broker says gone: offline.
        self.call("presence", "a", "0")
        self.assertEqual(self.r.scard(P + "online"), 0)
        self.assertEqual(self.counts(), {})
        self.assertEqual(self.r.scard(P + "known"), 1)
        # Late heartbeat from the dead connection: check, broker says gone, stays offline.
        self.assertEqual(self.event("a"), "check")
        self.call("presence", "a", "0")
        self.assertEqual(self.r.scard(P + "online"), 0)
        # Offline while already offline needs nothing.
        self.assertEqual(self.call("offline", "a"), "ok")

    def test_status_ordering_by_charger_timestamp(self):
        self.event("a", connector="1", status="Charging", ts="2026-01-01T00:00:02Z")
        self.call("presence", "a", "1")
        self.event("a", connector="1", status="Available", ts="2026-01-01T00:00:01Z")
        self.assertEqual(self.counts(), {"Charging": 1})
        self.event("a", connector="1", status="Available", ts="2026-01-01T00:00:03Z")
        self.assertEqual(self.counts(), {"Available": 1})

    def test_statuses_while_offline_are_counted_on_reconnect(self):
        self.event("a", connector="1", status="Available")
        self.event("a", connector="2", status="Faulted")
        self.assertEqual(self.counts(), {})
        self.call("presence", "a", "1")
        self.assertEqual(self.counts(), {"Available": 1, "Faulted": 1})

    def test_protocol_change_while_online(self):
        self.event("a", version="ocpp16")
        self.call("presence", "a", "1")
        self.event("a", version="ocpp21")
        self.assertEqual(self.r.scard(P + "online:v:ocpp16"), 0)
        self.assertEqual(self.r.smembers(P + "online:v:ocpp21"), {"a"})

    def test_presence_is_idempotent(self):
        self.event("a", connector="1", status="Available")
        for _ in range(3):
            self.call("presence", "a", "1")
        self.assertEqual(self.counts(), {"Available": 1})
        for _ in range(3):
            self.call("presence", "a", "0")
        self.assertEqual(self.counts(), {})


if __name__ == "__main__":
    unittest.main()
