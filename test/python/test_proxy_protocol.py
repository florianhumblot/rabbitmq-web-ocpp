"""PROXY protocol (web_ocpp.proxy_protocol = true), e.g. behind HAProxy or a
cloud load balancer. Needs a broker started with docker-compose.proxy.yml,
see the README."""

import os
import socket

import pytest

import ocpp_client as oc

pytestmark = pytest.mark.skipif(os.environ.get("OCPP_PROXY_PROTOCOL") != "1",
                                reason="needs a broker with web_ocpp.proxy_protocol = true")

CLIENT_IP = "192.0.2.10"


def proxied_socket():
    host, port = oc.ocpp_host_port()
    sock = socket.create_connection((host, port))
    sock.sendall(f"PROXY TCP4 {CLIENT_IP} 127.0.0.1 40000 {port}\r\n".encode())
    return sock


def test_client_address_is_taken_from_the_proxy_header(res):
    """The plugin ignores the client address announced in the PROXY header,
    and uses the load balancer's: for the loopback_users check (with the load
    balancer on the broker host, any client passes it, so e.g. 'guest' can
    log in from anywhere), for failed authentication attempts, and for the
    connection's details."""
    cid = res.cid()
    oc.ensure_user(cid)
    cp, status = res.try_connect(cid, create_user=False, sock=proxied_socket())
    assert cp is not None, f"upgrade failed with HTTP {status}"

    def peer_host():
        for c in oc.mgmt("GET", "/connections").json():
            if (c.get("client_properties") or {}).get("chargePointId") == cid:
                return c.get("peer_host")
        return None
    host = oc.wait_until(peer_host, timeout=15)
    assert host == CLIENT_IP, f"the connection's peer address is {host}, not {CLIENT_IP}"
