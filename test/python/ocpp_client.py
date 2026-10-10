"""Small clients for the tests: an OCPP-J charge point (WebSocket), a CSMS
worker (AMQP 0-9-1), the management HTTP API and the broker's shell."""

import base64
import json
import os
import shlex
import socket
import subprocess
import time
import uuid

import pika
import requests
import websocket

HERE = os.path.dirname(os.path.abspath(__file__))
COMPOSE = f"docker compose -f {HERE}/docker-compose.yml"


def env(name, default):
    return os.environ.get(name, default)


OCPP_URL = env("OCPP_URL", "ws://localhost:19520/ocpp")
VHOST = env("OCPP_VHOST", "/")
AMQP_HOST = env("AMQP_HOST", "localhost")
AMQP_PORT = int(env("AMQP_PORT", "5672"))
MGMT_URL = env("MGMT_URL", "http://localhost:15672")
PROMETHEUS_URL = env("PROMETHEUS_URL", "http://localhost:15692")
ADMIN_USER = env("ADMIN_USER", "admin")
ADMIN_PASS = env("ADMIN_PASS", "admin")
MQTT_PORT = int(env("MQTT_PORT", "1883"))
# The exchange the plugin publishes to and binds charge point queues to.
EXCHANGE = env("OCPP_EXCHANGE", "amq.topic")
# Prefix of commands run in the broker container, e.g. rabbitmqctl. Empty
# disables the tests that need it.
BROKER_EXEC = env("BROKER_EXEC", f"{COMPOSE} exec -T rabbitmq")
BROKER_RESTART = env("BROKER_RESTART", f"{COMPOSE} restart rabbitmq")
BROKER_LOGS = env("BROKER_LOGS", f"{COMPOSE} logs --no-color rabbitmq")

PASSWORD = "secret"


def unique_id(prefix="cp"):
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


# ---------------------------------------------------------------------------
# Management HTTP API
# ---------------------------------------------------------------------------

def mgmt(method, path, **kwargs):
    resp = requests.request(method, MGMT_URL + "/api" + path,
                            auth=(ADMIN_USER, ADMIN_PASS), timeout=30, **kwargs)
    if resp.status_code >= 400 and resp.status_code != 404:
        raise RuntimeError(f"{method} {path}: {resp.status_code} {resp.text}")
    return resp


def q(name):
    return requests.utils.quote(name, safe="")


def ensure_user(name, password=PASSWORD, vhost=VHOST, tags=""):
    mgmt("PUT", f"/users/{q(name)}", json={"password": password, "tags": tags})
    set_permissions(name, ".*", ".*", ".*", vhost=vhost)


def set_permissions(user, configure, write, read, vhost=VHOST):
    mgmt("PUT", f"/permissions/{q(vhost)}/{q(user)}",
         json={"configure": configure, "write": write, "read": read})


def delete_user(name):
    mgmt("DELETE", f"/users/{q(name)}")


def delete_queue(name, vhost=VHOST):
    mgmt("DELETE", f"/queues/{q(vhost)}/{q(name)}")


def get_queue(name, vhost=VHOST):
    resp = mgmt("GET", f"/queues/{q(vhost)}/{q(name)}")
    return resp.json() if resp.status_code == 200 else None


def wait_until(condition, timeout=10, interval=0.2):
    deadline = time.time() + timeout
    while True:
        value = condition()
        if value:
            return value
        if time.time() > deadline:
            return value
        time.sleep(interval)


# ---------------------------------------------------------------------------
# Charge point
# ---------------------------------------------------------------------------

class Closed(Exception):
    """The server closed the WebSocket. code is None for an abnormal close."""

    def __init__(self, code):
        super().__init__(f"connection closed (code {code})")
        self.code = code


class ChargePoint:
    def __init__(self, ws, client_id):
        self.ws = ws
        self.client_id = client_id
        self.closed_code = "open"

    def send(self, frame):
        self.ws.send(json.dumps(frame) if not isinstance(frame, str) else frame)

    def call(self, action, payload=None, msg_id=None):
        msg_id = msg_id or uuid.uuid4().hex[:20]
        self.send([2, msg_id, action, payload if payload is not None else {}])
        return msg_id

    def result(self, msg_id, payload=None):
        self.send([3, msg_id, payload if payload is not None else {}])

    def recv(self, timeout=5):
        """Returns the next OCPP frame, None on timeout. Raises Closed."""
        deadline = time.time() + timeout
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                return None
            self.ws.settimeout(remaining)
            try:
                opcode, frame = self.ws.recv_data_frame(True)
            except websocket.WebSocketTimeoutException:
                return None
            except (websocket.WebSocketConnectionClosedException, ConnectionError, OSError):
                self.closed_code = None
                raise Closed(None)
            if opcode == websocket.ABNF.OPCODE_CLOSE:
                code = int.from_bytes(frame.data[:2], "big") if len(frame.data) >= 2 else None
                self.closed_code = code
                raise Closed(code)
            if opcode in (websocket.ABNF.OPCODE_PING, websocket.ABNF.OPCODE_PONG):
                # websocket-client answers pings itself.
                continue
            return json.loads(frame.data)

    def wait_closed(self, timeout=5):
        """Returns the close code if the server closes the connection within
        timeout (None for an abnormal close), "open" otherwise."""
        try:
            while self.recv(timeout) is not None:
                pass
        except Closed as closed:
            return closed.code
        return "open"

    def is_open(self, timeout=0.5):
        return self.wait_closed(timeout) == "open"

    def close(self):
        try:
            self.ws.close()
        except Exception:
            pass


def try_connect(client_id, user=None, password=PASSWORD, protocols=("ocpp1.6",),
                vhost=VHOST, create_user=True, sock=None, url=OCPP_URL, timeout=10):
    """Returns (ChargePoint or None, HTTP status of the upgrade response).

    By default the charge point authenticates with a user named after its
    client ID (OCPP security profiles 1 and 2)."""
    user = client_id if user is None else user
    if create_user:
        ensure_user(user, password, vhost=vhost)
    headers = []
    if user is not False:
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        headers.append(f"Authorization: Basic {token}")
    full_url = f"{url}/{q(vhost)}/{q(client_id)}"
    try:
        ws = websocket.create_connection(full_url, subprotocols=list(protocols),
                                         header=headers, timeout=timeout, socket=sock,
                                         enable_multithread=True)
    except websocket.WebSocketBadStatusException as e:
        return None, e.status_code
    except (websocket.WebSocketException, ConnectionError, OSError) as e:
        # Nothing listening, or the connection was dropped.
        return None, repr(e)
    return ChargePoint(ws, client_id), 101


def connect(client_id, **kwargs):
    cp, status = try_connect(client_id, **kwargs)
    assert cp is not None, f"WebSocket upgrade for {client_id!r} failed with HTTP {status}"
    # The server sets up the session (queue, binding, consumer) right after
    # the upgrade response. Wait for the binding of the charge point queue.
    vhost = kwargs.get("vhost", VHOST)
    wait_until(lambda: cp_queue_bound(client_id, vhost), timeout=10)
    time.sleep(0.3)
    return cp


def cp_queue_bound(client_id, vhost=VHOST, exchange=None):
    resp = mgmt("GET", f"/bindings/{q(vhost)}/e/{q(exchange or EXCHANGE)}/q/{q('ocpp.' + client_id)}")
    return resp.status_code == 200 and len(resp.json()) > 0


# ---------------------------------------------------------------------------
# CSMS worker (AMQP 0-9-1)
# ---------------------------------------------------------------------------

class Worker:
    def _channel(self, vhost=VHOST):
        params = pika.ConnectionParameters(
            host=AMQP_HOST, port=AMQP_PORT, virtual_host=vhost,
            credentials=pika.PlainCredentials(ADMIN_USER, ADMIN_PASS))
        conn = pika.BlockingConnection(params)
        return conn, conn.channel()

    def declare_queue(self, name, binding_key, arguments=None, exchange=None):
        conn, ch = self._channel()
        try:
            ch.queue_declare(name, durable=True, arguments=arguments or {})
            ch.queue_bind(name, exchange or EXCHANGE, routing_key=binding_key)
        finally:
            if conn.is_open:
                conn.close()

    def publish_to_cp(self, client_id, frame, exchange=None):
        conn, ch = self._channel()
        try:
            ch.confirm_delivery()
            ch.basic_publish(exchange or EXCHANGE, client_id,
                             json.dumps(frame).encode())
        finally:
            if conn.is_open:
                conn.close()

    def get_all(self, queue):
        """Drains the queue: [(routing_key, properties, decoded frame)]."""
        conn, ch = self._channel()
        msgs = []
        try:
            while True:
                method, props, body = ch.basic_get(queue, auto_ack=True)
                if method is None:
                    return msgs
                try:
                    frame = json.loads(body)
                except ValueError:
                    frame = body
                msgs.append((method.routing_key, props, frame))
        finally:
            if conn.is_open:
                conn.close()

    def message_count(self, queue):
        conn, ch = self._channel()
        try:
            return ch.queue_declare(queue, passive=True).method.message_count
        finally:
            if conn.is_open:
                conn.close()

    def from_cp(self, queue, client_id, expected=1, timeout=10):
        """Collects messages published by the charge point (reply_to = its
        client ID) until `expected` arrived or the timeout expired."""
        found = []
        deadline = time.time() + timeout
        while True:
            found += [m for m in self.get_all(queue) if m[1].reply_to == client_id]
            if len(found) >= expected or time.time() > deadline:
                return found
            time.sleep(0.2)


# ---------------------------------------------------------------------------
# Broker shell (rabbitmqctl in the container)
# ---------------------------------------------------------------------------

class Broker:
    def __init__(self, prefix):
        self.prefix = shlex.split(prefix)

    def run(self, *args, timeout=120, check=True):
        proc = subprocess.run(self.prefix + list(args), capture_output=True, text=True,
                              timeout=timeout)
        if check and proc.returncode != 0:
            raise RuntimeError(f"{args}: {proc.returncode}\n{proc.stdout}\n{proc.stderr}")
        return proc

    def eval(self, expr, timeout=60):
        """Evaluates an Erlang expression on the broker node, returns its
        printed result."""
        return self.run("rabbitmqctl", "eval", expr, timeout=timeout).stdout.strip()

    def eval_int(self, expr):
        return int(self.eval(expr))

    def ocpp_connection_pid(self, client_id):
        """Erlang expression evaluating to the pid of the client's connection."""
        return ("hd([P || P <- rabbit_web_ocpp_app:list_connections(), "
                "(catch rabbit_web_ocpp_handler:info(P, [client_id])) =:= "
                f"[{{client_id, <<\"{client_id}\">>}}]])")

    def queue_pid(self, queue, vhost=VHOST):
        return (f"(fun() -> {{ok, Q}} = rabbit_amqqueue:lookup(rabbit_misc:r(<<\"{vhost}\">>, "
                f"queue, <<\"{queue}\">>)), amqqueue:get_pid(Q) end)()")

    def set_memory_alarm(self):
        self.eval("rabbit_alarm:set_alarm({{resource_limit, memory, node()}, []}).")

    def clear_memory_alarm(self):
        self.eval("rabbit_alarm:clear_alarm({resource_limit, memory, node()}).")


def broker_available(prefix):
    if not prefix:
        return False
    try:
        return Broker(prefix).run("rabbitmqctl", "status", timeout=60,
                                  check=False).returncode == 0
    except Exception:
        return False


def wait_for_broker(timeout=180):
    def ready():
        try:
            return requests.get(MGMT_URL + "/api/overview", auth=(ADMIN_USER, ADMIN_PASS),
                                timeout=5).status_code == 200
        except Exception:
            return False
    assert wait_until(ready, timeout=timeout, interval=2), "broker did not come up"
    # The OCPP listener is started by the plugin, after the management API.
    assert wait_until(lambda: ocpp_listening(), timeout=timeout, interval=2), \
        "OCPP listener did not come up"


def ocpp_listening(url=OCPP_URL):
    """Whether the OCPP listener answers HTTP requests. (A TCP connect is not
    enough: Docker's port forwarding accepts connections for a container that
    does not listen.)"""
    host, port = ocpp_host_port(url)
    try:
        requests.get(f"http://{host}:{port}/", timeout=3)
        return True
    except requests.RequestException:
        return False


def ocpp_host_port(url=OCPP_URL):
    hostport = url.split("://", 1)[1].split("/", 1)[0]
    host, _, port = hostport.partition(":")
    return host, int(port or 80)
