# Black-box tests for rabbitmq_web_ocpp

These tests show the flaws found in a review of the `rabbitmq_web_ocpp`
plugin, against a running broker. They act like a charge point (OCPP-J over
WebSocket), a CSMS worker (AMQP 0-9-1 and 1.0), an MQTT client and an
operator (management API, `rabbitmqctl`).

**Every test asserts the correct behaviour, so it fails while the flaw
exists.** The failure message says what went wrong, for example:

```
FAILED test_critical.py::test_c4_hash_client_id_receives_all_traffic
AssertionError: charge point '#' received traffic of charge point victim-634d7566c9:
[[2, 'csms-secret', 'ChangeConfiguration', {'key': 'AuthorizationKey', 'value': 's3cr3t'}],
 [2, '0871101a5cdc48ab95e3', 'Authorize', {'idTag': '0123456789'}]]
```

## Running

Needs Docker with Compose, and Python 3.9 or later.

```bash
cd test/python
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

docker compose up -d   # the published image, ghcr.io/vampirebyte/rabbitmq-web-ocpp:4.3.1-ocpp
pytest                 # about 5 minutes
docker compose down -v
```

To test another build, e.g. one built from this repository with its
Dockerfile:

```bash
docker build --build-context plugin=. --target runtime -t rabbitmq-web-ocpp:dev .
OCPP_IMAGE=rabbitmq-web-ocpp:dev docker compose -f test/python/docker-compose.yml up -d
```

Use `pytest -m "not slow"` to skip the two tests that wait for a broker
timeout (about a minute each), or `pytest -k c4` to run some of them.

Some tests use `rabbitmqctl` in the broker container (`docker compose exec`),
to provoke a condition (a resource alarm, a crashed queue, a stuck connection)
or to observe it (the atom table, a process mailbox). One of them restarts
the broker. They are skipped when the broker shell is not available.

Two setups are separate, because they need a different broker:

```bash
# PROXY protocol (web_ocpp.proxy_protocol = true), as behind a load balancer
docker compose -f docker-compose.yml -f docker-compose.proxy.yml up -d
OCPP_PROXY_PROTOCOL=1 pytest test_proxy_protocol.py
docker compose -f docker-compose.yml -f docker-compose.proxy.yml down -v

# 3 node cluster, for high availability
docker compose -f docker-compose.cluster.yml up -d
OCPP_CLUSTER=1 OCPP_URL=ws://localhost:19521/ocpp pytest test_cluster.py
docker compose -f docker-compose.cluster.yml down -v
```

### Settings

Environment variables, the defaults match `docker-compose.yml`:

| Variable | Default | |
|---|---|---|
| `OCPP_URL` | `ws://localhost:19520/ocpp` | OCPP-J endpoint, without vhost and client ID |
| `OCPP_EXCHANGE` | `amq.topic` | Exchange the plugin publishes to |
| `AMQP_HOST`, `AMQP_PORT` | `localhost`, `5672` | AMQP 0-9-1 and 1.0 |
| `MGMT_URL` | `http://localhost:15672` | Management API |
| `PROMETHEUS_URL` | `http://localhost:15692` | Prometheus metrics |
| `ADMIN_USER`, `ADMIN_PASS` | `admin`, `admin` | Administrator of the broker |
| `MQTT_PORT` | `1883` | MQTT |
| `BROKER_EXEC` | `docker compose ... exec -T rabbitmq` | Prefix to run `rabbitmqctl` on the broker, empty to skip those tests |
| `BROKER_RESTART` | `docker compose ... restart rabbitmq` | Restarts the broker |
| `BROKER_LOGS` | `docker compose ... logs --no-color rabbitmq` | Prints the broker's logs |

Each charge point authenticates with a user named after its client ID, as
OCPP security profiles 1 and 2 require. The tests create and delete those
users and their queues through the management API.

## The tests

C1–C5 and 6–17 refer to the findings of the review.

| Test | Shows that |
|---|---|
| **test_critical.py** | |
| `test_c1_quorum_queue_receives_every_message` | a quorum queue receives only the first of 5 Heartbeats: the queue client state is discarded after every publish |
| `test_c2_deleted_queue_disconnects_charge_point` | a charge point whose queue was deleted stays connected but never receives a command again |
| `test_c2_crashed_queue_disconnects_charge_point` | the same after the queue process crashes (its node restarts): the `DOWN` is swallowed |
| `test_c3_client_properties_are_bounded` | a charge point decides how many client properties its connection stores |
| `test_c3_client_input_does_not_create_atoms` | one BootNotification creates one atom per key (2000 here); atoms are never freed, enough of them crash the node |
| `test_c4_wildcard_client_ids_are_rejected` | charge points can connect as `#`, `*` or `ocpp16.Heartbeat.req` |
| `test_c4_hash_client_id_receives_all_traffic` | a charge point connected as `#` receives the requests of other charge points and the commands sent to them |
| `test_c4_client_id_length_is_limited` | client IDs are not limited (OCPP 2.0.1: 48 characters) |
| `test_c5_username_must_be_the_client_id` | any user can connect as any charge point |
| `test_c5_other_credentials_cannot_take_over_a_session` | with another user's password, an attacker disconnects a charge point and receives its commands |
| **test_calls.py** | |
| `test_f6_one_outstanding_call_at_a_time` | commands are sent back to back, before the previous one was answered |
| `test_f6_unanswered_call_survives_disconnect` | a command is lost when the connection drops before the charge point answers |
| `test_f7_answer_routing_key_carries_action` | answers are routed as `ocpp16.response.conf`, without the action of the command |
| `test_f11_unroutable_call_gets_callerror` | a CALL nobody handles gets no answer, the charge point runs into its timeout |
| `test_f14_invalid_message_id_is_a_protocol_error` | message IDs that are no strings, or longer than 36 characters, are accepted |
| `test_f14_non_string_action_is_a_protocol_error` | a CALL with a numeric action is published as `ocpp16.response.req` |
| `test_f14_non_string_message_id_does_not_break_workers` | a message ID that is an object becomes a poison message: every AMQP 0-9-1 worker reading it is disconnected with `INTERNAL_ERROR` |
| **test_durability.py** | |
| `test_f8_charge_point_messages_are_persistent` | messages of charge points are transient |
| `test_f8_charge_point_queue_expires_commands_and_itself` | commands wait forever for an offline charge point, queues of decommissioned charge points stay forever |
| `test_f8_offline_status_survives_broker_restart` | the offline status published when the broker stops is lost |
| `test_f15_reconnect_does_not_announce_offline` | a charge point that reconnects is announced offline |
| `test_f15_stuck_old_connection_does_not_block_reconnect` | while an old connection hangs, the charge point cannot reconnect |
| `test_f15_newer_connection_survives_join_of_older` | after a network partition heals, both connections of a charge point close |
| **test_backpressure.py** | |
| `test_f10_memory_alarm_stops_charge_points_publishing` | charge points keep publishing during a memory alarm |
| `test_f10_slow_queue_throttles_charge_point` | a charge point fills the mailbox of a stalled queue without limit (no credit flow) |
| **test_connection.py** | |
| `test_f12_quiet_charge_point_stays_connected` | a charge point that sends nothing for a minute is disconnected: the server never pings (slow) |
| `test_f13_oversized_frame_is_rejected` | frames of any size are accepted (4 MiB here) |
| `test_f13_invalid_json_is_not_logged_in_full` | 200 kB of invalid JSON produce about 9 MB of logs, and block the connection for seconds |
| `test_f16_mqtt_client_cannot_reach_charge_points` | an MQTT client publishing to topic `<client ID>` sends frames to that charge point |
| `test_f16_amq_topic_does_not_reach_charge_points` | the same for any client publishing to `amq.topic` |
| `test_f17_revoked_permissions_take_effect` | revoked permissions do not apply to connected charge points (slow) |
| `test_f17_soap_only_versions_are_rejected` | the subprotocols `ocpp1.2` and `ocpp1.5` (SOAP only) are accepted |
| `test_f17_disabling_the_plugin_stops_its_listener` | charge points can still connect after the plugin was disabled (RabbitMQ 4.2+) |
| **test_interop.py** | |
| `test_amqp10_worker_receives_charge_point_messages` | AMQP 1.0 consumers cannot receive messages of charge points: the session fails |
| `test_stream_bound_to_the_exchange_does_not_break_charge_points` | binding a stream (e.g. for auditing) crashes the connection of every charge point that publishes to it |
| `test_vhost_connection_limit_applies` | vhost connection limits do not apply to OCPP |
| `test_user_connection_limit_applies` | user connection limits do not apply to OCPP |
| `test_cli_lists_connections_while_http_clients_are_connected` | `rabbitmqctl list_web_ocpp_connections` prints `{:badrpc, ...}` instead of the charge points while slow HTTP clients are connected |
| `test_metrics_count_ocpp_connections_and_messages` | the consumers metric grows with every connection and never decreases; received messages are not counted |
| **test_proxy_protocol.py** (separate setup) | |
| `test_client_address_is_taken_from_the_proxy_header` | the load balancer's address is used instead of the client's, also for the `loopback_users` check |
| **test_cluster.py** (separate setup) | |
| `test_f9_charge_point_survives_the_loss_of_a_node` | when the node of its queue is down, a charge point cannot connect to any other node |

Not covered here, because they are not visible from outside the broker: the
repository had no OCPP tests and CI did not run any, `-Werror` was disabled
for the whole build (hiding the dead code in `process_connect/9`), and the
configuration schema still contains settings of the Web MQTT plugin.

## Results

Against an image built from the plugin's code at commit `aace275` on
RabbitMQ 4.2.7 (the runtime stage of the repository's Dockerfile), all tests
fail, including those of the two separate setups. Against the branch that
fixes them, all pass (with `OCPP_EXCHANGE=ocpp`, the default exchange there).
