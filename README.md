# RabbitMQ Web OCPP plugin

A RabbitMQ plugin that turns the broker into a [highly-scalable](https://www.rabbitmq.com/blog/2023/03/21/native-mqtt#1-million-mqtt-connections), memory-efficient and low-latency gateway for EV charge stations. This plugin provides a native thin translator layer for OCPP-over-WebSockets to RabbitMQ AMQP protocol. Both version `1.6J` and `2.x` should be supported as the base JSON format array was kept backwards compatible, even tho many of the action names and payloads are changed.

## Motivation

Doing research for our CSMS platform, we found [IoT and WebSockets in K8s: Operating and Scaling an EV Charging Station Network - Saadi Myftija](https://www.youtube.com/watch?v=CuiY1Vj-A5E) and [Building an OCPP-compliant electric vehicle charge point operator solution using AWS IoT Core](https://aws.amazon.com/blogs/iot/building-an-ocpp-compliant-electric-vehicle-charge-point-operator-solution-using-aws-iot-core/), both ilustrating a rather complex and costly cloud architecture for this use-case. CPOs frequently need to fan-in **tens of thousands of charge points (CPs)** over the public (LTE) Internet while keeping stateful request/response semantics (RPC), durable command queues and enterprise-grade HA. 
RabbitMQ already excels at message durability and routing, but traditionally the OCPP world still relies on proprietary gateways or heavyweight HTTP stacks.

`rabbitmq_web_ocpp` closes that gap:

* **Zero external proxy** – The native Erlang HTTP server included in RabbitMQ, Cowboy, terminates `wss://` connections on the broker node, allowing even mTLS peer verification.
* **Native AMQP routing** – every OCPP frame is stored as a RabbitMQ *message container*; you fan-out, DLX, or mirror it; OCPP backend can be written in any programming language as queue workers.
* **One Erlang process per charger** – the same footprint as the native MQTT rewrite.
* **Reconnection storms resilience** - handled by RabbitMQ HA cluster, backend scalling is decopled.

## Why “one Erlang process per charger” actually scales

* **Low memory usage per process** – a BEAM process starts with a 256-word heap; even
  with the pending-map and a few binaries a live Web-OCPP handler + channel
  stays < few KiB.
* **No kernel threads** – the BEAM scheduler multiplexes hundreds of
  thousands of lightweight processes onto a fixed pool of OS threads.
  Context switches are micro-seconds and never hit the kernel.
* **Per-process garbage collection** – pauses are micro-scopic and local;
  one slow charger cannot block the others.
* **Built-in crash isolation & supervision** – the classic *let it crash*
  idiom restarts a misbehaving charger process without touching neighbours,
  something a monolithic Java or Go gateway must re-implement.
* **Direct in-VM routing** – by skipping TCP and AMQP frames the path from
  WebSocket frame → queue deliver → WebSocket send is one message copy
  inside the VM—not four kernel crossings like a side-car proxy.


## Installation

This plugin works only with modern versions of RabbitMQ 4.x based on AMQP 1.0.

### Docker image

The quickest way to get started is the prebuilt multi-arch (`amd64`/`arm64`) image, published to GitHub Container Registry on every push to `master`: [`ghcr.io/vampirebyte/rabbitmq-web-ocpp`](https://github.com/vampirebyte/rabbitmq-web-ocpp/pkgs/container/rabbitmq-web-ocpp). It is the official `rabbitmq:<version>-management` image with the plugin baked in and already enabled, so no further setup is required:

``` bash
docker run -it --rm --name rabbitmq-ocpp \
    -p 5672:5672 -p 15672:15672 -p 19520:19520 \
    ghcr.io/vampirebyte/rabbitmq-web-ocpp:4.3.1-ocpp
```

Tags follow the bundled RabbitMQ version (`4.3.1-ocpp`, `4.2.7-ocpp`, ...), plus a `-<short sha>` variant pinning the exact plugin commit.

### Plugin archive (.ez)

You can [build from source](https://www.rabbitmq.com/plugin-development.html) or you can download the latest release build from GitHub. Unzip and place the `rabbitmq_web_ocpp-4.x.x.ez` file into your `/etc/rabbitmq/plugins/` folder.
Like all plugins, it [must be enabled](https://www.rabbitmq.com/plugins.html) before it can be used:

``` bash
# this might require sudo
rabbitmq-plugins enable rabbitmq_web_ocpp
```

Detailed instructions on how to install a plugin into RabbitMQ broker can be found [here](https://www.rabbitmq.com/plugins.html#installing-plugins).

Note that release branches (`v4.1.x` vs. `main`) and target RabbitMQ version need to be taken into account
when building plugins from source.

## How It Works

The communication flow is straightforward: 
1. Connect your EVSE to the following OCPP endpoint: `ws://127.0.0.1:19520/ocpp/%2F/<EVSE ID>` for `/` (default) vhost running on docker or adjust the URL accordingly. The EVSE authenticates with HTTP Basic auth using its EVSE ID as username (OCPP security profiles 1 and 2), or with a client certificate whose identity is the EVSE ID (security profile 3).
2. Messages arriving from the EVSE are published as persistent messages to the configured exchange (default: `ocpp`, a durable topic exchange the plugin declares) with `correlation_id` set to the OCPP `messageId` and `reply_to` set to the EVSE ID.
3. Configure backend worker routing on the CSMS side by creating a queue bound to the same exchange. Use routing keys in the format: `protocolver.actionname.req/conf/error`. Examples: `ocpp16.BootNotification.req`, `ocpp16.GetConfiguration.conf`, `ocpp201.StatusNotification.req`. Answers of the EVSE to commands of the CSMS carry the action of the command, e.g. the answer to a `RemoteStartTransaction` is routed as `ocpp16.RemoteStartTransaction.conf` (or `.error`) and its AMQP `type` property is `RemoteStartTransaction`. Common patterns include `ocpp16.#` for all v1.6 traffic or `*.StartTransaction.#` for billing-specific workers. See the [RabbitMQ Topics tutorial](https://www.rabbitmq.com/tutorials/tutorial-five-python#topic-exchange) for details.
4. After processing and validating the message in your async worker, build a valid OCPP Response (or error) and publish it back to the same exchange with the routing key set to the EVSE ID and `correlation_id` set to the original request's OCPP `messageId`. The plugin handles sending this message back to the EVSE via the correct WebSocket connection.
5. Commands of the CSMS (CALLs) are published the same way. As OCPP-J requires, the plugin sends an EVSE one CALL at a time: the next one once the EVSE answered the previous one or `web_ocpp.call_timeout` expired (the timeout is paused while the plugin does not read from the EVSE because of back pressure). An answer must be well formed (a CALLRESULT payload is an object; a CALLERROR has a string code and description and object details), otherwise the connection is closed and the CALL redelivered. A CALL stays unacknowledged in the EVSE's queue until then, so it is delivered again if the EVSE disconnects before answering. Commands for an EVSE that stays offline longer than `web_ocpp.queue_message_ttl` are dropped.
6. A CALL of the EVSE that no queue is bound for is answered with a `NotImplemented` CALLERROR right away.
7. Queues can be consumed by multiple identical, stateless workers written in any programming language. Monitor queues using built-in tools (e.g., Grafana) and configure auto-scaling based on message latency or queue depth.
8. If a worker throws an exception before sending a valid OCPP response, standard AMQP ACK/NACK principles apply: unconfirmed messages return to the queue for processing by another worker. Handle failure scenarios (e.g., database outages) gracefully to avoid infinite retry loops.

Workers can use AMQP 0-9-1 or AMQP 1.0 (send commands to the address `/exchanges/ocpp/<EVSE ID>`), and messages of EVSEs can also be stored in streams, e.g. for auditing. The usual [connection limits](https://www.rabbitmq.com/docs/vhosts#limits) of vhosts and users apply to EVSE connections (refused with HTTP status 429), and an EVSE stops being read from while the queues it publishes to cannot keep up (credit flow) or a resource alarm is in effect.

With `web_ocpp.proxy_protocol = true`, the client address announced by the load balancer is used for the `loopback_users` check, failed authentication attempts and connection details.

EVSE IDs may only contain letters, digits and `-_~!$&'()+,;=:@`, at most 48 characters (`web_ocpp.max_client_id_length`): the EVSE ID is used as binding key on a topic exchange, so `.`, `*` and `#` are rejected.

### Configuration

| Setting (`rabbitmq.conf`) | Default | |
|---|---|---|
| `web_ocpp.exchange` | `ocpp` | Exchange the plugin publishes to and binds EVSE queues to. Declared as a durable topic exchange when missing. |
| `web_ocpp.protocols.<n>` | `ocpp1.6`, `ocpp2.0`, `ocpp2.0.1`, `ocpp2.1` | Accepted OCPP-J versions. |
| `web_ocpp.username_must_match_client_id` | `true` | Require the Basic auth username to be the EVSE ID. |
| `web_ocpp.max_client_id_length` | `48` | Longest accepted EVSE ID. |
| `web_ocpp.queue_type` | `classic` | Type of new EVSE queues (`ocpp.<EVSE ID>`). Use `quorum` so that EVSEs can reconnect to another node when the node of their queue is down. |
| `web_ocpp.queue_message_ttl` | `300000` | Message TTL (ms) of new EVSE queues, `none` to keep commands forever. |
| `web_ocpp.queue_expires` | `604800000` | Queues of EVSEs that do not connect for this long (ms) are deleted, `none` to keep them. |
| `web_ocpp.prefetch_count` | `10` | Credit for deliveries from an EVSE queue. CALLs held back behind the outstanding one do not use it up, so answers to the EVSE's own requests are delivered past them. |
| `web_ocpp.max_held_calls` | `100` | CALLs held back (unacknowledged, in memory) per EVSE. Once reached, nothing more is delivered from the EVSE queue until CALLs were answered or timed out. |
| `web_ocpp.call_timeout` | `30000` | How long (ms) a CALL sent to an EVSE may stay unanswered. |
| `web_ocpp.permission_cache_ttl` | `60000` | How long (ms) connections cache permission checks. |
| `web_ocpp.ws_opts.idle_timeout` | `60000` | WebSocket idle timeout (ms). |
| `web_ocpp.ws_opts.ping_interval` | half the idle timeout | Interval (ms) of the WebSocket pings the plugin sends, `0` disables them. EVSEs answer with pongs, which keeps them connected. |
| `web_ocpp.ws_opts.max_frame_size` | `1048576` | Largest accepted WebSocket frame (bytes). |

Queue arguments only apply to newly declared EVSE queues: use a [policy](https://www.rabbitmq.com/docs/policies) for existing ones.

#### Upgrading

* The default exchange changed from `amq.topic` to `ocpp`, so that MQTT and STOMP clients publishing to `amq.topic` cannot reach EVSEs. Set `web_ocpp.exchange = amq.topic` to keep the previous behaviour. When an EVSE connects, the bindings of its queue made by previous versions (to another exchange, with the EVSE ID as key) are removed.
* Answers of EVSEs to commands are routed as `<version>.<action>.conf|error` instead of `<version>.response.conf|error`. Only answers that arrive after `web_ocpp.call_timeout` still use `response`.
* The Basic auth username must be the EVSE ID. Set `web_ocpp.username_must_match_client_id = false` for users shared by several EVSEs.
* EVSE IDs with characters outside the set above are rejected, as are the subprotocols `ocpp1.2` and `ocpp1.5` (which only exist as SOAP).

## Offline Detection

Whenever an established charge point connection terminates — clean WebSocket close, TCP drop or broker shutdown — the plugin publishes one final synthetic `StatusNotification` CALL on behalf of the charge point, so backend workers learn about the disconnect through the same channel as any other OCPP traffic. The payload marks the whole charge point (`connectorId` 0) unavailable, shaped for the protocol version the charge point was connected with:

OCPP 1.x:

```json
[2,"40a2216a-4c22-37f8-28f2-92b7e6ba205e","StatusNotification",{"connectorId":0,"errorCode":"NoError","status":"Unavailable","timestamp":"2026-07-17T13:31:19Z","vendorErrorCode":"Offline","vendorId":"rabbitmq"}]
```

OCPP 2.x:

```json
[2,"83c2c788-712b-18a4-7456-29a4586ddb4a","StatusNotification",{"connectorId":0,"connectorStatus":"Unavailable","customData":{"vendorErrorCode":"Offline","vendorId":"rabbitmq"},"evseId":0,"timestamp":"2026-07-17T13:10:27Z"}]
```

No offline status is published when a connection is replaced by a newer connection of the same charge point, which is online. Nothing can be published when a node crashes (e.g. on `kill -9` or power loss): use the event exchange described below, or heartbeat supervision on the CSMS side, to detect that.

Workers can recognize the synthetic frame by `vendorErrorCode` or `vendorId` — e.g. to skip sending the CALLRESULT, which would otherwise sit in the disconnected charge point's queue until it reconnects and be discarded because of an unknown `messageId`.

Alternatively (or additionally — e.g. to also detect chargers coming *online* - if you don't do this by StatusNotification), enable the [`rabbitmq_event_exchange`](https://www.rabbitmq.com/docs/event-exchange) plugin and bind a queue to the internal `amq.rabbitmq.event` topic exchange for the `connection.created` and `connection.closed` routing keys. Connections handled by this plugin carry a `protocol` header of `{'WS OCPP', ...}` and a `client_id` header with the EVSE ID, so consumers can filter out non-OCPP connections (management UI, shovels, backend workers) and map events back to charge points.

## Tests

The test suites run against a RabbitMQ source tree, like all plugins:

``` bash
git clone --branch v4.2.7 --depth 1 https://github.com/rabbitmq/rabbitmq-server.git
cp -r rabbitmq-web-ocpp rabbitmq-server/deps/rabbitmq_web_ocpp
cd rabbitmq-server/deps/rabbitmq_web_ocpp
make ct-config_schema ct-ocpp ct-ocpp_cluster
```

## Documentation

For all configuration options, please refer to the nearly identical plugin, [RabbitMQ Web MQTT guide](https://www.rabbitmq.com/web-mqtt.html).

## Screenshots

![RabbitMQ Web OCPP Management Interface](examples/screenshots/Screenshot_RabbitMQ_1.png)

## Enterprise-Grade Hosting & SLA Support

For CPOs or platform operators that need to onboard fleets of tens of thousands of chargers, our team offers cloud-native RabbitMQ HA deployments in AWS, Azure or GCP, complete with 24/7 monitoring, incident response, rolling upgrades, and expert assistance for PKI, Prometheus dashboards and OCPP-specific queue policies; we can also deliver custom feature work; we tailor service levels and cluster topologies so you can scale from pilot projects to nationwide networks without re-architecting.

## Copyright and License

(c) 2007-2024 Broadcom. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.  
(c) 2025 VAMPIRE BYTE SRL. All Rights Reserved.

Released under the same license as RabbitMQ. See [LICENSE](./LICENSE) for details.
