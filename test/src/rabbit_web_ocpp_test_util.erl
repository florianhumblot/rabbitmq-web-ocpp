%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2025 VAMPIRE BYTE SRL. All Rights Reserved.
%%

%% Helpers shared by the OCPP test suites: a charge point side (WebSocket
%% client speaking OCPP-J) and a CSMS worker side (AMQP 0-9-1 channel).
-module(rabbit_web_ocpp_test_util).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([merge_app_env/1,
         init_ocpp_listeners/1,
         init_ocpp_listener/2,
         set_env/3,
         unset_env/2,
         restart_plugin/2,
         port/2,
         tls_port/2,
         ensure_user/3,
         amqp10_get/2,
         amqp10_publish_to_cp/3,
         await_session/3,
         exchange/1,
         ensure_user/2,
         connect/2,
         connect/3,
         try_connect/3,
         http_status/1,
         send/2,
         call/4,
         call/3,
         result/3,
         recv/1,
         recv/2,
         assert_no_frame/2,
         assert_closed/2,
         is_open/1,
         channel/1,
         channel/2,
         with_channel/2,
         declare_worker_queue/3,
         declare_worker_queue/4,
         delete_queue/2,
         publish_to_cp/3,
         publish_to_cp/4,
         get_message/2,
         get_messages/2,
         message_count/2,
         cp_queue_pid/2,
         connection_pids/1,
         wait_for_connections/2,
         purge_bindings/1]).

-define(PASSWORD, <<"secret">>).
-define(RECV_TIMEOUT, 5000).

%% -------------------------------------------------------------------
%% Broker side
%% -------------------------------------------------------------------

%% Nodes start without an OCPP listener: they would all use the default port.
merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(Config, {rabbitmq_web_ocpp, [{tcp_config, []}]}).

%% rabbitmq_ct_helpers knows nothing about this plugin, so it does not
%% allocate an OCPP port. Reuse the port it allocates for Web MQTT, which is
%% not running in these suites, and restart the plugin on it.
init_ocpp_listeners(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [ok = init_ocpp_listener(Config, I) || I <- lists:seq(0, length(Nodes) - 1)],
    Config.

init_ocpp_listener(Config, Node) ->
    Port = port(Config, Node),
    ok = rabbit_ct_broker_helpers:rpc(Config, Node, application, set_env,
                                      [rabbitmq_web_ocpp, tcp_config, [{port, Port}]]),
    restart_plugin(Config, Node).

set_env(Config, Key, Val) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [ok = rabbit_ct_broker_helpers:rpc(Config, I, application, set_env,
                                       [rabbitmq_web_ocpp, Key, Val])
     || I <- lists:seq(0, length(Nodes) - 1)],
    ok.

unset_env(Config, Key) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [ok = rabbit_ct_broker_helpers:rpc(Config, I, application, unset_env,
                                       [rabbitmq_web_ocpp, Key])
     || I <- lists:seq(0, length(Nodes) - 1)],
    ok.

%% Needed for settings read when the listener starts.
restart_plugin(Config, Node) ->
    _ = rabbit_ct_broker_helpers:rpc(Config, Node, application, stop, [rabbitmq_web_ocpp]),
    ok = rabbit_ct_broker_helpers:rpc(Config, Node, application, start, [rabbitmq_web_ocpp]).

port(Config, Node) ->
    rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_web_mqtt).

tls_port(Config, Node) ->
    rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_web_mqtt_tls).

%% -------------------------------------------------------------------
%% CSMS worker side, AMQP 1.0
%% -------------------------------------------------------------------

amqp10_session(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    OpnConf = #{address => "localhost",
                port => Port,
                container_id => <<"ocpp-test">>,
                sasl => {plain, <<"guest">>, <<"guest">>}},
    {ok, Conn} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Conn, opened}} -> ok
    after 5000 -> ct:fail(amqp10_connection_not_opened)
    end,
    {ok, Session} = amqp10_client:begin_session_sync(Conn),
    {Conn, Session}.

%% Returns the next message of the queue.
amqp10_get(Config, QName) ->
    {Conn, Session} = amqp10_session(Config),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, <<"/queues/", QName/binary>>, settled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail(amqp10_receiver_not_attached)
    end,
    Result = amqp10_client:get_msg(Receiver, 5000),
    ok = amqp10_client:close_connection(Conn),
    Result.

%% Publishes to the exchange of the plugin, with the client ID as routing key.
amqp10_publish_to_cp(Config, ClientId, Frame) ->
    {Conn, Session} = amqp10_session(Config),
    Address = <<"/exchanges/", (exchange(Config))/binary, "/", ClientId/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link_sync(Session, <<"test-sender">>, Address),
    receive {amqp10_event, {link, Sender, credited}} -> ok
    after 5000 -> ct:fail(amqp10_sender_not_credited)
    end,
    Msg = amqp10_msg:new(<<"tag">>, iolist_to_binary(json:encode(Frame)), false),
    ok = amqp10_client:send_msg(Sender, Msg),
    receive {amqp10_disposition, {Outcome, <<"tag">>}} -> ok = amqp10_client:close_connection(Conn),
                                                          Outcome
    after 5000 -> ct:fail(amqp10_no_disposition)
    end.

%% The exchange the plugin publishes to and binds charge point queues to.
exchange(Config) ->
    X = rabbit_ct_broker_helpers:rpc(Config, 0, application, get_env,
                                     [rabbitmq_web_ocpp, exchange, <<"amq.topic">>]),
    rabbit_data_coercion:to_binary(X).

%% OCPP security profiles 1 and 2 use the charge point ID as Basic auth
%% username, so every test charge point gets a user named after it.
ensure_user(Config, Username) ->
    ensure_user(Config, Username, <<"/">>).

ensure_user(_Config, none, _Vhost) ->
    ok;
ensure_user(Config, Username, Vhost) ->
    _ = rabbit_ct_broker_helpers:add_user(Config, 0, Username, ?PASSWORD),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, Username, Vhost).

%% -------------------------------------------------------------------
%% Charge point side
%% -------------------------------------------------------------------

connect(Config, ClientId) ->
    connect(Config, ClientId, #{}).

%% Opts: node, protos, user, password, create_user, vhost, tcp_preface (e.g.
%% a PROXY protocol header), tls (TLS client options: connects to the TLS
%% listener).
connect(Config, ClientId, Opts) ->
    {WS, Status} = try_connect(Config, ClientId, Opts),
    case Status of
        101 ->
            %% The upgrade response is sent before the session is set up.
            await_session(Config, maps:get(node, Opts, 0), ClientId),
            WS;
        _ ->
            ct:fail({websocket_upgrade_failed, ClientId, Status})
    end.

await_session(Config, Node, ClientId) ->
    await(
      fun() ->
              Pids = rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_web_ocpp_app,
                                                  list_connections, [], 5000),
              lists:any(fun(Pid) ->
                                try rabbit_ct_broker_helpers:rpc(
                                      Config, Node, rabbit_web_ocpp_handler, info,
                                      [Pid, [client_id, connected_at]], 1000) of
                                    [{client_id, ClientId}, {connected_at, At}] ->
                                        is_integer(At);
                                    _ ->
                                        false
                                catch _:_ ->
                                          false
                                end
                        end, Pids)
      end, erlang:monotonic_time(millisecond) + 10000, {no_session, ClientId}).

%% Like rabbit_ct_helpers:await_condition/2, but with a deadline: the
%% condition itself may take a while.
await(Fun, Deadline, Error) ->
    case Fun() of
        true ->
            ok;
        false ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true -> ct:fail(Error);
                false -> timer:sleep(50), await(Fun, Deadline, Error)
            end
    end.

%% Returns the WebSocket client and the HTTP status of the upgrade response.
try_connect(Config, ClientId, Opts) ->
    Node = maps:get(node, Opts, 0),
    User = maps:get(user, Opts, ClientId),
    Password = maps:get(password, Opts, ?PASSWORD),
    Vhost = maps:get(vhost, Opts, <<"/">>),
    case maps:get(create_user, Opts, true) of
        true -> ensure_user(Config, User, Vhost);
        false -> ok
    end,
    Protos = maps:get(protos, Opts, ["ocpp1.6"]),
    {Scheme, Port} = case Opts of
                         #{tls := _} -> {"wss", tls_port(Config, Node)};
                         _ -> {"ws", port(Config, Node)}
                     end,
    Url = Scheme ++ "://127.0.0.1:" ++ integer_to_list(Port) ++ "/ocpp/"
          ++ uri_string:quote(binary_to_list(Vhost)) ++ "/"
          ++ uri_string:quote(binary_to_list(ClientId)),
    Auth = case User of
               none -> undefined;
               _ -> [{login, binary_to_list(User)}, {passcode, binary_to_list(Password)}]
           end,
    WS = rfc6455_client:new(Url, self(), Auth, Protos, maps:get(tcp_preface, Opts, <<>>),
                            maps:get(tls, Opts, [])),
    case rfc6455_client:open(WS) of
        {ok, [{http_response, Resp}]} -> {WS, http_status(Resp)};
        {close, _} -> {WS, closed}
    end.

http_status(Resp) ->
    {_, Status, _, _} = cow_http:parse_status_line(rabbit_data_coercion:to_binary(Resp ++ "\r\n")),
    Status.

send(WS, Term) ->
    rfc6455_client:send(WS, iolist_to_binary(json:encode(Term))).

call(WS, MsgId, Action, Payload) ->
    send(WS, [2, MsgId, Action, Payload]).

call(WS, Action, Payload) ->
    MsgId = integer_to_binary(erlang:unique_integer([positive])),
    call(WS, MsgId, Action, Payload),
    MsgId.

result(WS, MsgId, Payload) ->
    send(WS, [3, MsgId, Payload]).

recv(WS) ->
    recv(WS, ?RECV_TIMEOUT).

%% Returns the decoded OCPP frame, or {close, Reason} | {error, timeout}.
recv(WS, Timeout) ->
    case rfc6455_client:recv(WS, Timeout) of
        {ok, Payload} -> json:decode(Payload);
        Other -> Other
    end.

assert_no_frame(WS, Timeout) ->
    case rfc6455_client:recv(WS, Timeout) of
        {error, timeout} -> ok;
        Other -> ct:fail({unexpected_frame, Other})
    end.

%% Returns the close frame's status code.
assert_closed(WS, Timeout) ->
    case rfc6455_client:recv(WS, Timeout) of
        {close, {Code, _}} -> Code;
        Other -> ct:fail({expected_close, Other})
    end.

is_open(WS) ->
    is_process_alive(WS).

%% -------------------------------------------------------------------
%% CSMS worker side
%% -------------------------------------------------------------------

channel(Config) ->
    channel(Config, 0).

channel(Config, Node) ->
    rabbit_ct_client_helpers:open_channel(Config, Node).

%% A short lived connection, so that helpers keep working after a node restart.
with_channel(Config, Fun) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    try Fun(Ch)
    after catch amqp_connection:close(Conn)
    end.

declare_worker_queue(Config, QName, BindingKey) ->
    declare_worker_queue(Config, QName, BindingKey, []).

declare_worker_queue(Config, QName, BindingKey, Args) ->
    X = exchange(Config),
    with_channel(
      Config,
      fun(Ch) ->
              #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                                                              durable = true,
                                                                              arguments = Args}),
              #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName,
                                                                        exchange = X,
                                                                        routing_key = BindingKey}),
              ok
      end).

delete_queue(Config, QName) ->
    with_channel(Config, fun(Ch) ->
                                 #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
                                 ok
                         end).

publish_to_cp(Config, ClientId, Frame) ->
    publish_to_cp(Config, exchange(Config), ClientId, Frame).

publish_to_cp(Config, Exchange, ClientId, Frame) ->
    with_channel(
      Config,
      fun(Ch) ->
              amqp_channel:call(Ch, #'confirm.select'{}),
              ok = amqp_channel:call(Ch, #'basic.publish'{exchange = Exchange, routing_key = ClientId},
                                     #amqp_msg{payload = iolist_to_binary(json:encode(Frame))}),
              true = amqp_channel:wait_for_confirms(Ch, 5000),
              ok
      end).

%% Returns {RoutingKey, #'P_basic'{}, DecodedFrame} or empty.
get_message(Config, QName) ->
    case with_channel(Config, fun(Ch) -> amqp_channel:call(Ch, #'basic.get'{queue = QName, no_ack = true}) end) of
        {#'basic.get_ok'{routing_key = RK}, #amqp_msg{props = Props, payload = Payload}} ->
            {RK, Props, json:decode(Payload)};
        #'basic.get_empty'{} ->
            empty
    end.

get_messages(Config, QName) ->
    case get_message(Config, QName) of
        empty -> [];
        Msg -> [Msg | get_messages(Config, QName)]
    end.

message_count(Config, QName) ->
    #'queue.declare_ok'{message_count = N} =
        with_channel(Config, fun(Ch) ->
                                     amqp_channel:call(Ch, #'queue.declare'{queue = QName, passive = true})
                             end),
    N.

cp_queue_pid(Config, ClientId) ->
    QName = rabbit_misc:r(<<"/">>, queue, <<"ocpp.", ClientId/binary>>),
    {ok, Q} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [QName]),
    rabbit_ct_broker_helpers:rpc(Config, 0, amqqueue, get_pid, [Q]).

connection_pids(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_web_ocpp_app, list_connections, []).

wait_for_connections(Config, N) ->
    rabbit_ct_helpers:await_condition(
      fun() -> length(connection_pids(Config)) =:= N end, 10000).

%% Remove all queues and bindings that tests create, so that routing in one
%% test cannot be affected by another.
purge_bindings(Config) ->
    Qs = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, list, []),
    [begin
         QName = amqqueue:get_name(Q),
         rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, delete_with,
                                      [QName, false, false, <<"tests">>])
     end || Q <- Qs],
    ok.
