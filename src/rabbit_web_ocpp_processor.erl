%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2025 VAMPIRE BYTE SRL. All Rights Reserved.
%%
-module(rabbit_web_ocpp_processor).

-feature(maybe_expr, enable).

-export([info/2,
         init/8,
         terminate/3,
         handle_info/2,
         handle_down/2,
         handle_text_frame/2,
         duplicate_id_kicked/1,
         throttled/1,
         connected_at/1,
         format_status/1,
         proto_version_tuple/1,
         truncate/1
        ]).

-export_type([state/0,
              send_fun/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include_lib("rabbit/include/mc.hrl").
-include("rabbit_web_ocpp.hrl").

%% --- Constants ---
-define(MAX_PERMISSION_CACHE_SIZE, 12).
-define(CONSUMER_TAG_PREFIX, <<"ocpp.ctag-">>).
-define(DUPLICATE_ID_KICK_TIMEOUT_MS, 3000). %% Wait for a kicked duplicate client ID to terminate.
-define(MAX_ACTION_BYTES, 239). %% Room left for the Action string segment of the routing key
%% OCPP-J limits message IDs to 36 characters.
-define(MAX_MSG_ID_BYTES, 36).
%% Charge points reply with the message IDs of the CSMS, which do not have
%% to be that short. Only bound them by what fits a routing key.
-define(MAX_RESPONSE_MSG_ID_BYTES, 255).
%% Connector IDs shown as client properties in the management UI. Bounds
%% the properties a charge point can create.
-define(MAX_CONNECTOR_ID, 64).
-define(MAX_PROPERTY_VALUE_BYTES, 255).
%% OCPP 1.6 BootNotification.req fields shown as client properties.
-define(BOOT_NOTIFICATION_PROPS,
        [<<"chargePointVendor">>, <<"chargePointModel">>, <<"chargePointSerialNumber">>,
         <<"chargeBoxSerialNumber">>, <<"firmwareVersion">>, <<"iccid">>, <<"imsi">>,
         <<"meterType">>, <<"meterSerialNumber">>]).
%% Bytes of client input included in a log line.
-define(MAX_LOGGED_BYTES, 256).
%% How long a terminating connection waits for the queues to confirm the
%% offline status notification.
-define(OFFLINE_STATUS_CONFIRM_TIMEOUT_MS, 2000).
-define(OFFLINE_STATUS_CORRELATION, 1).

%% --- Types ---

%% Function provided by the WebSocket handler to send data (Erlang term) to the client.
%% The function itself should handle JSON encoding.
-type send_fun() :: fun((Data :: term()) -> ok | {error, any()}).

-record(auth_state,
        {user :: #user{},
         authz_ctx :: #{binary() := binary()}
        }).

-record(cfg, {
        socket :: rabbit_net:socket(),
        send_fun :: send_fun(),
        vhost :: rabbit_types:vhost(),
        client_id :: binary(), % Charge Point ID
        proto_ver :: ocpp_protocol_version_atom(),
        user :: #user{}, % Authenticated user details
        exchange :: rabbit_exchange:name(), % Exchange to publish *to* and bind *from*
        queue_name :: rabbit_amqqueue:name(), % The single queue for this client_id
        prefetch :: non_neg_integer(),
        call_timeout :: pos_integer(),
        conn_name :: option(binary()), % For logging/tracing
        user_prop :: user_property(),
        ip_addr :: inet:ip_address(),
        port :: inet:port_number(),
        peer_ip_addr :: inet:ip_address(),
        peer_port :: inet:port_number(),
        trace_state :: rabbit_trace:state(),
        consumer_tag :: binary(),
        connected_at :: pos_integer()
}).

%% A CALL from the CSMS to the charge point. It stays unacknowledged in the
%% charge point queue until the charge point answers it or it times out, so
%% that it is redelivered if the connection drops in the meantime.
-record(call, {
        msg_id :: binary(),
        action :: binary(),
        qname :: rabbit_amqqueue:name(),
        qmsg_id :: non_neg_integer(),
        payload :: binary(),
        timer :: option(reference())
}).

-record(state, {
    cfg :: #cfg{},
    queue_states :: rabbit_queue_type:state(),
    auth_state :: #auth_state{},
    %% Outbound WebSocket frames accumulated while handling a single event,
    %% drained and returned to cowboy in one go. Stored in reverse order.
    pending_frames = [] :: [cowboy_websocket:frame()],
    %% OCPP-J: one CALL in each direction may be outstanding at a time.
    outstanding_call :: option(#call{}),
    held_calls = queue:new() :: queue:queue(#call{}),
    %% Whether to announce the charge point offline when the connection ends.
    %% Not when another connection of the same charge point took over.
    publish_offline = true :: boolean(),
    %% Quorum queues that asked publishers to slow down.
    blocked_queues = [] :: [term()]
}).

-opaque state() :: #state{}.

%% --- Public API ---
-spec init(Vhost :: rabbit_types:vhost(),
           ClientId :: binary(),
           ProtoVer :: ocpp_protocol_version_atom(),
           RawSocket :: rabbit_net:socket(),
           ConnectionName :: binary(),
           User :: #user{},
           AuthzCtx :: #{binary() := binary()},
           SendFun :: send_fun()) ->
    {ok, state()} | {error, term()}.
init(Vhost, ClientId, ProtoVer, Socket, ConnName0, User, AuthzCtx, SendFun) ->
    %% Check whether peer closed the connection.
    %% For example, this can happen when connection was blocked because of resource
    %% alarm and client therefore disconnected.
    case rabbit_net:socket_ends(Socket, inbound) of
        {ok, SocketEnds} ->
            process_connect(Vhost, ClientId, ProtoVer, Socket, ConnName0, User, AuthzCtx, SendFun, SocketEnds);
        {error, Reason} ->
            {error, {socket_ends, Reason}}
    end.

process_connect(Vhost, ClientId, ProtoVer, Socket, ConnName0, User, AuthzCtx, SendFun,
                {PeerIp, PeerPort, Ip, Port}) ->
    maybe
        ok = register_client_id(Vhost, ClientId),
        rabbit_core_metrics:auth_attempt_succeeded(PeerIp, ClientId, ocpp),

        ExchangeName = rabbit_misc:r(Vhost, exchange, rabbit_web_ocpp_util:exchange()),
        QueueName = queue_name(Vhost, ClientId),
        {TraceState, ConnName} = init_trace(Vhost, ConnName0),
        ConnectedAt = os:system_time(millisecond),

        AuthState = #auth_state{user = User, authz_ctx = AuthzCtx},
        Cfg = #cfg{socket = Socket,
                   ip_addr = Ip,
                   port = Port,
                   peer_ip_addr = PeerIp,
                   peer_port = PeerPort,
                   send_fun = SendFun,
                   vhost = Vhost,
                   client_id = ClientId,
                   proto_ver = ProtoVer,
                   user = User,
                   user_prop = [],
                   exchange = ExchangeName,
                   queue_name = QueueName,
                   prefetch = rabbit_web_ocpp_util:get_env(prefetch_count),
                   call_timeout = rabbit_web_ocpp_util:get_env(call_timeout),
                   conn_name = ConnName,
                   trace_state = TraceState,
                   consumer_tag = consumer_tag(ConnectedAt),
                   connected_at = ConnectedAt},
        InitialState = #state{cfg = Cfg,
                              queue_states = rabbit_queue_type:init(),
                              auth_state = AuthState},

        ok ?= rabbit_web_ocpp_util:ensure_exchange(ExchangeName, User#user.username),
        {ok, StateAfterQueue} ?= ensure_queue_and_binding(InitialState),
        {ok, FinalState} ?= consume_from_queue(StateAfterQueue),

        %% Register the connection and let the handler emit connection_created
        %% only now that the connection is fully established (consume_from_queue succeeded).
        ok = rabbit_networking:register_non_amqp_connection(self()),
        ok = pg:join(?PG_SCOPE, ?CONNECTIONS_GROUP, self()),
        rabbit_global_counters:consumer_created(ProtoVer),
        self() ! connection_created,

        ?LOG_INFO("OCPP connection ~ts established for ClientId ~ts on vhost ~ts",
                  [ConnName0, ClientId, Vhost]),
        {ok, FinalState}
    else
        {error, Reason} ->
            ?LOG_ERROR("OCPP connection failed for ClientId ~ts: ~p", [ClientId, Reason]),
            {error, Reason}
    end.

-spec connected_at(state()) -> pos_integer().
connected_at(#state{cfg = #cfg{connected_at = ConnectedAt}}) ->
    ConnectedAt.

%% Another connection of the same charge point took over: the charge point
%% is online, so this connection must not announce it offline.
-spec duplicate_id_kicked(state()) -> state().
duplicate_id_kicked(State) ->
    State#state{publish_offline = false}.

%% Whether the connection must stop reading from the charge point because the
%% queues it publishes to cannot keep up (credit flow of classic queues,
%% quorum queues over their soft limit).
-spec throttled(state()) -> boolean().
throttled(#state{blocked_queues = BlockedQueues}) ->
    BlockedQueues =/= [] orelse credit_flow:blocked().

%% @doc Handles an incoming WebSocket text frame: decodes and validates the
%% OCPP message, then publishes it.
-spec handle_text_frame(binary(), state()) ->
    {ok, state(), cowboy_websocket:commands()} |
    {error, invalid_json | invalid_message | access_refused | term(), state()}.
handle_text_frame(Data, State = #state{cfg = #cfg{client_id = ClientId}}) ->
    try json:decode(Data) of
        Decoded ->
            case validate(Decoded) of
                {ok, Msg} ->
                    State1 = maybe_update_props_from_message(Msg, State),
                    handle_ocpp_message(Msg, Data, State1);
                {error, Reason} ->
                    ?LOG_WARNING("Web OCPP client ~ts sent an invalid OCPP message (~ts): ~ts",
                                 [ClientId, Reason, truncate(Data)]),
                    {error, invalid_message, State}
            end
    catch
        error:Reason ->
            ?LOG_WARNING("Web OCPP client ~ts sent invalid JSON (~0P): ~ts",
                         [ClientId, Reason, 5, truncate(Data)]),
            {error, invalid_json, State}
    end.

%% Validated message: {MessageType, MessageId, Action | undefined}
validate([Type, MsgId, Action, Payload])
  when Type =:= ?OCPP_MESSAGE_TYPE_CALL;
       Type =:= ?OCPP_MESSAGE_TYPE_SEND ->
    maybe
        ok ?= validate_msg_id(MsgId, ?MAX_MSG_ID_BYTES),
        ok ?= case is_binary(Action) andalso Action =/= <<>> of
                  true -> ok;
                  false -> {error, <<"action is not a non-empty string">>}
              end,
        ok ?= case is_map(Payload) of
                  true -> ok;
                  false -> {error, <<"payload is not an object">>}
              end,
        {ok, {Type, MsgId, Action, Payload}}
    end;
validate([?OCPP_MESSAGE_TYPE_CALLRESULT = Type, MsgId, Payload]) ->
    maybe
        ok ?= validate_msg_id(MsgId, ?MAX_RESPONSE_MSG_ID_BYTES),
        {ok, {Type, MsgId, undefined, Payload}}
    end;
validate([Type, MsgId, ErrorCode, _ErrorDescription, _ErrorDetails])
  when Type =:= ?OCPP_MESSAGE_TYPE_CALLERROR;
       Type =:= ?OCPP_MESSAGE_TYPE_CALLRESULTERROR ->
    maybe
        ok ?= validate_msg_id(MsgId, ?MAX_RESPONSE_MSG_ID_BYTES),
        ok ?= case is_binary(ErrorCode) of
                  true -> ok;
                  false -> {error, <<"error code is not a string">>}
              end,
        {ok, {Type, MsgId, undefined, undefined}}
    end;
validate(_) ->
    {error, <<"unknown message structure">>}.

validate_msg_id(MsgId, MaxBytes)
  when is_binary(MsgId), byte_size(MsgId) > 0, byte_size(MsgId) =< MaxBytes ->
    ok;
validate_msg_id(_, _) ->
    {error, <<"invalid message ID">>}.

handle_ocpp_message({Type, MsgId, Action0, _Payload}, Data,
                    State0 = #state{cfg = #cfg{client_id = ClientId, proto_ver = ProtoVer}}) ->
    rabbit_global_counters:messages_received(ProtoVer, 1),
    %% Answers to a CALL of the CSMS carry the action of that CALL, so that
    %% workers can bind to e.g. ocpp16.GetConfiguration.conf.
    {Action, State1} = case is_response(Type) of
                           true -> complete_call(MsgId, State0);
                           false -> {Action0, State0}
                       end,
    McOcpp = #ocpp_msg{msg_type = Type,
                       msg_id = MsgId,
                       action = Action,
                       payload = Data,
                       client_id = ClientId},
    case publish(McOcpp, #{flow => flow}, State1) of
        {ok, unroutable, State2} when Type =:= ?OCPP_MESSAGE_TYPE_CALL ->
            %% Nobody would answer. Do not let the charge point wait for a
            %% timeout (and maybe retry or reboot) [OCPP-J 4.2.3].
            Frame = [?OCPP_MESSAGE_TYPE_CALLERROR, MsgId, <<"NotImplemented">>,
                     <<"No handler for action ", Action/binary>>, #{}],
            drain_frames(buffer_frame({text, iolist_to_binary(json:encode(Frame))}, State2));
        {ok, _, State2} ->
            drain_frames(State2);
        {error, _, _} = Err ->
            Err
    end.

is_response(Type) ->
    Type =:= ?OCPP_MESSAGE_TYPE_CALLRESULT orelse
    Type =:= ?OCPP_MESSAGE_TYPE_CALLERROR orelse
    Type =:= ?OCPP_MESSAGE_TYPE_CALLRESULTERROR.

%% Publishes a message of the charge point to the exchange.
-spec publish(#ocpp_msg{}, rabbit_queue_type:delivery_options(), state()) ->
    {ok, {routed, [rabbit_amqqueue:name()]} | unroutable, state()} | {error, term(), state()}.
publish(McOcpp = #ocpp_msg{}, Options,
        State = #state{cfg = #cfg{exchange = ExchangeName = #resource{name = ExchangeNameBin},
                                  client_id = ClientId,
                                  trace_state = TraceState,
                                  conn_name = ConnName},
                       auth_state = #auth_state{user = #user{username = Username}}}) ->
    RoutingKey = generate_routing_key(McOcpp, State),
    case check_publish_permitted(ExchangeName, RoutingKey, State) of
        ok ->
            case rabbit_exchange:lookup(ExchangeName) of
                {ok, Exchange} ->
                    Anns = #{?ANN_EXCHANGE => ExchangeNameBin,
                             ?ANN_ROUTING_KEYS => [RoutingKey]},
                    McMsg = mc:init(mc_ocpp, McOcpp, Anns, #{}),
                    case rabbit_exchange:route(Exchange, McMsg, #{}) of
                        [] ->
                            ?LOG_WARNING("OCPP message from ClientId ~ts routed to 0 queues. "
                                         "Exchange: ~ts, routing key: ~ts",
                                         [ClientId, rabbit_misc:rs(ExchangeName), RoutingKey]),
                            {ok, unroutable, State};
                        QNames when is_list(QNames) ->
                            rabbit_trace:tap_in(McMsg, QNames, ConnName, Username, TraceState),
                            case deliver_to_queues(McMsg, QNames, Options, State) of
                                {ok, Targets, State1} -> {ok, {routed, Targets}, State1};
                                {error, _, _} = Err -> Err
                            end;
                        {error, Reason} ->
                            ?LOG_ERROR("OCPP failed to route message via exchange ~ts: ~p",
                                       [rabbit_misc:rs(ExchangeName), Reason]),
                            {error, publish_failed, State}
                    end;
                {error, not_found} ->
                    ?LOG_ERROR("Exchange ~ts does not exist for ClientId ~ts",
                               [rabbit_misc:rs(ExchangeName), ClientId]),
                    {error, exchange_not_found, State}
            end;
        {error, access_refused} ->
            ?LOG_WARNING("OCPP publish refused for ClientId ~ts to exchange ~ts",
                         [ClientId, rabbit_misc:rs(ExchangeName)]),
            {error, access_refused, State}
    end.

%% The queue type state must be kept: quorum queues track publishers by
%% sequence number, so a fresh state for every publish makes them drop all
%% but the first message of a connection as duplicates.
deliver_to_queues(Message, RoutedToQNames, Options,
                  State0 = #state{queue_states = QStates0,
                                  cfg = #cfg{proto_ver = ProtoVer}}) ->
    Qs0 = lookup_queue_targets(drop_local(RoutedToQNames, State0)),
    Qs = rabbit_amqqueue:prepend_extra_bcc(Qs0),
    case rabbit_queue_type:deliver(Qs, Message, Options, QStates0) of
        {ok, QStates, Actions} ->
            rabbit_global_counters:messages_routed(ProtoVer, length(Qs)),
            Targets = [amqqueue:get_name(target_queue(Q)) || Q <- Qs],
            try handle_queue_actions(Actions, State0#state{queue_states = QStates}) of
                State -> {ok, Targets, State}
            catch throw:consuming_queue_down ->
                    {error, consuming_queue_down, State0}
            end;
        {error, Reason} ->
            ?LOG_ERROR("OCPP failed to deliver message to queues ~p: ~p",
                       [[amqqueue:get_name(Q) || Q <- Qs0], Reason]),
            {error, publish_failed, State0}
    end.

target_queue({Q, _RouteInfos}) -> Q;
target_queue(Q) -> Q.

%% OCPP Messages MUST NOT be forwarded to a connection with a ClientID
%% equal to the ClientID of the publishing connection.
drop_local(QNames, #state{cfg = #cfg{queue_name = OwnQueueName}}) ->
    lists:filter(fun(QName) -> QName =/= OwnQueueName end, QNames).

lookup_queue_targets(QNames) ->
    case erlang:function_exported(rabbit_db_queue, get_targets, 1) of
        true ->
            %% RabbitMQ v4.2.x
            rabbit_db_queue:get_targets(QNames);
        false ->
            %% RabbitMQ <= v4.1.x
            rabbit_amqqueue:lookup_many(QNames)
    end.

-spec register_client_id(rabbit_types:vhost(), client_id()) -> ok.
register_client_id(Vhost, ClientId)
  when is_binary(Vhost), is_binary(ClientId) ->
    PgGroup = {Vhost, ClientId},
    %% "Last connection wins" without any per-connect cluster-wide calls,
    %% relying on the shared pg scope replicating memberships to all nodes:
    %%
    %% The monitor is installed *before* joining, so the event stream is
    %% complete: a member never sees joins that happened before its own
    %% monitor. Members that observe each other's join (e.g. after a network
    %% partition healed and pg re-synced memberships) exchange their connection
    %% times and only the older one closes (see the corresponding
    %% websocket_info clauses in rabbit_web_ocpp_handler).
    {_Ref, Members} = pg:monitor(?PG_SCOPE, PgGroup),
    ok = pg:join(?PG_SCOPE, PgGroup, self()),
    %% Disconnect the already-known members and wait for them to die, so that
    %% the exclusive consumer is released by the time this connection consumes.
    %% Kicked connections do not announce the charge point offline, so nothing
    %% they publish can overtake the online status of this connection. A
    %% connection that does not terminate in time (e.g. because it is stuck
    %% writing to a dead socket) is killed: otherwise it would keep the
    %% consumer and this connection would fail.
    lists:foreach(fun(Pid) ->
                          MRef = erlang:monitor(process, Pid),
                          gen_server:cast(Pid, {duplicate_id}),
                          receive
                              {'DOWN', MRef, process, Pid, _} -> ok
                          after ?DUPLICATE_ID_KICK_TIMEOUT_MS ->
                              ?LOG_WARNING("Web OCPP connection ~p with duplicate "
                                           "client ID did not terminate in ~bms, killing it",
                                           [Pid, ?DUPLICATE_ID_KICK_TIMEOUT_MS]),
                              exit(Pid, kill),
                              receive
                                  {'DOWN', MRef, process, Pid, _} -> ok
                              after ?DUPLICATE_ID_KICK_TIMEOUT_MS ->
                                  erlang:demonitor(MRef, [flush])
                              end
                          end
                  end, Members -- [self()]).

-spec consumer_tag(pos_integer()) -> binary().
consumer_tag(ConnectedAt) ->
    %% The unique integer disambiguates same-millisecond reconnects.
    Unique = erlang:unique_integer([positive]),
    <<?CONSUMER_TAG_PREFIX/binary, (integer_to_binary(ConnectedAt))/binary,
      ".", (integer_to_binary(Unique))/binary>>.

%% Handle internal messages, queue events, etc.
-spec handle_info(term(), state()) ->
    {ok, state(), cowboy_websocket:commands()} | {stop, term(), state()}.
handle_info({queue_event, QName, Evt}, State0 = #state{queue_states = QStates0}) ->
    try
        case rabbit_queue_type:handle_event(QName, Evt, QStates0) of
            {ok, QStates, Actions} ->
                State1 = State0#state{queue_states = QStates},
                drain_frames(handle_queue_actions(Actions, State1));
            {eol, Actions} -> % Queue deleted
                State1 = handle_queue_actions(Actions, State0),
                QStates = rabbit_queue_type:remove(QName, QStates0),
                drain_frames(handle_queue_down(QName, State1#state{queue_states = QStates}));
            {protocol_error, _, _, _} = Error ->
                {stop, {shutdown, Error}, State0}
        end
    catch throw:consuming_queue_down ->
              {stop, consuming_queue_down, State0}
    end;
handle_info({ocpp_call_timeout, MsgId},
            State0 = #state{outstanding_call = #call{msg_id = MsgId, action = Action},
                            cfg = #cfg{client_id = ClientId, call_timeout = Timeout}}) ->
    ?LOG_WARNING("OCPP charge point ~ts did not answer ~ts CALL ~ts within ~bms",
                 [ClientId, Action, truncate(MsgId), Timeout]),
    {_, State} = complete_call(MsgId, State0),
    drain_frames(State);
handle_info({ocpp_call_timeout, _MsgId}, State) ->
    %% Answered in the meantime.
    drain_frames(State);
handle_info(Msg, State = #state{cfg = #cfg{client_id = ClientId}}) ->
    ?LOG_WARNING("OCPP processor for ~ts received unknown message: ~p", [ClientId, Msg]),
    drain_frames(State).

%% A queue process this connection publishes to or consumes from went down.
-spec handle_down(term(), state()) ->
    {ok, state(), cowboy_websocket:commands()} | {stop, term(), state()}.
handle_down({{'DOWN', QName}, _MRef, process, QPid, Reason},
            State0 = #state{queue_states = QStates0}) ->
    credit_flow:peer_down(QPid),
    try
        case rabbit_queue_type:handle_down(QPid, QName, Reason, QStates0) of
            {ok, QStates, Actions} ->
                drain_frames(handle_queue_actions(Actions, State0#state{queue_states = QStates}));
            {eol, QStates1, QRef} ->
                QStates = rabbit_queue_type:remove(QRef, QStates1),
                drain_frames(handle_queue_down(QRef, State0#state{queue_states = QStates}));
            {error, _} = Err ->
                ?LOG_WARNING("OCPP failed to handle down queue ~ts: ~p",
                             [rabbit_misc:rs(QName), Err]),
                drain_frames(State0)
        end
    catch throw:consuming_queue_down ->
              {stop, consuming_queue_down, State0}
    end.

%% Without its consumer the charge point would look online but never receive
%% another command. Make it reconnect.
handle_queue_down(QName, #state{cfg = #cfg{queue_name = QName, client_id = ClientId}}) ->
    ?LOG_WARNING("Terminating Web OCPP connection of ~ts because its queue ~ts is down",
                 [ClientId, rabbit_misc:rs(QName)]),
    throw(consuming_queue_down);
handle_queue_down(_QName, State) ->
    State.

%% Drain accumulated outbound frames and return them to the caller along with
%% a state that no longer holds them.
drain_frames(State = #state{pending_frames = Frames}) ->
    {ok, State#state{pending_frames = []}, lists:reverse(Frames)}.

%% Terminate the processor
-spec terminate(any(), rabbit_event:event_props(), state()) -> ok.
terminate(Reason, Infos, State = #state{queue_states = QStates,
                                        publish_offline = PublishOffline,
                                        cfg = #cfg{client_id = ClientId,
                                                   proto_ver = ProtoVer}}) ->
    rabbit_global_counters:consumer_deleted(ProtoVer),
    ?LOG_INFO("OCPP processor terminating. ClientId: ~ts, Reason: ~p", [ClientId, Reason]),
    %% Tell the backends the charge point went offline with one final
    %% synthetic StatusNotification, unless the charge point reconnected.
    case PublishOffline of
        true -> publish_offline_status(State);
        false -> ok
    end,
    %% Unanswered CALLs are requeued when the consumer goes away.
    ok = rabbit_queue_type:close(QStates),
    rabbit_core_metrics:connection_closed(self()),
    rabbit_event:notify(connection_closed, Infos),
    ok = rabbit_networking:unregister_non_amqp_connection(self()),
    %% Note: We typically DO NOT delete the durable queue on disconnect for OCPP.
    %% It should persist messages while the CP is offline.
    ok.

%% Publishes a synthetic StatusNotification CALL marking the whole
%% charge point (connectorId 0) Unavailable/Offline, using the
%% OCPP version schema the CP was originally connected with.
-spec publish_offline_status(state()) -> ok.
publish_offline_status(State = #state{cfg = #cfg{client_id = ClientId,
                                                 proto_ver = ProtoVer}}) ->
    MsgId = list_to_binary(rabbit_guid:to_string(rabbit_guid:gen())),
    Timestamp = list_to_binary(
                  calendar:system_time_to_rfc3339(os:system_time(second),
                                                  [{offset, "Z"}])),
    Payload = case proto_version_tuple(ProtoVer) of
                  {1, _} -> % OCPP 1.x StatusNotification.req
                      #{<<"connectorId">> => 0,
                        <<"status">> => <<"Unavailable">>,
                        <<"errorCode">> => <<"NoError">>,
                        <<"timestamp">> => Timestamp,
                        <<"vendorId">> => <<"rabbitmq">>,
                        <<"vendorErrorCode">> => <<"Offline">>};
                  _ -> % OCPP 2.x StatusNotificationRequest
                      #{<<"timestamp">> => Timestamp,
                        <<"connectorStatus">> => <<"Unavailable">>,
                        <<"evseId">> => 0,
                        <<"connectorId">> => 0,
                        <<"customData">> => #{<<"vendorId">> => <<"rabbitmq">>,
                                              <<"vendorErrorCode">> => <<"Offline">>}}
              end,
    Frame = iolist_to_binary(json:encode([?OCPP_MESSAGE_TYPE_CALL, MsgId,
                                          <<"StatusNotification">>, Payload])),
    McOcpp = #ocpp_msg{msg_type = ?OCPP_MESSAGE_TYPE_CALL,
                       msg_id = MsgId,
                       action = <<"StatusNotification">>,
                       payload = Frame,
                       client_id = ClientId},
    %% The connection may be closing because the broker shuts down. Wait for
    %% the queues to confirm the message, so that it is not lost while the
    %% message stores stop.
    try publish(McOcpp, #{correlation => ?OFFLINE_STATUS_CORRELATION}, State) of
        {ok, {routed, Targets}, State1} ->
            Deadline = erlang:monotonic_time(millisecond) + ?OFFLINE_STATUS_CONFIRM_TIMEOUT_MS,
            case await_confirms(Targets, Deadline, State1) of
                ok ->
                    ok;
                timeout ->
                    ?LOG_WARNING("OCPP offline StatusNotification for ClientId ~ts "
                                 "was not confirmed in time", [ClientId])
            end;
        {ok, unroutable, _} ->
            ok;
        {error, Err, _} ->
            ?LOG_WARNING("OCPP offline StatusNotification for ClientId ~ts failed: ~p",
                         [ClientId, Err])
    catch Class:Err ->
        ?LOG_WARNING("OCPP offline StatusNotification for ClientId ~ts failed: ~p:~p",
                     [ClientId, Class, Err])
    end.

await_confirms([], _Deadline, _State) ->
    ok;
await_confirms(Pending, Deadline, State = #state{queue_states = QStates0}) ->
    Timeout = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {'$gen_cast', {queue_event, QName, Evt}} ->
            case rabbit_queue_type:handle_event(QName, Evt, QStates0) of
                {ok, QStates, Actions} ->
                    Done = [Q || {Settlement, Q, Corrs} <- Actions,
                                 Settlement =:= settled orelse Settlement =:= rejected,
                                 lists:member(?OFFLINE_STATUS_CORRELATION, Corrs)],
                    await_confirms(Pending -- Done, Deadline,
                                   State#state{queue_states = QStates});
                _ ->
                    await_confirms(lists:delete(QName, Pending), Deadline, State)
            end;
        {{'DOWN', QName}, _MRef, process, _Pid, _Reason} ->
            await_confirms(lists:delete(QName, Pending), Deadline, State)
    after Timeout ->
              timeout
    end.

%% --- Internal Functions ---

%% @doc Generates a structured routing key in the format: protocolver.actionname.req/conf/error
%% Examples: "ocpp16.BootNotification.req", "ocpp16.Heartbeat.conf", "ocpp201.StatusNotification.req"
-spec generate_routing_key(#ocpp_msg{}, state()) -> binary().
generate_routing_key(#ocpp_msg{msg_type = MsgType, action = Action},
                     #state{cfg = #cfg{proto_ver = ProtoVer}}) ->
    ProtoVerBin = atom_to_binary(ProtoVer, utf8),

    %% Responses to unknown (e.g. timed out) CALLs have no action. The Action
    %% is client input with no restrictions in OCPP so we need to clean it up
    %% for routing key usage.
    ActionBin = case Action of
        undefined -> <<"response">>;
        ActionName when is_binary(ActionName) ->
            case re:replace(ActionName, "[^A-Za-z0-9]", "", [global, {return, binary}]) of
                <<>> -> <<"unknown">>;
                <<Trimmed:?MAX_ACTION_BYTES/binary, _/binary>> -> Trimmed;
                Clean -> Clean
            end
    end,

    %% Determine message direction (req/conf/error)
    MsgTypeBin = case MsgType of
        ?OCPP_MESSAGE_TYPE_CALL -> <<"req">>;      % Request from charge point
        ?OCPP_MESSAGE_TYPE_SEND -> <<"req">>;      % Request in OCPP 2.1
        ?OCPP_MESSAGE_TYPE_CALLRESULT -> <<"conf">>; % Response/confirmation
        ?OCPP_MESSAGE_TYPE_CALLERROR -> <<"error">>; % Error response
        ?OCPP_MESSAGE_TYPE_CALLRESULTERROR -> <<"error">> % Error in OCPP 2.1
    end,

    <<ProtoVerBin/binary, ".", ActionBin/binary, ".", MsgTypeBin/binary>>.

%% Handle actions resulting from queue events (deliveries, etc.)
handle_queue_actions([], State) -> State;
handle_queue_actions([{deliver, _ConsumerTag, _AckRequired, Msgs} | Rest], State) ->
    State1 = lists:foldl(fun deliver_to_client/2, State, Msgs),
    handle_queue_actions(Rest, State1);
handle_queue_actions([{queue_down, QName} | Rest], State) ->
    handle_queue_actions(Rest, handle_queue_down(QName, State));
handle_queue_actions([{block, QName} | Rest], State = #state{blocked_queues = Blocked}) ->
    handle_queue_actions(Rest, State#state{blocked_queues = lists:usort([QName | Blocked])});
handle_queue_actions([{unblock, QName} | Rest], State = #state{blocked_queues = Blocked}) ->
    handle_queue_actions(Rest, State#state{blocked_queues = lists:delete(QName, Blocked)});
handle_queue_actions([Action | Rest], State) ->
    ?LOG_DEBUG("OCPP unhandled queue action: ~p", [Action]),
    handle_queue_actions(Rest, State).

%% Deliver a message from the charge point queue to the charge point.
%% Outbound frames are buffered into State#state.pending_frames and returned
%% to cowboy by the caller.
deliver_to_client({QName, QPid, QMsgId, Redelivered, Mc} = Delivery,
                  State0 = #state{cfg = #cfg{client_id = ClientId,
                                             trace_state = TraceState,
                                             conn_name = ConnName},
                                  auth_state = #auth_state{user = User}}) ->
    State = try
                #ocpp_msg{payload = Payload0} = mc:protocol_state(mc:convert(mc_ocpp, Mc, #{})),
                Payload = iolist_to_binary(Payload0),
                rabbit_trace:tap_out(Delivery, ConnName, User#user.username, TraceState),
                count_delivery(QName, Redelivered, State0),
                case classify_outbound(Payload) of
                    {call, MsgId, Action} ->
                        %% Sent once the outstanding CALL, if any, completed.
                        Call = #call{msg_id = MsgId, action = Action, qname = QName,
                                     qmsg_id = QMsgId, payload = Payload},
                        maybe_send_next_call(
                          State0#state{held_calls = queue:in(Call, State0#state.held_calls)});
                    other ->
                        settle(QName, complete, QMsgId, buffer_frame({text, Payload}, State0))
                end
            catch throw:consuming_queue_down = Thrown ->
                      throw(Thrown);
                  Class:Reason:Stacktrace ->
                      ?LOG_ERROR("OCPP error delivering message to ~ts: ~p:~p~n~p",
                                 [ClientId, Class, Reason, Stacktrace]),
                      settle(QName, discard, QMsgId, State0)
            end,
    ok = maybe_notify_sent(QName, QPid, State),
    State.

%% Only CALLs need to be told apart: they wait for the answer of the charge
%% point. Anything else (answers to the charge point's own CALLs) is sent
%% right away.
classify_outbound(Payload) ->
    try json:decode(Payload) of
        [Type, MsgId, Action | _]
          when (Type =:= ?OCPP_MESSAGE_TYPE_CALL orelse Type =:= ?OCPP_MESSAGE_TYPE_SEND),
               is_binary(MsgId), is_binary(Action) ->
            case Type of
                ?OCPP_MESSAGE_TYPE_CALL -> {call, MsgId, Action};
                %% OCPP 2.1 SEND is not answered.
                ?OCPP_MESSAGE_TYPE_SEND -> other
            end;
        _ ->
            other
    catch error:_ ->
              other
    end.

maybe_send_next_call(State = #state{outstanding_call = undefined,
                                    held_calls = Held0,
                                    cfg = #cfg{call_timeout = Timeout}}) ->
    case queue:out(Held0) of
        {{value, Call = #call{msg_id = MsgId, payload = Payload}}, Held} ->
            TRef = erlang:send_after(Timeout, self(), {ocpp_call_timeout, MsgId}),
            buffer_frame({text, Payload},
                         State#state{outstanding_call = Call#call{timer = TRef},
                                     held_calls = Held});
        {empty, _} ->
            State
    end;
maybe_send_next_call(State) ->
    State.

%% The charge point answered (or did not answer in time) the outstanding CALL.
%% Returns the action of the CALL for routing the answer.
complete_call(MsgId, State0 = #state{outstanding_call = #call{msg_id = MsgId,
                                                              action = Action,
                                                              qname = QName,
                                                              qmsg_id = QMsgId,
                                                              timer = TRef}}) ->
    _ = erlang:cancel_timer(TRef),
    State = settle(QName, complete, QMsgId, State0#state{outstanding_call = undefined}),
    {Action, maybe_send_next_call(State)};
complete_call(_MsgId, State) ->
    {undefined, State}.

count_delivery(QName, Redelivered, #state{queue_states = QStates,
                                          cfg = #cfg{proto_ver = ProtoVer}}) ->
    case rabbit_queue_type:module(QName, QStates) of
        {ok, QType} ->
            rabbit_global_counters:messages_delivered(ProtoVer, QType, 1),
            rabbit_global_counters:messages_delivered_consume_manual_ack(ProtoVer, QType, 1),
            case Redelivered of
                true -> rabbit_global_counters:messages_redelivered(ProtoVer, QType, 1);
                false -> ok
            end;
        _ ->
            ok
    end.

settle(QName, Op, QMsgId, State = #state{queue_states = QStates0,
                                         cfg = #cfg{consumer_tag = ConsumerTag,
                                                    proto_ver = ProtoVer}}) ->
    case Op of
        complete ->
            case rabbit_queue_type:module(QName, QStates0) of
                {ok, QType} -> rabbit_global_counters:messages_acknowledged(ProtoVer, QType, 1);
                _ -> ok
            end;
        _ ->
            ok
    end,
    case rabbit_queue_type:settle(QName, Op, ConsumerTag, [QMsgId], QStates0) of
        {ok, QStates, Actions} ->
            handle_queue_actions(Actions, State#state{queue_states = QStates});
        {protocol_error, _Type, Fmt, Args} ->
            ?LOG_WARNING("OCPP failed to settle message of ~ts: " ++ Fmt,
                         [rabbit_misc:rs(QName) | Args]),
            State
    end.

buffer_frame(Frame, State = #state{pending_frames = Frames}) ->
    State#state{pending_frames = [Frame | Frames]}.

maybe_notify_sent(QName, QPid, #state{queue_states = QStates}) ->
    case rabbit_queue_type:module(QName, QStates) of
        {ok, rabbit_classic_queue} ->
            rabbit_amqqueue:notify_sent(QPid, self());
        _ ->
            ok
    end.

%% Ensure the queue exists and is bound
-spec ensure_queue_and_binding(state()) -> {ok, state()} | {error, term()}.
ensure_queue_and_binding(State = #state{cfg = #cfg{queue_name = QName,
                                                   vhost = Vhost},
                                        auth_state = #auth_state{user = User = #user{username = Username},
                                                                 authz_ctx = AuthzCtx}}) ->
    case check_resource_access(User, QName, configure, AuthzCtx) of
        ok ->
            case ensure_queue(QName, queue_args(), Vhost, Username) of
                ok ->
                    bind_queue(State);
                {error, _} = Error ->
                    Error
            end;
        {error, access_refused} ->
            ?LOG_WARNING("OCPP configure permission refused for queue ~ts", [rabbit_misc:rs(QName)]),
            {error, {queue_declare_failed, access_refused}}
    end.

%% Arguments of newly declared charge point queues. Existing queues keep
%% theirs: change those with a policy.
-spec queue_args() -> rabbit_framing:amqp_table().
queue_args() ->
    Type = case rabbit_web_ocpp_util:get_env(queue_type) of
               quorum -> <<"quorum">>;
               classic -> <<"classic">>
           end,
    [{<<"x-queue-type">>, longstr, Type}] ++
    [{Arg, long, Val}
     || {Arg, Key} <- [{<<"x-message-ttl">>, queue_message_ttl},
                       {<<"x-expires">>, queue_expires}],
        Val <- [rabbit_web_ocpp_util:get_env(Key)],
        is_integer(Val)].

%% Declaring a classic queue always starts a queue process, which then writes
%% to the metadata store even when the queue turns out to already exist. Under
%% a connection storm that write is what times out, so look the queue up first
%% and only declare when it is missing, the way rabbit_mqtt_processor does.
-spec ensure_queue(rabbit_amqqueue:name(), rabbit_framing:amqp_table(),
                   rabbit_types:vhost(), rabbit_types:username()) ->
    ok | {error, term()}.
ensure_queue(QName, QArgs, Vhost, Username) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, _Q} ->
            ok;
        {error, not_found} ->
            declare_queue(QName, QArgs, Vhost, Username)
    end.

-spec declare_queue(rabbit_amqqueue:name(), rabbit_framing:amqp_table(),
                    rabbit_types:vhost(), rabbit_types:username()) ->
    ok | {error, term()}.
declare_queue(QName, QArgs, Vhost, Username) ->
    QType = rabbit_amqqueue:get_queue_type(QArgs),
    Owner = none,
    Durable = true,
    AutoDelete = false,
    Q0 = amqqueue:new(QName, none, Durable, AutoDelete, Owner, QArgs, Vhost,
                      #{user => Username}, QType),
    case rabbit_queue_type:declare(Q0, node()) of
        {new, _Queue} ->
            rabbit_core_metrics:queue_created(QName),
            ok;
        {existing, _ExistingQ} ->
            %% Another connection won the race between the lookup and here.
            ok;
        {error, queue_limit_exceeded, Reason, ReasonArgs} ->
            ?LOG_ERROR(Reason, ReasonArgs),
            {error, {queue_declare_failed, queue_limit_exceeded}};
        Other ->
            ?LOG_ERROR("Failed to declare OCPP queue ~s: ~p",
                       [rabbit_misc:rs(QName), Other]),
            {error, {queue_declare_failed, queue_declare_error}}
    end.

%% Helper function for binding logic
-spec bind_queue(state()) -> {ok, state()} | {error, term()}.
bind_queue(State = #state{cfg = #cfg{queue_name = QName,
                                      exchange = ExchangeName,
                                      client_id = ClientId},
                          auth_state = #auth_state{user = User}}) ->
    %% The client ID was validated not to contain topic separators or
    %% wildcards (see rabbit_web_ocpp_util:validate_client_id/1).
    RoutingKey = ClientId,
    Binding = #binding{source = ExchangeName, destination = QName,
                       key = RoutingKey, args = []},
    case check_binding_permitted(QName, ExchangeName, RoutingKey, State) of
        ok ->
            case rabbit_binding:add(Binding, User#user.username) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    ?LOG_ERROR("OCPP failed to bind queue ~ts to ~ts: ~p",
                              [rabbit_misc:rs(QName), rabbit_misc:rs(ExchangeName), Reason]),
                    {error, {binding_failed, Reason}}
            end;
        {error, access_refused} ->
            ?LOG_WARNING("OCPP binding permission refused for queue ~ts / exchange ~ts",
                         [rabbit_misc:rs(QName), rabbit_misc:rs(ExchangeName)]),
            {error, {binding_failed, access_refused}}
    end.

%% Start consuming from the queue
-spec consume_from_queue(state()) -> {ok, state()} | {error, term()}.
consume_from_queue(State = #state{cfg = #cfg{queue_name = QName, client_id = ClientId,
                                             prefetch = Prefetch, consumer_tag = ConsumerTag},
                                  queue_states = QStates0,
                                  auth_state = #auth_state{user = User, authz_ctx = AuthzCtx}}) ->
    case check_resource_access(User, QName, read, AuthzCtx) of
        ok ->
            Spec = #{no_ack => false,
                     channel_pid => self(),
                     limiter_pid => none,
                     limiter_active => false,
                     mode => {simple_prefetch, Prefetch},
                     consumer_tag => ConsumerTag,
                     exclusive_consume => true,
                     args => [],
                     ok_msg => undefined,
                     acting_user => User#user.username},
            rabbit_amqqueue:with(
                QName,
                fun(Q) ->
                    case rabbit_queue_type:consume(Q, Spec, QStates0) of
                        {ok, QStates} ->
                            {ok, State#state{queue_states = QStates}};
                        {error, Type, Fmt, FmtArgs} ->
                            ?LOG_ERROR("OCPP failed to consume from ~ts for ClientId ~ts: ~ts",
                                       [rabbit_misc:rs(QName), ClientId,
                                        rabbit_misc:format(Fmt, FmtArgs)]),
                            {error, {consume_failed, Type}}
                    end
                end,
                fun(ErrorType) ->
                    ?LOG_ERROR("OCPP cannot consume, queue ~ts lookup failed for ClientId ~ts: ~p",
                               [rabbit_misc:rs(QName), ClientId, ErrorType]),
                    {error, {consume_failed, ErrorType}}
                end);
        {error, access_refused} ->
            ?LOG_WARNING("OCPP consume permission refused for queue ~ts for ClientId ~ts",
                         [rabbit_misc:rs(QName), ClientId]),
            {error, {consume_failed, access_refused}}
    end.

%% Generate queue name (e.g., ocpp.chargepoint_id)
-spec queue_name(Vhost :: rabbit_types:vhost(), ClientId :: binary()) -> rabbit_amqqueue:name().
queue_name(Vhost, ClientId) ->
    QNameBin = << "ocpp.", ClientId/binary >>,
    rabbit_misc:r(Vhost, queue, QNameBin).

%% --- Permission Checks ---

%% Check permissions for publishing to the OCPP exchange
check_publish_permitted(Exchange, RoutingKey, State = #state{auth_state = AuthState}) ->
    case check_resource_access(AuthState#auth_state.user, Exchange, write, AuthState#auth_state.authz_ctx) of
        ok -> check_topic_access(RoutingKey, write, State);
        Err -> Err
    end.

%% Check permissions for binding queue to exchange. Requires 'write' on the
%% queue, 'read' on the exchange, and topic 'read' on the binding key
check_binding_permitted(QName, ExchangeName, RoutingKey,
                        State = #state{auth_state = AuthState}) ->
    User = AuthState#auth_state.user,
    Ctx = AuthState#auth_state.authz_ctx,
    case check_resource_access(User, QName, write, Ctx) of
        ok ->
            case check_resource_access(User, ExchangeName, read, Ctx) of
                ok -> check_topic_access(RoutingKey, read, State);
                Err -> Err
            end;
        Err -> Err
    end.

%% Permission checks are cached, but only for a while: otherwise permission
%% changes would never apply to established connections.
expire_permission_caches() ->
    Now = erlang:monotonic_time(millisecond),
    case get(permission_cache_expires_at) of
        ExpiresAt when is_integer(ExpiresAt), Now < ExpiresAt ->
            ok;
        _ ->
            erase(permission_cache),
            erase(topic_permission_cache),
            put(permission_cache_expires_at,
                Now + rabbit_web_ocpp_util:get_env(permission_cache_ttl)),
            ok
    end.

check_resource_access(User, Resource, Perm, Context) ->
    expire_permission_caches(),
    V = {Resource, Context, Perm},
    Cache = case get(permission_cache) of
                undefined -> [];
                Other     -> Other
            end,
    case lists:member(V, Cache) of
        true ->
            ok;
        false ->
            try rabbit_access_control:check_resource_access(User, Resource, Perm, Context) of
                ok ->
                    CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE-1),
                    put(permission_cache, [V | CacheTail]),
                    ok
            catch
                exit:#amqp_error{name = access_refused,
                                 explanation = Msg} ->
                    ?LOG_ERROR("OCPP resource access refused: ~s", [Msg]),
                    {error, access_refused}
            end
    end.

check_topic_access(
  RoutingKey, Access,
  #state{auth_state = #auth_state{user = User = #user{username = Username}},
         cfg = #cfg{client_id = ClientId,
                    vhost = Vhost,
                    exchange = XName = #resource{name = XNameBin}}}) ->
    expire_permission_caches(),
    Cache = case get(topic_permission_cache) of
                undefined -> [];
                Other     -> Other
            end,
    Key = {RoutingKey, Username, ClientId, Vhost, XNameBin, Access},
    case lists:member(Key, Cache) of
        true ->
            ok;
        false ->
            Resource = XName#resource{kind = topic},
            Context = #{routing_key  => RoutingKey,
                        variable_map => #{<<"username">>  => Username,
                                          <<"vhost">>     => Vhost,
                                          <<"client_id">> => ClientId}},
            try rabbit_access_control:check_topic_access(User, Resource, Access, Context) of
                ok ->
                    CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE - 1),
                    put(topic_permission_cache, [Key | CacheTail]),
                    ok
            catch
                exit:#amqp_error{name = access_refused,
                                 explanation = Msg} ->
                    ?LOG_ERROR("OCPP topic access refused: ~s", [Msg]),
                    {error, access_refused}
            end
    end.

-spec init_trace(rabbit_types:vhost(), binary()) ->
    {rabbit_trace:state(), undefined | binary()}.
init_trace(Vhost, ConnName0) ->
    TraceState = rabbit_trace:init(Vhost),
    ConnName = case rabbit_trace:enabled(TraceState) of
                   true ->
                       ConnName0;
                   false ->
                       %% Tracing does not need connection name.
                       %% Use less memmory by setting to undefined.
                       undefined
               end,
    {TraceState, ConnName}.

%% Format status for management UI (very basic)
-spec format_status(state()) -> map().
format_status(#state{cfg = Cfg, queue_states = QStates, auth_state = AuthState}) ->
    #{cfg => Cfg,
      queue_states => rabbit_queue_type:format_status(QStates),
      auth_state => AuthState}.

%% Client input in log lines is truncated, so that a misbehaving charge point
%% cannot flood the logs.
-spec truncate(binary()) -> binary().
truncate(<<Head:?MAX_LOGGED_BYTES/binary, _/binary>> = Bin) ->
    <<Head/binary, "... (", (integer_to_binary(byte_size(Bin)))/binary, " bytes)">>;
truncate(Bin) when is_binary(Bin) ->
    Bin.

%% Seamlessly update both ETS tables without causing 404 errors.
%% connection_created_stats is owned by rabbit_mgmt_storage, which the
%% management agent does not start when management_agent.disable_metrics_collector
%% is set, so there is nothing to refresh in that case.
-spec force_stats_refresh(state()) -> ok.
force_stats_refresh(State) ->
    case ets:whereis(connection_created_stats) of
        undefined ->
            ok;
        Tid ->
            force_stats_refresh(Tid, State)
    end.

-spec force_stats_refresh(ets:tid(), state()) -> ok.
force_stats_refresh(Tid, State = #state{cfg = #cfg{client_id = ClientId}}) ->
    try
        Pid = self(),
        FreshClientProps = info(client_properties, State),
        case ets:lookup(Tid, Pid) of
            [{Pid, ConnName, OldStatsInfos}] ->
                %% Convert proplist to map format using the same function as management plugin
                FormattedClientProps = rabbit_misc:amqp_table(FreshClientProps),
                UpdatedStatsInfos = lists:keystore(client_properties, 1, OldStatsInfos,
                                                 {client_properties, FormattedClientProps}),
                ets:insert(Tid, {Pid, ConnName, UpdatedStatsInfos});
            [] ->
                %% Created on the next collection.
                ok
        end
    catch
        Class:Reason:Stacktrace ->
            ?LOG_WARNING("Failed to refresh connection stats for ~ts: ~p:~p~n~p",
                         [ClientId, Class, Reason, Stacktrace])
    end,
    ok.

%% @doc Updates the client properties shown in the management UI from
%% BootNotification and StatusNotification. Only known keys and a bounded
%% number of connectors are stored: the properties are client input.
-spec maybe_update_props_from_message(tuple(), state()) -> state().
maybe_update_props_from_message({?OCPP_MESSAGE_TYPE_CALL, _MsgId, <<"BootNotification">>, Payload},
                                State = #state{cfg = Cfg = #cfg{user_prop = OldProps,
                                                                proto_ver = ?OCPP_PROTO_V16}}) ->
    MergedProps = lists:foldl(
                    fun(Key, Acc) ->
                            case maps:get(Key, Payload, undefined) of
                                Val when is_binary(Val) ->
                                    lists:keystore(Key, 1, Acc,
                                                   {Key, longstr, truncate_value(Val)});
                                _ ->
                                    Acc
                            end
                    end, OldProps, ?BOOT_NOTIFICATION_PROPS),
    UpdatedState = State#state{cfg = Cfg#cfg{user_prop = MergedProps}},
    force_stats_refresh(UpdatedState),
    UpdatedState;
maybe_update_props_from_message({?OCPP_MESSAGE_TYPE_CALL, _MsgId, <<"StatusNotification">>, Payload},
                                State = #state{cfg = Cfg = #cfg{user_prop = OldProps,
                                                                proto_ver = ?OCPP_PROTO_V16}}) ->
    case {maps:get(<<"connectorId">>, Payload, undefined),
          maps:get(<<"status">>, Payload, undefined),
          maps:get(<<"errorCode">>, Payload, undefined)} of
        {ConnectorId, Status, ErrorCode}
          when is_integer(ConnectorId), ConnectorId >= 0, ConnectorId =< ?MAX_CONNECTOR_ID,
               is_binary(Status), is_binary(ErrorCode) ->
            Key = <<"statusConnectorId", (integer_to_binary(ConnectorId))/binary>>,
            Value = [{<<"status">>, truncate_value(Status)},
                     {<<"errorCode">>, truncate_value(ErrorCode)}],
            UpdatedProps = lists:keystore(Key, 1, OldProps, {Key, longstr, Value}),
            UpdatedState = State#state{cfg = Cfg#cfg{user_prop = UpdatedProps}},
            force_stats_refresh(UpdatedState),
            UpdatedState;
        _ ->
            State
    end;
maybe_update_props_from_message(_Msg, State) ->
    State.

truncate_value(<<Val:?MAX_PROPERTY_VALUE_BYTES/binary, _/binary>>) -> Val;
truncate_value(Val) -> Val.

-spec info(rabbit_types:info_key(), state()) -> any().
info(host, #state{cfg = #cfg{ip_addr = Val}}) -> Val;
info(port, #state{cfg = #cfg{port = Val}}) -> Val;
info(peer_host, #state{cfg = #cfg{peer_ip_addr = Val}}) -> Val;
info(peer_port, #state{cfg = #cfg{peer_port = Val}}) -> Val;
info(connected_at, #state{cfg = #cfg{connected_at = Val}}) -> Val;
info(user_who_performed_action, S) ->
    info(user, S);
info(prefetch_count, #state{cfg = #cfg{prefetch = Val}}) -> Val;
info(user, #state{auth_state = #auth_state{user = #user{username = Val}}}) -> Val;
info(user_property, #state{cfg = #cfg{user_prop = Val}}) -> Val;
info(vhost, #state{cfg = #cfg{vhost = Val}}) -> Val;
%% for rabbitmq_management/priv/www/js/tmpl/connection.ejs
%% Keys stay binaries, like AMQP 0-9-1 client properties: atoms are never
%% garbage collected.
info(client_properties, #state{cfg = #cfg{client_id = ClientId,
                                          user_prop = Prop}}) ->
    [{<<"chargePointId">>, longstr, ClientId},
     {<<"connection_name">>, longstr, <<"Charging Point">>}
     | Prop];
info(channel_max, _) -> 0;
info(node, _) -> node();
info(frame_max, _) -> 0;
info(_Other, _State) ->
    undefined.

-spec proto_version_tuple(ocpp_protocol_version_atom() | undefined) -> tuple() | undefined.
proto_version_tuple(?OCPP_PROTO_V16) -> {1, 6};
proto_version_tuple(?OCPP_PROTO_V20) -> {2, 0};
proto_version_tuple(?OCPP_PROTO_V201) -> {2, 0, 1};
proto_version_tuple(?OCPP_PROTO_V21) -> {2, 1};
proto_version_tuple(_) -> undefined.
