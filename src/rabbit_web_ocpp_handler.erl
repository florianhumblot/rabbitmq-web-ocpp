%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_web_ocpp_handler).

-feature(maybe_expr, enable).

-behaviour(cowboy_websocket).
-behaviour(cowboy_sub_protocol).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/logging.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_web_ocpp.hrl").

-export([
    init/2,
    websocket_init/1,
    websocket_handle/2,
    websocket_info/2,
    terminate/3
]).

-export([info/2,
         conserve_resources/3]).

%% cowboy_sub_protocol
-export([upgrade/4,
         upgrade/5,
         takeover/7]).

-ifdef(TEST).
-define(SILENT_CLOSE_DELAY, 10).
-else.
-define(SILENT_CLOSE_DELAY, 3_000).
-endif.

-record(state, {
          socket :: {rabbit_proxy_socket, any(), any()} | rabbit_net:socket(),
          proc_state :: rabbit_web_ocpp_processor:state(),
          connection_state = running :: running | blocked,
          %% Resource alarms this connection is blocked by.
          blocked_by = sets:new([{version, 2}]) :: sets:set(rabbit_alarm:resource_alarm_source()),
          ping_interval = infinity :: timeout(),
          stats_timer :: option(rabbit_event:state()),
          vhost :: rabbit_types:vhost(),
          client_id :: client_id(),
          user :: #user{},
          authz_ctx = #{} :: #{binary() => binary()},
          ssl_login_name = none :: none | binary(),
          auth_mechanism = <<"UNKNOWN">> :: binary(),
          conn_name :: option(binary()),
          idle_timeout :: timeout(), %% from cowboy, in seconds
          proto_ver :: ocpp_protocol_version_atom()
         }).

-type state() :: #state{}.

%% cowboy_sub_protcol
upgrade(Req, Env, Handler, HandlerState) ->
    upgrade(Req, Env, Handler, HandlerState, #{}).

upgrade(Req, Env, Handler, HandlerState, Opts) ->
    cowboy_websocket:upgrade(Req, Env, Handler, HandlerState, Opts).

takeover(Parent, Ref, Socket, Transport, Opts, Buffer, {Handler, HandlerState}) ->
    Sock = case HandlerState#state.socket of
               undefined ->
                   Socket;
               ProxyInfo ->
                   {rabbit_proxy_socket, Socket, ProxyInfo}
           end,
    cowboy_websocket:takeover(Parent, Ref, Socket, Transport, Opts, Buffer,
                              {Handler, HandlerState#state{socket = Sock}}).

%% cowboy_websocket
init(Req, Opts) ->
    %% Retrieve the vhost and client_id from URL path first
    Vhost = cowboy_req:binding(vhost, Req),
    ClientId = cowboy_req:binding(client_id, Req),
    {PeerIp, _PeerPort} = cowboy_req:peer(Req),
    %% The transport socket is not accessible before the WebSocket takeover,
    %% so rejected connections cannot use rabbit_net:connection_string/2.
    %% Keep the peer address for rejection logging instead.
    PeerAddr = peer_addr(Req),

    case {Vhost, ClientId} of
        {<<>>, _} ->
            {ok, cowboy_req:reply(404, #{}, <<"Vhost not specified">>, Req),
             #state{conn_name = PeerAddr}};
        {_, <<>>} ->
            {ok, cowboy_req:reply(404, #{}, <<"Client ID not specified">>, Req),
             #state{conn_name = PeerAddr}};
        _ ->
            {Username0, Password0} = basic_auth_creds(Req),
            SslLoginName = ssl_login_name_from_req(Req),
            %% We don't use rabbit_net:maybe_get_proxy_socket(Sock) on purpose,
            %% because we don't trust the PROXY protocol, maybe @TODO.
            IsSsl = cowboy_req:scheme(Req) =:= <<"https">>,
            Result = maybe
                ok ?= check_client_id(ClientId, PeerIp),
                ok ?= check_vhost_exists(Vhost, ClientId, PeerIp),
                ok ?= check_vhost_alive(Vhost),
                {ok, ProtoVer, Req1} ?= pick_protocol(Req, ClientId),
                {ok, Username1, Password1} ?= check_credentials(ClientId, Username0, Password0, SslLoginName, PeerIp),
                ok ?= check_username_matches_client_id(ClientId, Username0, SslLoginName, PeerIp),
                {ok, User0} ?= check_user_login(Vhost, Username1, Password1, ClientId, PeerIp, IsSsl),
                ok ?= check_user_loopback(User0, PeerIp),
                ok ?= check_tls_only(User0, IsSsl, PeerIp),
                AuthzCtx = #{<<"client_id">> => ClientId, <<"protocol">> => <<"ocpp">>,
                             <<"ssl">> => rabbit_data_coercion:to_binary(IsSsl)},
                ok ?= check_vhost_access(Vhost, User0, ClientId, PeerIp, AuthzCtx),
                {ok, Req1, Vhost, ClientId, User0, ProtoVer, AuthzCtx}
            end,
            %% Keep identifying info in the state so terminate/3 can log
            %% meaningfully when the connection is rejected below.
            RejState = #state{vhost = Vhost, client_id = ClientId, conn_name = PeerAddr},
            case Result of
                {ok, Req2, V2, CId, User, ProtocolVer, AuthzCtx1} ->
                    ProxyInfo   = maps:get(proxy_header, Req2, undefined),
                    WsOpts0     = proplists:get_value(ws_opts, Opts, #{}),
                    IdleMs      = maps:get(idle_timeout, WsOpts0, ?DEFAULT_IDLE_TIMEOUT_MS),
                    %% Compression is opt-in (web_ocpp.ws_opts.compress): each
                    %% permessage-deflate connection holds two zlib contexts for
                    %% its whole lifetime. When enabled, ?DEFAULT_DEFLATE_OPTS is
                    %% applied unless the operator supplies deflate_opts.
                    WsOpts1     = maps:merge(#{compress => false,
                                               idle_timeout => IdleMs,
                                               max_frame_size => ?DEFAULT_MAX_FRAME_SIZE}, WsOpts0),
                    WsOpts      = case WsOpts1 of
                                      #{compress := true} ->
                                          maps:merge(#{deflate_opts => ?DEFAULT_DEFLATE_OPTS}, WsOpts1);
                                      _ ->
                                          WsOpts1
                                  end,
                    IdleSec     = case IdleMs of infinity -> 0; Ms -> Ms div 1000 end,
                    State = #state{socket = ProxyInfo, proto_ver = ProtocolVer, vhost = V2,
                                   user = User, authz_ctx = AuthzCtx1,
                                   client_id = CId, idle_timeout = IdleSec,
                                   ping_interval = ping_interval(IdleMs),
                                   ssl_login_name = SslLoginName,
                                   auth_mechanism = auth_mechanism(Username0, SslLoginName)},
                    {?MODULE, Req2, State, WsOpts};
                {error, {invalid_client_id, Msg}} ->
                    {ok, cowboy_req:reply(400, #{<<"connection">> => <<"close">>}, Msg, Req), RejState};
                {error, bad_vhost} ->
                    {ok, cowboy_req:reply(404, #{}, <<"Invalid Vhost">>, Req), RejState};
                {error, vhost_down} ->
                    {ok, cowboy_req:reply(503, #{}, <<"Vhost is down">>, Req), RejState};
                {error, invalid_subprotocol} ->
                    {ok, cowboy_req:reply(400, #{<<"connection">> => <<"close">>},
                                          <<"Unsupported or missing OCPP subprotocol">>, Req), RejState};
                {error, _} ->
                    {ok, cowboy_req:reply(401,
                          #{<<"www-authenticate">> => <<"Basic realm=\"OCPP\"">>},
                          <<"Unauthorized">>, Req), RejState}
            end
    end.

%% We cannot use a gen_server call, because the handler process is a
%% special cowboy_websocket process (not a gen_server) which assumes
%% all gen_server calls are supervisor calls, and does not pass on the
%% request to this callback module. (see cowboy_websocket:loop/3 and
%% cowboy_children:handle_supervisor_call/4) However using a generic
%% gen:call with a special label ?MODULE works fine.
-spec info(pid(), rabbit_types:info_keys()) ->
    rabbit_types:infos().
info(Pid, all) ->
    info(Pid, ?INFO_ITEMS);
info(Pid, Items) ->
    {ok, Res} = gen:call(Pid, ?MODULE, {info, Items}),
    Res.
-spec websocket_init(state()) ->
    {cowboy_websocket:commands(), state()} |
    {cowboy_websocket:commands(), state, hibernate}.
websocket_init(State0 = #state{socket = Socket, vhost = Vhost, client_id = ClientId,
                               user = User, authz_ctx = AuthzCtx, proto_ver = ProtoVer}) ->
    logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_CONN ++ [web_ocpp]}),
    %% The socket (incl. proxy protocol info) is only available from
    %% takeover/7 onwards, so the proper connection name can only be
    %% built here, not in init/2.
    case rabbit_net:connection_string(Socket, inbound) of
        {ok, ConnStr} ->
            ConnName = rabbit_data_coercion:to_binary(ConnStr),
            State1 = State0#state{conn_name = ConnName},
            State2 = rabbit_event:init_stats_timer(State1, #state.stats_timer),
            % Inside `init` of the processor "connection_created" is called for management UI to show the connection
            case rabbit_web_ocpp_processor:init(Vhost, ClientId, ProtoVer,
                                                rabbit_net:unwrap_socket(Socket),
                                                ConnName, User, AuthzCtx, fun send_reply/1) of
                {ok, ProcState} ->
                    ?LOG_INFO("Accepted Web OCPP connection ~ts for client ID ~ts",
                                [ConnName, ClientId]),
                    Alarms = rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
                    State3 = State2#state{proc_state = ProcState,
                                          blocked_by = sets:from_list(Alarms, [{version, 2}])},
                    process_flag(trap_exit, true),
                    schedule_ping(State3),
                    % `ensure_stats_timer` is needed to trigger the initial stats collection
                    % and update the connection state to "running" in the management UI
                    {Cmds, FinalState} = control_throttle(ensure_stats_timer(State3)),
                    {Cmds, FinalState, hibernate};
                {error, Reason} ->
                    ?LOG_ERROR("Rejected Web OCPP connection ~ts: ~p", [ConnName, Reason]),
                    self() ! {stop, ?CLOSE_PROTOCOL_ERROR, connect_packet_rejected},
                    {[], State2}
            end;
        {error, Reason} ->
            {[{shutdown_reason, Reason}], State0}
    end.

-spec websocket_handle(ping | pong | {text | binary | ping | pong, binary()}, State) ->
    {cowboy_websocket:commands(), State} |
    {cowboy_websocket:commands(), State, hibernate}.
%% Handle text (JSON) frames (pass to processor)
websocket_handle({text, Data}, State = #state{conn_name = ConnName, client_id = ClientId,
                                              proc_state = ProcState0}) ->
    case rabbit_web_ocpp_processor:handle_text_frame(Data, ProcState0) of
        {ok, ProcState, Frames} ->
            {Frames, ensure_stats_timer(State#state{proc_state = ProcState}), hibernate};
        {error, Reason, ProcState} ->
            ?LOG_WARNING("Web OCPP closing connection ~ts of client ID ~ts: ~p",
                         [ConnName, ClientId, Reason]),
            CloseCode = case Reason of
                            invalid_json -> ?CLOSE_INVALID_PAYLOAD;
                            invalid_message -> ?CLOSE_PROTOCOL_ERROR;
                            access_refused -> ?CLOSE_POLICY_VIOLATION;
                            _ -> ?CLOSE_INTERNAL_ERROR
                        end,
            stop(State#state{proc_state = ProcState}, CloseCode, Reason)
    end;
%% Silently ignore ping and pong frames as Cowboy will automatically reply to ping frames.
websocket_handle({Ping, _}, State)
  when Ping =:= ping orelse Ping =:= pong ->
    {[], State, hibernate};
websocket_handle(Ping, State)
  when Ping =:= ping orelse Ping =:= pong ->
    {[], State, hibernate};
%% Log and close connection when receiving any other unexpected frames.
%% This includes binary (compressed) frames, which are not implemented yet.
websocket_handle(Frame, State = #state{conn_name = ConnName, client_id = ClientId}) ->
    ?LOG_INFO("Web OCPP: unexpected WebSocket frame from client ID ~p (~p): ~tp", [ClientId, ConnName, Frame]),
    stop(State, ?CLOSE_UNACCEPTABLE_DATA_TYPE, <<"unexpected WebSocket frame type">>).

-spec websocket_info(any(), State) ->
    {cowboy_websocket:commands(), State} |
    {cowboy_websocket:commands(), State, hibernate}.
websocket_info({reply, Data}, State) ->
    % Send the data as text frame (JSON)
    {[{text, Data}], State, hibernate};
websocket_info({stop, CloseCode, Error}, State) ->
    stop(State, CloseCode, Error);
websocket_info({'EXIT', _, _}, State) ->
    stop(State);
websocket_info({conserve_resources, Source, Conserve},
               State = #state{blocked_by = BlockedBy0}) ->
    BlockedBy = case Conserve of
                    true -> sets:add_element(Source, BlockedBy0);
                    false -> sets:del_element(Source, BlockedBy0)
                end,
    {Cmds, State1} = control_throttle(State#state{blocked_by = BlockedBy}),
    {Cmds, State1, hibernate};
websocket_info(ping, State) ->
    %% Charge points answer pings with pongs, which resets the idle timeout:
    %% connected charge points that send little are not disconnected.
    schedule_ping(State),
    {[ping], State, hibernate};
websocket_info({'$gen_cast', QueueEvent = {queue_event, _, _}}, State) ->
    handle_processor_result(
      rabbit_web_ocpp_processor:handle_info(QueueEvent, State#state.proc_state), State);
websocket_info({ocpp_call_timeout, _} = Timeout, State) ->
    handle_processor_result(
      rabbit_web_ocpp_processor:handle_info(Timeout, State#state.proc_state), State);
websocket_info({'$gen_cast', {duplicate_id}},
               State = #state{client_id = ClientId,
                              conn_name = ConnName}) ->
    ?LOG_WARNING("Web OCPP disconnecting a client with duplicate ID '~s' (~p)",
                 [ClientId, ConnName]),
    yield_to_newer_connection(State);
%% pg group membership events, see rabbit_web_ocpp_processor:register_client_id/2.
%% Usually the newer connection kicks the older one directly. When two
%% connections with the same client ID see each other's join instead (after
%% a network partition heals), either could be the newer one: tell the other
%% when this one connected, and the older one yields.
websocket_info({Ref, join, _PgGroup, Pids}, State = #state{proc_state = PState})
  when is_reference(Ref) ->
    case {Pids -- [self()], PState} of
        {Others, _} when Others =:= [] orelse PState =:= undefined ->
            %% Our own join, reported because the monitor is
            %% installed before the group is joined.
            {[], State, hibernate};
        {Others, _} ->
            ConnectedAt = rabbit_web_ocpp_processor:connected_at(PState),
            [gen_server:cast(Pid, {duplicate_id_check, self(), ConnectedAt}) || Pid <- Others],
            {[], State, hibernate}
    end;
websocket_info({'$gen_cast', {duplicate_id_check, Other, OtherConnectedAt}},
               State = #state{proc_state = PState,
                              client_id = ClientId,
                              conn_name = ConnName})
  when PState =/= undefined ->
    ConnectedAt = rabbit_web_ocpp_processor:connected_at(PState),
    case {ConnectedAt, self()} < {OtherConnectedAt, Other} of
        true ->
            ?LOG_WARNING("Web OCPP disconnecting client with ID '~s' (~p): a newer "
                         "connection with the same client ID exists",
                         [ClientId, ConnName]),
            yield_to_newer_connection(State);
        false ->
            {[], State, hibernate}
    end;
websocket_info({Ref, leave, _PgGroup, _Pids}, State)
  when is_reference(Ref) ->
    {[], State, hibernate};
websocket_info({'$gen_cast', {close_connection, Reason}},
               State = #state{client_id = ClientId,
                              conn_name = ConnName}) ->
    ?LOG_WARNING("Web OCPP disconnecting client with ID '~s' (~p), reason: ~s",
                 [ClientId, ConnName, Reason]),
    case Reason of
        maintenance ->
            defer_close(?CLOSE_SERVER_GOING_DOWN),
            {[], State};
        _ ->
            stop(State)
    end;
websocket_info({'$gen_cast', {force_event_refresh, Ref}}, State0) ->
    Infos = infos(?EVENT_KEYS, State0),
    rabbit_event:notify(connection_created, Infos, Ref),
    State = rabbit_event:init_stats_timer(State0, #state.stats_timer),
    {[], State, hibernate};
websocket_info({'$gen_cast', refresh_config},
               State0 = #state{conn_name = _ConnName}) ->
    State = State0,
    {[], State, hibernate};
websocket_info(credential_expired,
               State = #state{client_id = ClientId,
                              conn_name = ConnName}) ->
    ?LOG_WARNING("Web OCPP disconnecting client with ID '~s' (~p) because credential expired",
                 [ClientId, ConnName]),
    defer_close(?CLOSE_NORMAL),
    {[], State};
websocket_info(emit_stats, State) ->
    {[], emit_stats(State), hibernate};
websocket_info({{'DOWN', _QName}, _MRef, process, _Pid, _Reason} = Evt,
               State = #state{proc_state = PState}) when PState =/= undefined ->
    handle_processor_result(rabbit_web_ocpp_processor:handle_down(Evt, PState), State);
websocket_info({'DOWN', _MRef, process, QPid, _Reason}, State) ->
    rabbit_amqqueue_common:notify_sent_queue_down(QPid),
    {[], State, hibernate};
websocket_info({shutdown, Reason}, #state{conn_name = ConnName} = State) ->
    %% rabbitmq_management plugin requests to close connection.
    ?LOG_INFO("Web OCPP closing connection ~tp: ~tp", [ConnName, Reason]),
    stop(State, ?CLOSE_NORMAL, Reason);
websocket_info(connection_created, State) ->
    Infos = infos(?EVENT_KEYS, State),
    rabbit_core_metrics:connection_created(self(), Infos),
    rabbit_event:notify(connection_created, Infos),
    {[], State, hibernate};
websocket_info({?MODULE, From, {info, Items}}, State) ->
    Infos = infos(Items, State),
    gen:reply(From, Infos),
    {[], State, hibernate};
websocket_info(Msg, State) ->
    ?LOG_WARNING("Web OCPP: unexpected message ~tp", [Msg]),
    {[], State, hibernate}.

terminate(_Reason, _Request, #state{proc_state = undefined,
                                    client_id = ClientId,
                                    conn_name = ConnName}) ->
    %% Connection was rejected before a session was established (unknown vhost,
    %% missing/invalid subprotocol, failed authentication, processor init
    %% failure). The specific reason has already been logged at error level.
    ?LOG_DEBUG("Web OCPP connection ~ts rejected before session started for client ID ~p",
               [ConnName, ClientId]),
    ok;
terminate(Reason, _Request, #state{conn_name = ConnName,
                            proc_state = PState,
                            client_id = ClientId} = State) ->
    ?LOG_INFO("Web OCPP closing connection ~ts for client ID ~p", [ConnName, ClientId]),
    maybe_emit_stats(State),
    Infos = infos(?EVENT_KEYS, State),
    rabbit_web_ocpp_processor:terminate(Reason, Infos, PState);

terminate(Reason, _Request, Opts) ->
    %% Fallback clause when init crashed before state record was established
    ?LOG_INFO("Web OCPP closing connection. Reason: ~p Opts: ~p", [Reason, Opts]),
    ok.

%% Internal.

handle_processor_result({ok, PState, Frames}, State) ->
    {Frames, State#state{proc_state = PState}, hibernate};
handle_processor_result({stop, Reason, PState}, State = #state{conn_name = ConnName}) ->
    ?LOG_WARNING("Web OCPP closing connection ~ts: ~p", [ConnName, Reason]),
    stop(State#state{proc_state = PState}, ?CLOSE_INTERNAL_ERROR, <<"internal error">>).

%% The charge point is still online, on another connection.
yield_to_newer_connection(State = #state{proc_state = PState}) ->
    defer_close(?CLOSE_NORMAL),
    {[], State#state{proc_state = rabbit_web_ocpp_processor:duplicate_id_kicked(PState)}}.

-spec conserve_resources(pid(),
                         rabbit_alarm:resource_alarm_source(),
                         rabbit_alarm:resource_alert()) -> ok.
conserve_resources(Pid, Source, {_, Conserve, _}) ->
    Pid ! {conserve_resources, Source, Conserve},
    ok.

%% Stop reading from the socket while a resource alarm is in effect, so that
%% charge points cannot publish into a broker that is running out of memory
%% or disk. TCP back pressure makes them wait.
control_throttle(State = #state{connection_state = running, blocked_by = BlockedBy}) ->
    case sets:is_empty(BlockedBy) of
        true -> {[], State};
        false -> {[{active, false}], State#state{connection_state = blocked}}
    end;
control_throttle(State = #state{connection_state = blocked, blocked_by = BlockedBy}) ->
    case sets:is_empty(BlockedBy) of
        true -> {[{active, true}], State#state{connection_state = running}};
        false -> {[], State}
    end.

%% By default, ping at half the idle timeout.
ping_interval(IdleMs) ->
    case rabbit_web_ocpp_util:get_env(ws_ping_interval) of
        Interval when is_integer(Interval), Interval > 0 -> Interval;
        0 -> infinity;
        _ when is_integer(IdleMs) -> max(1, IdleMs div 2);
        _ -> infinity
    end.

schedule_ping(#state{ping_interval = infinity}) ->
    ok;
schedule_ping(#state{ping_interval = Interval}) ->
    _ = erlang:send_after(Interval, self(), ping),
    ok.

check_client_id(ClientId, PeerIp) ->
    case rabbit_web_ocpp_util:validate_client_id(ClientId) of
        ok ->
            ok;
        {error, Msg} ->
            ?LOG_ERROR("OCPP connection refused: ~ts: ~ts",
                       [Msg, rabbit_web_ocpp_processor:truncate(ClientId)]),
            auth_attempt_failed(PeerIp, <<>>),
            {error, {invalid_client_id, Msg}}
    end.

%% OCPP security profiles 1 and 2 [OCPP 1.6 Security Whitepaper, OCPP 2.0.1
%% Part 2 A00.FR.203]: the HTTP Basic username is the charge point identity.
%% Otherwise, valid credentials of one charge point (or a user shared by a
%% fleet) let anybody take over the session of any charge point. A client
%% certificate (security profile 3) was already checked against the client ID,
%% and anonymous logins (web_ocpp.allow_anonymous) send no username.
check_username_matches_client_id(_ClientId, undefined, _SslLoginName, _PeerIp) ->
    ok;
check_username_matches_client_id(_ClientId, _Username, SslLoginName, _PeerIp)
  when SslLoginName =/= none ->
    ok;
check_username_matches_client_id(ClientId, ClientId, none, _PeerIp) ->
    ok;
check_username_matches_client_id(ClientId, Username, none, PeerIp) ->
    case rabbit_web_ocpp_util:get_env(username_must_match_client_id) of
        false ->
            ok;
        true ->
            ?LOG_ERROR("OCPP login failed: username '~ts' does not match client ID '~ts'",
                       [Username, ClientId]),
            auth_attempt_failed(PeerIp, Username),
            {error, username_mismatch}
    end.

%% Authentication mechanism shown in the management UI. Mirrors the
%% decision taken by creds/4: a validated client certificate takes
%% precedence, then HTTP Basic auth, then anonymous.
-spec auth_mechanism(undefined | binary(), none | binary()) -> binary().
auth_mechanism(_Username, SslLoginName) when SslLoginName =/= none ->
    <<"MTLS">>;
auth_mechanism(undefined, none) ->
    <<"ANONYMOUS">>;
auth_mechanism(_Username, none) ->
    <<"BASIC">>.

%% Peer address of a connection before the WebSocket takeover, for logging.
%% Prefers the source advertised by a PROXY protocol header, applying the
%% same selection rule as rabbit_net:socket_ends/2 does for proxy sockets.
-spec peer_addr(cowboy_req:req()) -> binary().
peer_addr(Req) ->
    {Ip, Port} = case maps:get(proxy_header, Req, undefined) of
                     #{src_address := SrcIp, src_port := SrcPort} ->
                         {SrcIp, SrcPort};
                     _ ->
                         cowboy_req:peer(Req)
                 end,
    rabbit_data_coercion:to_binary(
      rabbit_misc:format("~s:~b", [rabbit_misc:ntoab(Ip), Port])).

%% Extract SSL login name from the Cowboy request (available before WS upgrade).
%% Used for mutual TLS / certificate-based authentication.
-spec ssl_login_name_from_req(cowboy_req:req()) -> none | binary().
ssl_login_name_from_req(Req) ->
    case cowboy_req:cert(Req) of
        undefined -> none;
        Cert      -> case rabbit_ssl:peer_cert_auth_name(Cert) of
                         unsafe    -> none;
                         not_found -> none;
                         Name      -> Name
                     end
    end.

pick_protocol(Req, ClientId) ->
    %% The client MUST include a valid ocpp version in the list of
    %% WebSocket Sub Protocols it offers [OCPP 1.6 JSON spec §3.1.2].
    case cowboy_req:parse_header(<<"sec-websocket-protocol">>, Req) of
        undefined ->
            ?LOG_ERROR("Web OCPP: missing subprotocol list for client ~p", [ClientId]),
            {error, invalid_subprotocol};
        ProtoList ->
            Allowed = rabbit_web_ocpp_util:allowed_protocols(),
            case [ {P, ?OCPP_PROTO_TO_ATOM(P)} || P <- ProtoList,
                                             lists:member(P, Allowed),
                                             ?OCPP_PROTO_TO_ATOM(P) =/= undefined ] of
                [] ->
                    ?LOG_ERROR("Web OCPP: no supported ocppX.X subprotocol in ~p for client ~p",
                               [ProtoList, ClientId]),
                    {error, invalid_subprotocol};
                [{Matched, Ver}|_] ->
                    {ok, Ver, cowboy_req:set_resp_header(<<"sec-websocket-protocol">>, Matched, Req)}
            end
    end.

basic_auth_creds(Req) ->
    case cowboy_req:header(<<"authorization">>, Req, <<>>) of
        <<>> -> {undefined, undefined};
        H ->
            try cow_http_hd:parse_authorization(H) of
                {basic, U, P} -> {U, P};
                _ -> {invalid, invalid}
            catch _:_ ->
                %% Bearer token or malformed value
                {invalid, invalid}
            end
    end.

check_credentials(ClientId, Username, Password, SslLoginName, PeerIp) ->
    case creds(ClientId, Username, Password, SslLoginName) of
        {ok, _, _} = Ok ->
            Ok;
        {invalid_cert_creds, CertLoginName} ->
            ?LOG_ERROR("OCPP TLS client certificate identity '~s' does not match client ID '~s'",
                       [CertLoginName, ClientId]),
            auth_attempt_failed(PeerIp, CertLoginName),
            {error, ?CLOSE_POLICY_VIOLATION};
        nocreds ->
            ?LOG_ERROR("OCPP login failed: no credentials provided"),
            auth_attempt_failed(PeerIp, <<>>),
            {error, ?CLOSE_POLICY_VIOLATION};
        {invalid_creds, {invalid, invalid}} ->
            ?LOG_ERROR("OCPP login failed: malformed or non-Basic Authorization header"),
            auth_attempt_failed(PeerIp, <<>>),
            {error, ?CLOSE_POLICY_VIOLATION};
        {invalid_creds, {undefined, Pass}} when is_binary(Pass) ->
            ?LOG_ERROR("OCPP login failed: no username is provided"),
            auth_attempt_failed(PeerIp, <<>>),
            {error, ?CLOSE_POLICY_VIOLATION};
        {invalid_creds, {User, _Pass}} when is_binary(User) ->
            ?LOG_ERROR("OCPP login failed for user '~s': no password provided", [User]),
            auth_attempt_failed(PeerIp, User),
            {error, ?CLOSE_POLICY_VIOLATION}
    end.

creds(ClientId, User, Pass, SSLLoginName) ->
    CredentialsProvided = User =/= undefined orelse Pass =/= undefined,
    ValidCredentials = is_binary(User) andalso is_binary(Pass) andalso Pass =/= <<>>,
    SSLLoginProvided = SSLLoginName =/= none,

    case {CredentialsProvided, ValidCredentials, SSLLoginProvided} of
        %% If a validated client cert is present, use it as the principal.
        %% This lets the TLS listener support OCPP Security Profile 3 first,
        %% while still allowing Profile 2 clients to fall back to Basic auth.
        {_, _, true} when SSLLoginName =:= ClientId ->
            {ok, SSLLoginName, none};
        {_, _, true} ->
            {invalid_cert_creds, SSLLoginName};
        {true, true, false} ->
            {ok, User, Pass};
        {true, false, false} ->
            %% Either username or password is provided
            {invalid_creds, {User, Pass}};
        {false, false, false} ->
            AllowAnon = application:get_env(?APP_NAME, allow_anonymous, false),
            case AllowAnon of
                true ->
                    case rabbit_auth_mechanism_anonymous:credentials() of
                        {ok, _, _} = Ok ->
                            Ok;
                        error ->
                            nocreds
                    end;
                false ->
                    nocreds
            end;
        _ ->
            nocreds
    end.

check_vhost_exists(Vhost, ClientId, PeerIp) ->
    case rabbit_vhost:exists(Vhost) of
        true -> ok;
        false ->
            ?LOG_ERROR("OCPP connection failed: vhost '~s' does not exist", [Vhost]),
            auth_attempt_failed(PeerIp, ClientId),
            {error, bad_vhost}
    end.

check_vhost_alive(Vhost) ->
     case rabbit_vhost_sup_sup:is_vhost_alive(Vhost) of
        true -> ok;
        false ->
            ?LOG_ERROR("OCPP connection failed: vhost '~s' is down", [Vhost]),
            {error, vhost_down}
    end.

check_vhost_access(Vhost, User, _ClientId, PeerIp, AuthzCtx) ->
    try rabbit_access_control:check_vhost_access(User, Vhost, {ip, PeerIp}, AuthzCtx) of
        ok -> ok
    catch exit:#amqp_error{name = not_allowed, explanation = Msg} ->
        ?LOG_ERROR("OCPP vhost access refused for user '~s' to vhost '~s': ~s",
                   [User#user.username, Vhost, Msg]),
        auth_attempt_failed(PeerIp, User#user.username),
        {error, access_refused}
    end.

check_user_login(Vhost, Username, Password, ClientId, PeerIp, IsSsl) ->
    %% rabbitmq_auth_backend_http forwards every non-internal auth property to
    %% the HTTP service, so 'ssl' reaches it as a request parameter.
    AuthProps = [{vhost, Vhost}, {client_id, ClientId}, {password, Password},
                 {ssl, IsSsl}],
    % For cases when authenticating using an x.509 certificate, Password equals atom "none"
    case rabbit_access_control:check_user_login(Username, AuthProps) of
        {ok, User = #user{username = RabbitUser}} ->
            notify_auth_result(user_authentication_success, RabbitUser, PeerIp),
            {ok, User};
        {refused, UserForLog, Msg, Args} ->
            ?LOG_ERROR("OCPP login failed for user '~s': " ++ Msg, [UserForLog | Args]),
            notify_auth_result(user_authentication_failure, UserForLog, PeerIp),
            auth_attempt_failed(PeerIp, UserForLog),
            {error, authentication_failure}
    end.

%% Reject users listed in {rabbit, loopback_users} unless they connect from a
%% loopback address. Mirrors the equivalent check in the other plugins.
%% Uses the effective post-login username so that anonymous logins (which map
%% to anonymous_login_user, e.g. 'guest') are also covered.
check_user_loopback(#user{username = Username}, PeerIp) ->
    case rabbit_access_control:check_user_loopback(Username, PeerIp) of
        ok ->
            ok;
        not_allowed ->
            ?LOG_ERROR("OCPP login failed: user '~s' can only connect via localhost", [Username]),
            auth_attempt_failed(PeerIp, Username),
            {error, access_refused}
    end.

%% Reject users tagged 'tlsonly' on the plain listener. The tag is read off the
%% authenticated user, so any authentication backend can set it. Rejections look
%% like any other refusal to the client, so the plain listener cannot be used to
%% tell valid credentials apart from invalid ones.
check_tls_only(_User, _IsSsl = true, _PeerIp) ->
    ok;
check_tls_only(#user{username = Username, tags = Tags}, false, PeerIp) ->
    case lists:member(tlsonly, Tags) of
        false ->
            ok;
        true ->
            ?LOG_ERROR("OCPP login failed: user '~s' is tagged 'tlsonly' and can "
                       "only connect over TLS", [Username]),
            auth_attempt_failed(PeerIp, Username),
            {error, access_refused}
    end.

notify_auth_result(Event, Username, PeerIp) ->
    rabbit_event:notify(Event, [{name, Username}, {peer_id, PeerIp},
                                {connection_type, network}, {protocol, ocpp}]).

-spec auth_attempt_failed(inet:ip_address(), binary()) -> ok.
auth_attempt_failed(PeerIp, Username) ->
    rabbit_core_metrics:auth_attempt_failed(PeerIp, Username, ocpp),
    timer:sleep(?SILENT_CLOSE_DELAY).

%% Allow DISCONNECT packet to be sent to client before closing the connection.
defer_close(CloseStatusCode) ->
    self() ! {stop, CloseStatusCode, server_initiated_disconnect},
    ok.

% stop_ocpp_protocol_error(State, Reason, ConnName) ->
%     ?LOG_WARNING("Web OCPP protocol error ~tp for connection ~tp", [Reason, ConnName]),
%     stop(State, ?CLOSE_PROTOCOL_ERROR, Reason).

stop(State) ->
    stop(State, ?CLOSE_NORMAL, "OCPP died").

stop(State, CloseCode, Error0) ->
    %% Every caller passes a binary, an atom or a string: rabbit_data_coercion has no tuple clause.
    Error = rabbit_data_coercion:to_binary(Error0),
    {[{close, CloseCode, Error}], State}.

-spec send_reply(binary()) -> ok. % Data is now JSON binary
send_reply(Data) ->
    self() ! {reply, Data},
    ok.

ensure_stats_timer(State) ->
    rabbit_event:ensure_stats_timer(State, #state.stats_timer, emit_stats).

maybe_emit_stats(#state{stats_timer = undefined}) ->
    ok;
maybe_emit_stats(State) ->
    rabbit_event:if_enabled(State, #state.stats_timer,
                                fun() -> emit_stats(State) end).

emit_stats(State) ->
    [{_, Pid},
     {_, RecvOct},
     {_, SendOct},
     {_, Reductions}] = infos(?SIMPLE_METRICS, State),
    Infos = infos(?OTHER_METRICS, State),
    rabbit_core_metrics:connection_stats(Pid, Infos),
    rabbit_core_metrics:connection_stats(Pid, RecvOct, SendOct, Reductions),
    State1 = rabbit_event:reset_stats_timer(State, #state.stats_timer),
    ensure_stats_timer(State1).

infos(Items, State) ->
    [{Item, i(Item, State)} || Item <- Items].

i(pid, _) ->
    self();
i(SockStat, #state{socket = Sock})
  when SockStat =:= recv_oct;
       SockStat =:= recv_cnt;
       SockStat =:= send_oct;
       SockStat =:= send_cnt;
       SockStat =:= send_pend ->
    case rabbit_net:getstat(Sock, [SockStat]) of
        {ok, [{_, N}]} when is_number(N) ->
            N;
        _ ->
            0
    end;
i(reductions, _) ->
    {reductions, Reductions} = erlang:process_info(self(), reductions),
    Reductions;
i(garbage_collection, _) ->
    rabbit_misc:get_gc_info(self());
i(protocol, #state{proto_ver = ProtocolVer} = State) ->
    ProtocolName = case i(ssl, State) of
        true  -> "WSS OCPP";
        false -> "WS OCPP"
    end,
    {ProtocolName, rabbit_web_ocpp_processor:proto_version_tuple(ProtocolVer)};
i(SSL, #state{socket = Sock})
  when SSL =:= ssl;
       SSL =:= ssl_protocol;
       SSL =:= ssl_key_exchange;
       SSL =:= ssl_cipher;
       SSL =:= ssl_hash ->
    rabbit_ssl:info(SSL, {rabbit_net:unwrap_socket(Sock),
                          rabbit_net:maybe_get_proxy_socket(Sock)});
i(ssl_login_name, #state{ssl_login_name = Val}) ->
    Val;
i(auth_mechanism, #state{auth_mechanism = Val}) ->
    Val;
i(name, S) ->
    i(conn_name, S);
i(conn_name, #state{conn_name = Val}) ->
    Val;
i(client_id, #state{client_id = Val}) ->
    Val;
i(Cert, #state{socket = Sock})
  when Cert =:= peer_cert_issuer;
       Cert =:= peer_cert_subject;
       Cert =:= peer_cert_serial_number;
       Cert =:= peer_cert_validity ->
    try rabbit_ssl:cert_info(Cert, rabbit_net:unwrap_socket(Sock))
    %% serial_number not upstream yet
    catch error:function_clause -> <<>>
    end;
i(state, S) ->
    i(connection_state, S);
i(connection_state, #state{connection_state = Val}) ->
    Val;
i(timeout, #state{idle_timeout = Val}) ->
    Val;
i(Key, #state{proc_state = PState}) ->
    % Handle the rest of the keys from processor state
    case PState of
        undefined -> undefined;
        _ -> rabbit_web_ocpp_processor:info(Key, PState)
    end.
