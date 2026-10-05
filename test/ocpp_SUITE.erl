%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2025 VAMPIRE BYTE SRL. All Rights Reserved.
%%

%% Single node tests. Most test cases reproduce one shortcoming found while
%% reviewing the plugin; the prefix of a test case name refers to that finding.
-module(ocpp_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_web_ocpp_test_util,
        [connect/2, connect/3, try_connect/3, call/3, call/4, result/3, send/2,
         recv/1, recv/2, assert_no_frame/2, assert_closed/2,
         declare_worker_queue/3, declare_worker_queue/4, publish_to_cp/3,
         publish_to_cp/4, get_message/2, message_count/2, set_env/3,
         connection_pids/1, wait_for_connections/2]).

-define(WAIT, 10000).

all() ->
    [{group, tests},
     {group, foreseen},
     {group, review},
     {group, tls},
     %% Restarts the node, so it runs last.
     {group, node_restart}].

groups() ->
    [{tests, [],
      [round_trip,
       %% C1: queue client state is thrown away on publish
       c1_quorum_queue_receives_every_publish,
       %% C2: a queue 'DOWN' leaves the charge point connected but deaf
       c2_consumer_queue_down_closes_connection,
       %% C3: client controlled keys are turned into atoms
       c3_boot_notification_keys_do_not_create_atoms,
       c3_status_notification_connectors_are_bounded,
       %% C4: client ID is used as topic binding key without validation
       c4_wildcard_client_ids_are_rejected,
       c4_client_id_length_is_limited,
       %% C5: any user can connect as any charge point
       c5_username_must_match_client_id,
       c5_username_check_can_be_disabled,
       %% 6: outbound CALLs are not tracked
       f6_one_outstanding_call_at_a_time,
       f6_call_results_pass_an_outstanding_call,
       f6_unanswered_call_times_out,
       f6_unanswered_call_is_requeued_on_disconnect,
       %% 7: responses lose their action
       f7_call_result_routing_key_carries_action,
       %% 8: no durability, no expiry
       f8_inbound_messages_are_persistent,
       f8_cp_queue_has_default_expiry,
       f8_stale_commands_expire,
       %% 9: charge point queues cannot be made highly available
       f9_cp_queue_type_is_configurable,
       %% 10: no back pressure
       f10_memory_alarm_blocks_inbound_messages,
       %% 11: unroutable CALLs are dropped silently
       f11_unroutable_call_gets_callerror,
       %% 12: idle charge points are disconnected
       f12_server_pings_keep_connection_alive,
       f12_idle_timeout_applies_without_pings,
       %% 13: unbounded frames and log lines
       f13_oversized_frame_is_rejected,
       f13_invalid_json_is_not_logged_in_full,
       %% 14: weak message validation
       f14_invalid_message_ids_close_connection,
       f14_invalid_action_closes_connection,
       %% 15: offline notification on duplicate client ID
       f15_kicked_connection_does_not_publish_offline,
       f15_slow_kicked_connection_does_not_publish_offline,
       f15_newer_connection_survives_join_of_older,
       f15_older_connection_yields_to_newer,
       %% 16: shared default exchange
       f16_amq_topic_does_not_reach_charge_points,
       %% 17: smaller issues
       f17_permission_changes_take_effect,
       f17_soap_only_versions_are_rejected,
       f17_allowed_versions_are_configurable,
       %% The plugin did not stop its listener
       listener_stops_with_plugin
      ]},
     %% Issues found after the review, while fixing the above.
     {foreseen, [],
      [%% Workers using AMQP 1.0, RabbitMQ's main protocol since 4.0
       amqp10_worker_receives_cp_messages,
       amqp10_worker_can_send_commands,
       stream_queue_receives_cp_messages,
       %% Connection limits
       vhost_connection_limit_is_enforced,
       user_connection_limit_is_enforced,
       %% PROXY protocol
       proxy_protocol_loopback_check_uses_client_address,
       proxy_protocol_peer_address_is_reported,
       %% Listing connections
       http_clients_are_not_listed_as_connections,
       %% Exchange in vhosts created after the plugin started
       exchange_is_declared_in_new_vhosts,
       %% Metrics
       global_counters_track_ocpp_traffic,
       %% Back pressure from slow queues
       slow_queue_throttles_charge_point
      ]},
     %% Findings of a review of the fixes above.
     {review, [],
      [legacy_amq_topic_binding_is_removed,
       replies_bypass_queued_calls,
       call_deadline_is_paused_while_reads_are_blocked,
       idle_timeout_is_suspended_while_reads_are_blocked,
       malformed_responses_do_not_complete_calls,
       forced_eviction_does_not_leak_consumers_gauge
      ]},
     %% Coverage of the TLS listener: OCPP security profiles 2 and 3.
     {tls, [],
      [tls_basic_auth,
       tls_client_certificate,
       tls_client_certificate_must_match_client_id,
       tls_only_user_is_rejected_on_plain_listener
      ]},
     {node_restart, [],
      [f15_offline_status_survives_node_restart]}
    ].

suite() ->
    [{timetrap, {minutes, 3}}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodename_suffix, ?MODULE}]),
    Config2 = rabbit_ct_helpers:run_setup_steps(
                rabbit_web_ocpp_test_util:merge_app_env(Config1),
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    Config3 = rabbit_web_ocpp_test_util:init_ocpp_listeners(Config2),
    Env = rabbit_ct_broker_helpers:rpc(Config3, 0, application, get_all_env,
                                       [rabbitmq_web_ocpp]),
    rabbit_ct_helpers:set_config(Config3, {initial_env, Env}).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(tls, Config) ->
    CertsDir = ?config(rmq_certsdir, Config),
    TlsConfig = [{port, rabbit_web_ocpp_test_util:tls_port(Config, 0)},
                 {cacertfile, filename:join([CertsDir, "testca", "cacert.pem"])},
                 {certfile, filename:join([CertsDir, "server", "cert.pem"])},
                 {keyfile, filename:join([CertsDir, "server", "key.pem"])},
                 {verify, verify_peer},
                 {fail_if_no_peer_cert, false}],
    set_env(Config, ssl_config, TlsConfig),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbit, ssl_cert_login_from, common_name]),
    ok = rabbit_web_ocpp_test_util:restart_plugin(Config, 0),
    Env = rabbit_ct_broker_helpers:rpc(Config, 0, application, get_all_env,
                                       [rabbitmq_web_ocpp]),
    rabbit_ct_helpers:set_config(Config, [{initial_env, Env},
                                          {initial_env_before_tls, ?config(initial_env, Config)}]);
init_per_group(_, Config) ->
    Config.

end_per_group(tls, Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
                                      [rabbitmq_web_ocpp, ssl_config]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
                                      [rabbit, ssl_cert_login_from]),
    ok = rabbit_web_ocpp_test_util:restart_plugin(Config, 0),
    rabbit_ct_helpers:set_config(Config, {initial_env, ?config(initial_env_before_tls, Config)});
end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_web_ocpp_test_util:purge_bindings(Config),
    Config1 = rabbit_ct_helpers:set_config(Config, {client_id, client_id(Testcase)}),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config) ->
    {ok, _} = rabbit_ct_broker_helpers:rpc(Config, 0, application, ensure_all_started,
                                           [rabbitmq_web_ocpp]),
    close_all_connections(Config),
    rabbit_ct_broker_helpers:clear_alarm(Config, 0, memory),
    restore_env(Config),
    rabbit_web_ocpp_test_util:purge_bindings(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

client_id(Testcase) ->
    Bin = atom_to_binary(Testcase),
    binary:part(Bin, 0, min(40, byte_size(Bin))).

cid(Config) ->
    ?config(client_id, Config).

close_all_connections(Config) ->
    Pids = connection_pids(Config),
    [begin
         %% In case a test case suspended it.
         catch rabbit_ct_broker_helpers:rpc(Config, 0, sys, resume, [Pid]),
         Pid ! {shutdown, end_of_test}
     end || Pid <- Pids],
    try wait_for_connections(Config, 0)
    catch _:_ ->
              [exit(Pid, kill) || Pid <- connection_pids(Config)],
              wait_for_connections(Config, 0)
    end.

restore_env(Config) ->
    Initial = ?config(initial_env, Config),
    Current = rabbit_ct_broker_helpers:rpc(Config, 0, application, get_all_env,
                                           [rabbitmq_web_ocpp]),
    case lists:sort(Initial) =:= lists:sort(Current) of
        true ->
            ok;
        false ->
            [ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
                                               [rabbitmq_web_ocpp, K])
             || {K, _} <- Current],
            [ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                               [rabbitmq_web_ocpp, K, V])
             || {K, V} <- Initial],
            ok = rabbit_web_ocpp_test_util:restart_plugin(Config, 0)
    end.

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

%% Both directions of the happy path.
round_trip(Config) ->
    Cid = cid(Config),
    declare_worker_queue(Config, <<"workers">>, <<"ocpp16.BootNotification.req">>),
    WS = connect(Config, Cid),
    MsgId = call(WS, <<"BootNotification">>, #{<<"chargePointVendor">> => <<"V">>,
                                                <<"chargePointModel">> => <<"M">>}),
    ok = await_count(Config, <<"workers">>, 1),
    {_RK, Props, [2, MsgId, <<"BootNotification">>, _]} = get_message(Config, <<"workers">>),
    ?assertEqual(MsgId, Props#'P_basic'.correlation_id),
    ?assertEqual(Cid, Props#'P_basic'.reply_to),
    publish_to_cp(Config, Cid, [3, MsgId, #{<<"status">> => <<"Accepted">>}]),
    ?assertMatch([3, MsgId, #{<<"status">> := <<"Accepted">>}], recv(WS)).

%% Every publish used a fresh queue client state. A quorum queue then sees
%% the same enqueuer with sequence number 1 over and over, treats all but the
%% first message as duplicates and drops them.
c1_quorum_queue_receives_every_publish(Config) ->
    declare_worker_queue(Config, <<"workers">>, <<"ocpp16.Heartbeat.req">>,
                         [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    WS = connect(Config, cid(Config)),
    [call(WS, <<"Heartbeat">>, #{}) || _ <- lists:seq(1, 5)],
    ok = await_count(Config, <<"workers">>, 5),
    timer:sleep(500),
    ?assertEqual(5, message_count(Config, <<"workers">>)).

%% When the process of the charge point queue dies (e.g. because the node
%% hosting it restarts), the consumer is gone. The WebSocket must be closed so
%% that the charge point reconnects and consumes again, instead of looking
%% online while it never receives a command.
c2_consumer_queue_down_closes_connection(Config) ->
    Cid = cid(Config),
    WS = connect(Config, Cid),
    QPid = rabbit_web_ocpp_test_util:cp_queue_pid(Config, Cid),
    true = rabbit_ct_broker_helpers:rpc(Config, 0, erlang, exit, [QPid, kill]),
    _ = assert_closed(WS, 5000),
    %% After reconnecting the charge point receives commands again.
    wait_for_connections(Config, 0),
    WS2 = connect(Config, Cid),
    publish_to_cp(Config, Cid, [2, <<"csms-1">>, <<"Reset">>, #{<<"type">> => <<"Soft">>}]),
    ?assertMatch([2, <<"csms-1">>, <<"Reset">>, _], recv(WS2)).

%% Arbitrary BootNotification keys must not end up in the atom table.
c3_boot_notification_keys_do_not_create_atoms(Config) ->
    declare_worker_queue(Config, <<"workers">>, <<"ocpp16.BootNotification.req">>),
    WS = connect(Config, cid(Config)),
    Before = atom_count(Config),
    Junk = maps:from_list([{<<"k", (integer_to_binary(erlang:unique_integer([positive])))/binary,
                             "_", (integer_to_binary(I))/binary>>, <<"v">>}
                           || I <- lists:seq(1, 2000)]),
    call(WS, <<"BootNotification">>, Junk#{<<"chargePointVendor">> => <<"VendorX">>,
                                           <<"chargePointModel">> => <<"ModelY">>}),
    ok = await_count(Config, <<"workers">>, 1),
    %% What the management UI and `rabbitmqctl list_web_ocpp_connections
    %% client_properties` do.
    Props = client_properties(Config),
    ?assert(atom_count(Config) - Before < 100),
    %% Well known properties are still shown.
    ?assertEqual(<<"VendorX">>, prop(<<"chargePointVendor">>, Props)),
    ?assertEqual(<<"ModelY">>, prop(<<"chargePointModel">>, Props)).

%% Each StatusNotification connectorId added one property (and one atom).
c3_status_notification_connectors_are_bounded(Config) ->
    declare_worker_queue(Config, <<"workers">>, <<"ocpp16.StatusNotification.req">>),
    WS = connect(Config, cid(Config)),
    Before = atom_count(Config),
    N = 500,
    [call(WS, <<"StatusNotification">>, #{<<"connectorId">> => 1000 + I,
                                          <<"status">> => <<"Available">>,
                                          <<"errorCode">> => <<"NoError">>})
     || I <- lists:seq(1, N)],
    call(WS, <<"StatusNotification">>, #{<<"connectorId">> => 1,
                                         <<"status">> => <<"Charging">>,
                                         <<"errorCode">> => <<"NoError">>}),
    ok = await_count(Config, <<"workers">>, N + 1),
    Props = client_properties(Config),
    ?assert(atom_count(Config) - Before < 100),
    ?assert(length(Props) < 50),
    %% Realistic connector IDs are still shown.
    ?assertNotEqual(undefined, prop(<<"statusConnectorId1">>, Props)).

%% Client IDs are used as binding keys on a topic exchange. '#' would receive
%% the traffic of all charge points, '*' all replies, and IDs containing dots
%% could match worker bindings.
c4_wildcard_client_ids_are_rejected(Config) ->
    [begin
         {_WS, Status} = try_connect(Config, Id, #{}),
         ?assertEqual({Id, 400}, {Id, Status})
     end || Id <- [<<"#">>, <<"*">>, <<"ocpp16.Heartbeat.req">>, <<"x.StartTransaction">>,
                   <<"a b">>]].

%% OCPP 2.0.1 limits the identity to 48 characters.
c4_client_id_length_is_limited(Config) ->
    Long = binary:copy(<<"a">>, 49),
    {_, Status} = try_connect(Config, Long, #{}),
    ?assertEqual(400, Status),
    WS = connect(Config, binary:copy(<<"b">>, 48)),
    ?assert(rabbit_web_ocpp_test_util:is_open(WS)).

%% OCPP security profiles 1 and 2: the Basic auth username is the charge point
%% identity. Otherwise one leaked password lets anybody take over any charge
%% point's session.
c5_username_must_match_client_id(Config) ->
    rabbit_web_ocpp_test_util:ensure_user(Config, <<"fleet">>),
    {_, Status} = try_connect(Config, cid(Config), #{user => <<"fleet">>, create_user => false}),
    ?assertEqual(401, Status).

c5_username_check_can_be_disabled(Config) ->
    set_env(Config, username_must_match_client_id, false),
    rabbit_web_ocpp_test_util:ensure_user(Config, <<"fleet">>),
    WS = connect(Config, cid(Config), #{user => <<"fleet">>, create_user => false}),
    ?assert(rabbit_web_ocpp_test_util:is_open(WS)).

%% OCPP-J: a sender must not send a new CALL before the previous one was
%% answered or timed out.
f6_one_outstanding_call_at_a_time(Config) ->
    Cid = cid(Config),
    WS = connect(Config, Cid),
    publish_to_cp(Config, Cid, [2, <<"csms-1">>, <<"GetConfiguration">>, #{}]),
    publish_to_cp(Config, Cid, [2, <<"csms-2">>, <<"GetConfiguration">>, #{}]),
    ?assertMatch([2, <<"csms-1">>, _, _], recv(WS)),
    assert_no_frame(WS, 1000),
    result(WS, <<"csms-1">>, #{}),
    ?assertMatch([2, <<"csms-2">>, _, _], recv(WS)).

%% Holding back CALLs must not hold back the answers to the charge point's
%% own requests.
f6_call_results_pass_an_outstanding_call(Config) ->
    Cid = cid(Config),
    WS = connect(Config, Cid),
    publish_to_cp(Config, Cid, [2, <<"csms-1">>, <<"GetConfiguration">>, #{}]),
    ?assertMatch([2, <<"csms-1">>, _, _], recv(WS)),
    publish_to_cp(Config, Cid, [3, <<"cp-1">>, #{<<"currentTime">> => <<"now">>}]),
    ?assertMatch([3, <<"cp-1">>, _], recv(WS)).

f6_unanswered_call_times_out(Config) ->
    set_env(Config, call_timeout, 1000),
    Cid = cid(Config),
    WS = connect(Config, Cid),
    publish_to_cp(Config, Cid, [2, <<"csms-1">>, <<"GetConfiguration">>, #{}]),
    publish_to_cp(Config, Cid, [2, <<"csms-2">>, <<"GetConfiguration">>, #{}]),
    ?assertMatch([2, <<"csms-1">>, _, _], recv(WS)),
    ?assertMatch([2, <<"csms-2">>, _, _], recv(WS, 5000)).

%% A CALL was acked when it was queued for sending. If the charge point goes
%% away before answering, the command must not be lost.
f6_unanswered_call_is_requeued_on_disconnect(Config) ->
    Cid = cid(Config),
    WS = connect(Config, Cid),
    publish_to_cp(Config, Cid, [2, <<"csms-1">>, <<"GetConfiguration">>, #{}]),
    ?assertMatch([2, <<"csms-1">>, _, _], recv(WS)),
    rfc6455_client:close(WS),
    wait_for_connections(Config, 0),
    ok = await_count(Config, <<"ocpp.", Cid/binary>>, 1).

%% Answers of the charge point were routed as ocpp16.response.conf, so a
%% worker could not tell which command they belong to.
f7_call_result_routing_key_carries_action(Config) ->
    Cid = cid(Config),
    declare_worker_queue(Config, <<"workers">>, <<"ocpp16.GetConfiguration.conf">>),
    WS = connect(Config, Cid),
    publish_to_cp(Config, Cid, [2, <<"csms-7">>, <<"GetConfiguration">>, #{}]),
    ?assertMatch([2, <<"csms-7">>, _, _], recv(WS)),
    result(WS, <<"csms-7">>, #{<<"configurationKey">> => []}),
    ok = await_count(Config, <<"workers">>, 1),
    {RK, Props, Frame} = get_message(Config, <<"workers">>),
    ?assertEqual(<<"ocpp16.GetConfiguration.conf">>, RK),
    ?assertEqual(<<"GetConfiguration">>, Props#'P_basic'.type),
    ?assertMatch([3, <<"csms-7">>, _], Frame).

f8_inbound_messages_are_persistent(Config) ->
    declare_worker_queue(Config, <<"workers">>, <<"ocpp16.Heartbeat.req">>),
    WS = connect(Config, cid(Config)),
    call(WS, <<"Heartbeat">>, #{}),
    ok = await_count(Config, <<"workers">>, 1),
    {_, Props, _} = get_message(Config, <<"workers">>),
    ?assertEqual(2, Props#'P_basic'.delivery_mode).

%% Commands must not wait forever for a charge point that does not come back,
%% and queues of decommissioned charge points must go away.
f8_cp_queue_has_default_expiry(Config) ->
    Cid = cid(Config),
    _WS = connect(Config, Cid),
    QName = rabbit_misc:r(<<"/">>, queue, <<"ocpp.", Cid/binary>>),
    {ok, Q} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [QName]),
    Args = amqqueue:get_arguments(Q),
    ?assertMatch({_, _}, rabbit_misc:table_lookup(Args, <<"x-message-ttl">>)),
    ?assertMatch({_, _}, rabbit_misc:table_lookup(Args, <<"x-expires">>)).

f8_stale_commands_expire(Config) ->
    set_env(Config, queue_message_ttl, 200),
    Cid = cid(Config),
    WS = connect(Config, Cid),
    rfc6455_client:close(WS),
    wait_for_connections(Config, 0),
    publish_to_cp(Config, Cid, [2, <<"csms-1">>, <<"Reset">>, #{<<"type">> => <<"Hard">>}]),
    timer:sleep(1000),
    WS2 = connect(Config, Cid),
    assert_no_frame(WS2, 1000).

%% Quorum queues survive the loss of a node, see also ocpp_cluster_SUITE.
f9_cp_queue_type_is_configurable(Config) ->
    set_env(Config, queue_type, quorum),
    Cid = cid(Config),
    WS = connect(Config, Cid),
    QName = rabbit_misc:r(<<"/">>, queue, <<"ocpp.", Cid/binary>>),
    {ok, Q} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [QName]),
    ?assertEqual(rabbit_quorum_queue, amqqueue:get_type(Q)),
    publish_to_cp(Config, Cid, [2, <<"csms-1">>, <<"GetConfiguration">>, #{}]),
    publish_to_cp(Config, Cid, [2, <<"csms-2">>, <<"GetConfiguration">>, #{}]),
    ?assertMatch([2, <<"csms-1">>, _, _], recv(WS)),
    result(WS, <<"csms-1">>, #{}),
    ?assertMatch([2, <<"csms-2">>, _, _], recv(WS)).

%% Charge points must stop publishing while the broker is under a resource alarm.
f10_memory_alarm_blocks_inbound_messages(Config) ->
    declare_worker_queue(Config, <<"workers">>, <<"ocpp16.Heartbeat.req">>),
    WS = connect(Config, cid(Config)),
    rabbit_ct_broker_helpers:set_alarm(Config, 0, memory),
    timer:sleep(500),
    call(WS, <<"Heartbeat">>, #{}),
    timer:sleep(1000),
    ?assertEqual(0, message_count(Config, <<"workers">>)),
    rabbit_ct_broker_helpers:clear_alarm(Config, 0, memory),
    ok = await_count(Config, <<"workers">>, 1).

%% Without a worker for an action, the charge point waited for a timeout.
f11_unroutable_call_gets_callerror(Config) ->
    WS = connect(Config, cid(Config)),
    MsgId = call(WS, <<"UnboundAction">>, #{}),
    ?assertMatch([4, MsgId, <<"NotImplemented">>, _, #{}], recv(WS)).

%% The server never pinged, so charge points that do not ping themselves
%% and send heartbeats less often than the idle timeout were disconnected.
f12_server_pings_keep_connection_alive(Config) ->
    set_env(Config, cowboy_ws_opts, [{idle_timeout, 2000}]),
    ok = rabbit_web_ocpp_test_util:restart_plugin(Config, 0),
    WS = connect(Config, cid(Config)),
    assert_no_frame(WS, 5000),
    ?assertEqual(1, length(connection_pids(Config))).

%% Control for the above: without pings, the idle timeout does apply.
f12_idle_timeout_applies_without_pings(Config) ->
    set_env(Config, cowboy_ws_opts, [{idle_timeout, 2000}]),
    set_env(Config, ws_ping_interval, 0),
    ok = rabbit_web_ocpp_test_util:restart_plugin(Config, 0),
    WS = connect(Config, cid(Config)),
    _ = assert_closed(WS, 5000).

%% Stopping (or disabling) the plugin must stop its listener, so that it can
%% be started again with a new configuration.
listener_stops_with_plugin(Config) ->
    Port = rabbit_web_ocpp_test_util:port(Config, 0),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, stop, [rabbitmq_web_ocpp]),
    ?assertEqual({error, econnrefused}, gen_tcp:connect("127.0.0.1", Port, [])),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, start, [rabbitmq_web_ocpp]),
    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, []),
    gen_tcp:close(Sock).

f13_oversized_frame_is_rejected(Config) ->
    WS = connect(Config, cid(Config)),
    call(WS, <<"DataTransfer">>, #{<<"vendorId">> => <<"v">>,
                                   <<"data">> => binary:copy(<<"a">>, 2 * 1024 * 1024)}),
    ?assertEqual(1009, assert_closed(WS, 5000)).

f13_invalid_json_is_not_logged_in_full(Config) ->
    [LogFile | _] = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit, log_locations, []),
    WS = connect(Config, cid(Config)),
    timer:sleep(500),
    Before = filelib:file_size(LogFile),
    rfc6455_client:send(WS, [<<"[2, \"1\", \"Heartbeat\", ">>, binary:copy(<<"x">>, 200000)]),
    ?assertEqual(1007, assert_closed(WS, 5000)),
    timer:sleep(1000),
    Logged = filelib:file_size(LogFile) - Before,
    ct:pal("Logged ~b bytes", [Logged]),
    ?assert(Logged < 20000).

f14_invalid_message_ids_close_connection(Config) ->
    Cid = cid(Config),
    declare_worker_queue(Config, <<"workers">>, <<"#">>),
    [begin
         WS = connect(Config, Cid),
         send(WS, Frame),
         ?assertEqual({Frame, 1002}, {Frame, assert_closed(WS, 5000)}),
         wait_for_connections(Config, 0)
     end || Frame <- [[2, #{<<"a">> => 1}, <<"Heartbeat">>, #{}],
                      [2, [1, 2], <<"Heartbeat">>, #{}],
                      [2, binary:copy(<<"1">>, 37), <<"Heartbeat">>, #{}]]],
    %% Nothing but the offline status notifications got published.
    [?assertMatch({_, _, [2, _, <<"StatusNotification">>, _]}, M)
     || M <- rabbit_web_ocpp_test_util:get_messages(Config, <<"workers">>)].

f14_invalid_action_closes_connection(Config) ->
    Cid = cid(Config),
    declare_worker_queue(Config, <<"workers">>, <<"#">>),
    [begin
         WS = connect(Config, Cid),
         send(WS, Frame),
         ?assertEqual({Frame, 1002}, {Frame, assert_closed(WS, 5000)}),
         wait_for_connections(Config, 0)
     end || Frame <- [[2, <<"1">>, 123, #{}],
                      [2, <<"2">>, <<>>, #{}],
                      [2, <<"3">>, <<"Heartbeat">>, 5]]],
    [?assertMatch({_, _, [2, _, <<"StatusNotification">>, _]}, M)
     || M <- rabbit_web_ocpp_test_util:get_messages(Config, <<"workers">>)].

%% A charge point that reconnects is online: its old connection must not
%% announce it offline, which could also land after the new online status.
f15_kicked_connection_does_not_publish_offline(Config) ->
    Cid = cid(Config),
    declare_worker_queue(Config, <<"workers">>, <<"ocpp16.StatusNotification.req">>),
    WS1 = connect(Config, Cid),
    WS2 = connect(Config, Cid),
    _ = assert_closed(WS1, 5000),
    wait_for_connections(Config, 1),
    timer:sleep(500),
    ?assertEqual(0, message_count(Config, <<"workers">>)),
    rfc6455_client:close(WS2),
    ok = await_count(Config, <<"workers">>, 1).

%% The new connection stops waiting for the old one after a timeout. It must
%% still take over, and the old one must not announce the charge point offline.
f15_slow_kicked_connection_does_not_publish_offline(Config) ->
    Cid = cid(Config),
    declare_worker_queue(Config, <<"workers">>, <<"ocpp16.StatusNotification.req">>),
    WS1 = connect(Config, Cid),
    [Pid1] = connection_pids(Config),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, sys, suspend, [Pid1]),
    WS2 = connect(Config, Cid),
    %% The new charge point session announces itself.
    call(WS2, <<"StatusNotification">>, #{<<"connectorId">> => 0,
                                          <<"status">> => <<"Available">>,
                                          <<"errorCode">> => <<"NoError">>}),
    ok = await_count(Config, <<"workers">>, 1),
    catch rabbit_ct_broker_helpers:rpc(Config, 0, sys, resume, [Pid1]),
    _ = assert_closed(WS1, 5000),
    wait_for_connections(Config, 1),
    timer:sleep(500),
    Msgs = rabbit_web_ocpp_test_util:get_messages(Config, <<"workers">>),
    %% The last word on the charge point is that it is available.
    ?assertMatch([{_, _, [2, _, _, #{<<"status">> := <<"Available">>}]}], Msgs).

%% After a network partition heals, both connections with the same client ID
%% see each other's join. Only the older one may close.
f15_newer_connection_survives_join_of_older(Config) ->
    Cid = cid(Config),
    WS = connect(Config, Cid),
    [Pid] = connection_pids(Config),
    Older = spawn_older_peer(),
    Pid ! {make_ref(), join, {<<"/">>, Cid}, [Older]},
    assert_no_frame(WS, 1000),
    ?assertEqual([Pid], connection_pids(Config)),
    %% The peer was told about this, younger, connection.
    Older ! {report, self()},
    receive {older_peer_got, Msgs} -> ?assertNotEqual([], Msgs)
    after 5000 -> ct:fail(no_report)
    end.

f15_older_connection_yields_to_newer(Config) ->
    Cid = cid(Config),
    WS = connect(Config, Cid),
    [Pid] = connection_pids(Config),
    Newer = spawn_older_peer(),
    Pid ! {'$gen_cast', {duplicate_id_check, Newer, os:system_time(millisecond) + 60000}},
    _ = assert_closed(WS, 5000).

%% Publishing to amq.topic, e.g. from an MQTT or STOMP client with topic CP001,
%% must not reach charge point CP001.
f16_amq_topic_does_not_reach_charge_points(Config) ->
    Cid = cid(Config),
    WS = connect(Config, Cid),
    ?assertNotEqual(<<"amq.topic">>, rabbit_web_ocpp_test_util:exchange(Config)),
    publish_to_cp(Config, <<"amq.topic">>, Cid, [2, <<"mqtt-1">>, <<"Reset">>, #{}]),
    assert_no_frame(WS, 1000),
    publish_to_cp(Config, Cid, [2, <<"csms-1">>, <<"Reset">>, #{}]),
    ?assertMatch([2, <<"csms-1">>, _, _], recv(WS)).

%% Cached permission checks never expired, so revoking permissions had no
%% effect on established connections.
f17_permission_changes_take_effect(Config) ->
    set_env(Config, permission_cache_ttl, 200),
    Cid = cid(Config),
    declare_worker_queue(Config, <<"workers">>, <<"ocpp16.Heartbeat.req">>),
    WS = connect(Config, Cid),
    call(WS, <<"Heartbeat">>, #{}),
    ok = await_count(Config, <<"workers">>, 1),
    ok = rabbit_ct_broker_helpers:set_permissions(Config, Cid, <<"/">>,
                                                  <<".*">>, <<"^$">>, <<".*">>),
    timer:sleep(500),
    call(WS, <<"Heartbeat">>, #{}),
    ?assertEqual(1008, assert_closed(WS, 5000)),
    ?assertEqual(1, message_count(Config, <<"workers">>)).

%% OCPP 1.2 and 1.5 only exist as SOAP.
f17_soap_only_versions_are_rejected(Config) ->
    [begin
         {_, Status} = try_connect(Config, cid(Config), #{protos => [P]}),
         ?assertEqual({P, 400}, {P, Status})
     end || P <- ["ocpp1.2", "ocpp1.5"]].

f17_allowed_versions_are_configurable(Config) ->
    set_env(Config, protocols, [<<"ocpp2.0.1">>]),
    {_, Status} = try_connect(Config, cid(Config), #{protos => ["ocpp1.6"]}),
    ?assertEqual(400, Status),
    WS = connect(Config, cid(Config), #{protos => ["ocpp1.6", "ocpp2.0.1"]}),
    ?assert(rabbit_web_ocpp_test_util:is_open(WS)).

%% -------------------------------------------------------------------
%% Issues found after the review
%% -------------------------------------------------------------------

%% mc_ocpp converted to AMQP 1.0 with untagged property values.
amqp10_worker_receives_cp_messages(Config) ->
    Cid = cid(Config),
    declare_worker_queue(Config, <<"workers">>, <<"ocpp16.Heartbeat.req">>),
    WS = connect(Config, Cid),
    MsgId = call(WS, <<"Heartbeat">>, #{}),
    ok = await_count(Config, <<"workers">>, 1),
    {ok, Msg} = rabbit_web_ocpp_test_util:amqp10_get(Config, <<"workers">>),
    ?assertMatch([2, MsgId, <<"Heartbeat">>, _], json:decode(amqp10_msg:body_bin(Msg))),
    Props = amqp10_msg:properties(Msg),
    ?assertEqual(MsgId, maps:get(correlation_id, Props)),
    ?assertEqual(Cid, maps:get(reply_to, Props)),
    ?assertEqual(<<"Heartbeat">>, maps:get(subject, Props)).

%% mc_ocpp expected a list of AMQP 1.0 sections, but gets mc_amqp's state.
amqp10_worker_can_send_commands(Config) ->
    Cid = cid(Config),
    WS = connect(Config, Cid),
    ?assertEqual(accepted,
                 rabbit_web_ocpp_test_util:amqp10_publish_to_cp(
                   Config, Cid, [2, <<"csms-1">>, <<"Reset">>, #{<<"type">> => <<"Soft">>}])),
    ?assertMatch([2, <<"csms-1">>, <<"Reset">>, _], recv(WS)).

%% Streams store messages in AMQP 1.0 format, e.g. for auditing.
stream_queue_receives_cp_messages(Config) ->
    declare_worker_queue(Config, <<"audit">>, <<"ocpp16.#">>,
                         [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    WS = connect(Config, cid(Config)),
    MsgIds = [call(WS, <<"Heartbeat">>, #{}) || _ <- [1, 2]],
    Frames = consume_stream(Config, <<"audit">>, 2),
    ?assertEqual(MsgIds, [MsgId || [2, MsgId, _, _] <- Frames]),
    assert_no_frame(WS, 100).

%% Connection limits of vhosts and users (e.g. set by rabbitmqctl
%% set_vhost_limits) did not apply to OCPP connections.
vhost_connection_limit_is_enforced(Config) ->
    Vhost = <<"limited">>,
    ok = rabbit_ct_broker_helpers:add_vhost(Config, Vhost),
    try
        ok = rabbit_ct_broker_helpers:set_vhost_limit(Config, 0, Vhost, max_connections, 1),
        _WS = connect(Config, <<"limited-cp1">>, #{vhost => Vhost}),
        {_, Status} = try_connect(Config, <<"limited-cp2">>, #{vhost => Vhost}),
        ?assertEqual(429, Status)
    after
        close_all_connections(Config),
        rabbit_ct_broker_helpers:delete_vhost(Config, Vhost)
    end.

user_connection_limit_is_enforced(Config) ->
    Cid = cid(Config),
    rabbit_web_ocpp_test_util:ensure_user(Config, Cid),
    ok = rabbit_ct_broker_helpers:set_user_limits(Config, Cid, #{max_connections => 0}),
    {_, Status} = try_connect(Config, Cid, #{create_user => false}),
    ?assertEqual(429, Status).

%% Behind a load balancer using the PROXY protocol, every client appeared to
%% connect from the load balancer's address, e.g. from localhost.
proxy_protocol_loopback_check_uses_client_address(Config) ->
    set_env(Config, proxy_protocol, true),
    ok = rabbit_web_ocpp_test_util:restart_plugin(Config, 0),
    {ok, LoopbackUsers} = rabbit_ct_broker_helpers:rpc(Config, 0, application, get_env,
                                                       [rabbit, loopback_users]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbit, loopback_users, [<<"guest">>]]),
    try
        {_, Status} = try_connect(Config, <<"guest">>,
                                  #{password => <<"guest">>, create_user => false,
                                    tcp_preface => proxy_header(Config)}),
        ?assertEqual(401, Status)
    after
        ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                          [rabbit, loopback_users, LoopbackUsers])
    end.

proxy_protocol_peer_address_is_reported(Config) ->
    set_env(Config, proxy_protocol, true),
    ok = rabbit_web_ocpp_test_util:restart_plugin(Config, 0),
    _WS = connect(Config, cid(Config), #{tcp_preface => proxy_header(Config)}),
    [Pid] = connection_pids(Config),
    ?assertEqual([{peer_host, {192, 168, 1, 5}}, {peer_port, 40000}],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_web_ocpp_handler, info,
                                              [Pid, [peer_host, peer_port]])).

%% Plain HTTP connections (e.g. an idle keep-alive connection, or one that
%% is being authenticated) were listed as OCPP connections. Asking them for
%% their details timed out, which broke `rabbitmqctl list_web_ocpp_connections`.
http_clients_are_not_listed_as_connections(Config) ->
    Cid = cid(Config),
    {ok, Idle} = gen_tcp:connect("127.0.0.1", rabbit_web_ocpp_test_util:port(Config, 0), []),
    try
        _WS = connect(Config, Cid),
        ?assertEqual(1, length(connection_pids(Config))),
        {ok, Out} = rabbit_ct_broker_helpers:rabbitmqctl(
                      Config, 0, ["list_web_ocpp_connections", "client_id"], 60000),
        ?assertNotEqual(nomatch, binary:match(rabbit_data_coercion:to_binary(Out), Cid))
    after
        gen_tcp:close(Idle)
    end.

%% Unlike amq.topic, the plugin's exchange does not exist by default: it must
%% also be declared in vhosts created later, so that workers can bind to it.
exchange_is_declared_in_new_vhosts(Config) ->
    Vhost = <<"created-later">>,
    ok = rabbit_ct_broker_helpers:add_vhost(Config, Vhost),
    try
        XName = rabbit_misc:r(Vhost, exchange, rabbit_web_ocpp_test_util:exchange(Config)),
        rabbit_ct_helpers:await_condition(
          fun() ->
                  element(1, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange,
                                                          lookup, [XName])) =:= ok
          end, 5000)
    after
        rabbit_ct_broker_helpers:delete_vhost(Config, Vhost)
    end.

%% Prometheus metrics: consumers were never decremented, received and
%% delivered messages never counted.
global_counters_track_ocpp_traffic(Config) ->
    Cid = cid(Config),
    declare_worker_queue(Config, <<"workers">>, <<"ocpp16.Heartbeat.req">>),
    Before = counters(Config),
    [begin
         WS = connect(Config, Cid),
         call(WS, <<"Heartbeat">>, #{}),
         publish_to_cp(Config, Cid, [3, <<"cp-1">>, #{}]),
         ?assertMatch([3, <<"cp-1">>, _], recv(WS)),
         rfc6455_client:close(WS),
         wait_for_connections(Config, 0)
     end || _ <- lists:seq(1, 3)],
    After = counters(Config),
    ?assertEqual(maps:get(consumers, Before), maps:get(consumers, After)),
    ?assertEqual(maps:get(messages_received_total, Before) + 3,
                 maps:get(messages_received_total, After)),
    ?assertEqual(maps:get(messages_delivered_total, Before) + 3,
                 maps:get(messages_delivered_total, After)).

%% Publishes did not use credit flow: a charge point could flood the mailbox
%% of a queue that cannot keep up.
slow_queue_throttles_charge_point(Config) ->
    declare_worker_queue(Config, <<"workers">>, <<"ocpp16.DataTransfer.req">>),
    QPid = queue_pid(Config, <<"workers">>),
    WS = connect(Config, cid(Config)),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, sys, suspend, [QPid]),
    N = 2000,
    Data = binary:copy(<<"x">>, 1000),
    try
        [call(WS, <<"DataTransfer">>, #{<<"vendorId">> => <<"v">>, <<"data">> => Data})
         || _ <- lists:seq(1, N)],
        timer:sleep(3000),
        {message_queue_len, Len} = rabbit_ct_broker_helpers:rpc(Config, 0, erlang, process_info,
                                                                [QPid, message_queue_len]),
        ct:pal("Queue process mailbox: ~b messages", [Len]),
        ?assert(Len < N div 2)
    after
        rabbit_ct_broker_helpers:rpc(Config, 0, sys, resume, [QPid])
    end,
    %% Nothing is lost.
    rabbit_ct_helpers:await_condition(
      fun() -> message_count(Config, <<"workers">>) =:= N end, 60000).

%% -------------------------------------------------------------------
%% Review findings
%% -------------------------------------------------------------------

%% Queues of charge points that connected before the default exchange changed
%% are still bound to amq.topic, so MQTT and STOMP clients could still reach
%% them.
legacy_amq_topic_binding_is_removed(Config) ->
    Cid = cid(Config),
    QName = <<"ocpp.", Cid/binary>>,
    rabbit_web_ocpp_test_util:with_channel(
      Config,
      fun(Ch) ->
              #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                                                              durable = true}),
              #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName,
                                                                        exchange = <<"amq.topic">>,
                                                                        routing_key = Cid})
      end),
    WS = connect(Config, Cid),
    publish_to_cp(Config, <<"amq.topic">>, Cid, [2, <<"mqtt-1">>, <<"Reset">>, #{}]),
    assert_no_frame(WS, 1000),
    publish_to_cp(Config, Cid, [2, <<"csms-1">>, <<"Reset">>, #{}]),
    ?assertMatch([2, <<"csms-1">>, _, _], recv(WS)).

%% Unanswered CALLs held back behind the outstanding one used up the
%% prefetch window: an answer to a request of the charge point queued behind
%% them could not be delivered until the CALLs timed out.
replies_bypass_queued_calls(Config) ->
    set_env(Config, call_timeout, 20000),
    [begin
         set_env(Config, queue_type, QueueType),
         set_env(Config, prefetch_count, Prefetch),
         Cid = iolist_to_binary(io_lib:format("replies-~s-~b", [QueueType, Prefetch])),
         WS = connect(Config, Cid),
         [publish_to_cp(Config, Cid, [2, <<"csms-", (integer_to_binary(I))/binary>>,
                                      <<"GetConfiguration">>, #{}])
          || I <- lists:seq(1, Prefetch + 1)],
         publish_to_cp(Config, Cid, [3, <<"cp-1">>, #{<<"currentTime">> => <<"now">>}]),
         ?assertMatch([2, <<"csms-1">>, _, _], recv(WS)),
         ?assertMatch({_, _, [3, <<"cp-1">>, _]}, {QueueType, Prefetch, recv(WS, 5000)}),
         rfc6455_client:close(WS),
         wait_for_connections(Config, 0)
     end || QueueType <- [classic, quorum], Prefetch <- [1, 10]].

%% While the broker does not read from the charge point (resource alarm,
%% credit flow), its answer cannot arrive: the CALL must not time out.
call_deadline_is_paused_while_reads_are_blocked(Config) ->
    set_env(Config, call_timeout, 1000),
    Cid = cid(Config),
    declare_worker_queue(Config, <<"workers">>, <<"ocpp16.GetConfiguration.conf">>),
    WS = connect(Config, Cid),
    publish_to_cp(Config, Cid, [2, <<"csms-1">>, <<"GetConfiguration">>, #{}]),
    publish_to_cp(Config, Cid, [2, <<"csms-2">>, <<"GetConfiguration">>, #{}]),
    ?assertMatch([2, <<"csms-1">>, _, _], recv(WS)),
    rabbit_ct_broker_helpers:set_alarm(Config, 0, memory),
    timer:sleep(500),
    result(WS, <<"csms-1">>, #{}),
    %% The next CALL is not sent while the answer to the first is unread.
    assert_no_frame(WS, 2500),
    rabbit_ct_broker_helpers:clear_alarm(Config, 0, memory),
    ok = await_count(Config, <<"workers">>, 1),
    ?assertMatch({<<"ocpp16.GetConfiguration.conf">>, _, [3, <<"csms-1">>, _]},
                 get_message(Config, <<"workers">>)),
    ?assertMatch([2, <<"csms-2">>, _, _], recv(WS)).

%% Pongs of a healthy charge point are not read while reads are blocked.
idle_timeout_is_suspended_while_reads_are_blocked(Config) ->
    set_env(Config, cowboy_ws_opts, [{idle_timeout, 2000}]),
    ok = rabbit_web_ocpp_test_util:restart_plugin(Config, 0),
    declare_worker_queue(Config, <<"workers">>, <<"ocpp16.Heartbeat.req">>),
    WS = connect(Config, cid(Config)),
    rabbit_ct_broker_helpers:set_alarm(Config, 0, memory),
    timer:sleep(500),
    call(WS, <<"Heartbeat">>, #{}),
    assert_no_frame(WS, 4000),
    rabbit_ct_broker_helpers:clear_alarm(Config, 0, memory),
    ok = await_count(Config, <<"workers">>, 1),
    ?assertEqual(1, length(connection_pids(Config))).

%% A malformed answer must not complete (acknowledge) the CALL.
malformed_responses_do_not_complete_calls(Config) ->
    Cid = cid(Config),
    %% Declares the charge point queue.
    WS0 = connect(Config, Cid),
    rfc6455_client:close(WS0),
    wait_for_connections(Config, 0),
    publish_to_cp(Config, Cid, [2, <<"csms-1">>, <<"GetConfiguration">>, #{}]),
    [begin
         WS = connect(Config, Cid),
         ?assertMatch([2, <<"csms-1">>, _, _], recv(WS)),
         send(WS, Frame),
         ?assertEqual({Frame, 1002}, {Frame, assert_closed(WS, 5000)}),
         wait_for_connections(Config, 0)
     end || Frame <- [[3, <<"csms-1">>, 42],
                      [4, <<"csms-1">>, <<"GenericError">>, 5, #{}],
                      [4, <<"csms-1">>, <<"GenericError">>, <<"description">>, [1]]]],
    %% The CALL is still there.
    WS2 = connect(Config, Cid),
    ?assertMatch([2, <<"csms-1">>, _, _], recv(WS2)).

%% A duplicate connection that does not terminate in time is killed, which
%% skips its terminate callback.
forced_eviction_does_not_leak_consumers_gauge(Config) ->
    Cid = cid(Config),
    Before = maps:get(consumers, counters(Config)),
    _WS1 = connect(Config, Cid),
    [Pid1] = connection_pids(Config),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, sys, suspend, [Pid1]),
    WS2 = connect(Config, Cid),
    rfc6455_client:close(WS2),
    wait_for_connections(Config, 0),
    rabbit_ct_helpers:await_condition(
      fun() -> maps:get(consumers, counters(Config)) =:= Before end, 5000).

%% -------------------------------------------------------------------
%% TLS listener
%% -------------------------------------------------------------------

%% Security profile 2: TLS with Basic auth.
tls_basic_auth(Config) ->
    WS = connect(Config, cid(Config), #{tls => []}),
    [Pid] = connection_pids(Config),
    ?assertEqual([{ssl, true}, {auth_mechanism, <<"BASIC">>}],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_web_ocpp_handler, info,
                                              [Pid, [ssl, auth_mechanism]])),
    ?assert(rabbit_web_ocpp_test_util:is_open(WS)).

%% Security profile 3: the client certificate identifies the charge point.
%% The test certificates' common name is the host name.
tls_client_certificate(Config) ->
    Cid = hostname(),
    rabbit_web_ocpp_test_util:ensure_user(Config, Cid),
    _WS = connect(Config, Cid, #{tls => client_cert_opts(Config), user => none,
                                 create_user => false}),
    [Pid] = connection_pids(Config),
    ?assertEqual([{auth_mechanism, <<"MTLS">>}, {user, Cid}],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_web_ocpp_handler, info,
                                              [Pid, [auth_mechanism, user]])).

tls_client_certificate_must_match_client_id(Config) ->
    {_, Status} = try_connect(Config, cid(Config), #{tls => client_cert_opts(Config)}),
    ?assertEqual(401, Status).

tls_only_user_is_rejected_on_plain_listener(Config) ->
    Cid = cid(Config),
    rabbit_web_ocpp_test_util:ensure_user(Config, Cid),
    ok = rabbit_ct_broker_helpers:set_user_tags(Config, 0, Cid, [tlsonly]),
    {_, Status} = try_connect(Config, Cid, #{create_user => false}),
    ?assertEqual(401, Status),
    _WS = connect(Config, Cid, #{tls => [], create_user => false}).

%% The offline status published while the node shuts down is persistent
%% and survives the restart.
f15_offline_status_survives_node_restart(Config) ->
    declare_worker_queue(Config, <<"workers">>, <<"ocpp16.StatusNotification.req">>),
    _WS = connect(Config, cid(Config)),
    ok = rabbit_ct_broker_helpers:restart_node(Config, 0),
    ok = rabbit_web_ocpp_test_util:init_ocpp_listener(Config, 0),
    ?assertEqual(1, message_count(Config, <<"workers">>)),
    ?assertMatch({_, _, [2, _, <<"StatusNotification">>,
                         #{<<"status">> := <<"Unavailable">>}]},
                 get_message(Config, <<"workers">>)).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

await_count(Config, QName, N) ->
    rabbit_ct_helpers:await_condition(
      fun() -> message_count(Config, QName) >= N end, ?WAIT).

atom_count(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, erlang, system_info, [atom_count]).

client_properties(Config) ->
    [Pid] = connection_pids(Config),
    [{client_properties, Props}] =
        rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_web_ocpp_handler, info,
                                     [Pid, [client_properties]]),
    Props.

%% Looks up a client property by binary or (legacy) atom key.
prop(Key, Props) ->
    case [V || {K, _T, V} <- Props, rabbit_data_coercion:to_binary(K) =:= Key] of
        [V] -> V;
        [] -> undefined
    end.

counters(Config) ->
    Overview = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_global_counters, overview, []),
    Protocol = maps:get(#{protocol => ocpp16}, Overview),
    Classic = maps:get(#{protocol => ocpp16, queue_type => rabbit_classic_queue}, Overview),
    #{consumers => maps:get(consumers, Protocol),
      messages_received_total => maps:get(messages_received_total, Protocol),
      messages_delivered_total => maps:get(messages_delivered_total, Classic)}.

queue_pid(Config, QName) ->
    {ok, Q} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup,
                                           [rabbit_misc:r(<<"/">>, queue, QName)]),
    amqqueue:get_pid(Q).

%% PROXY protocol v1 header of a client at 192.168.1.5:40000.
proxy_header(Config) ->
    Port = rabbit_web_ocpp_test_util:port(Config, 0),
    iolist_to_binary(["PROXY TCP4 192.168.1.5 127.0.0.1 40000 ", integer_to_list(Port), "\r\n"]).

hostname() ->
    {ok, Hostname} = inet:gethostname(),
    list_to_binary(Hostname).

client_cert_opts(Config) ->
    CertsDir = ?config(rmq_certsdir, Config),
    [{certfile, filename:join([CertsDir, "client", "cert.pem"])},
     {keyfile, filename:join([CertsDir, "client", "key.pem"])}].

consume_stream(Config, QName, N) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'basic.qos_ok'{} = amqp_channel:call(Ch, #'basic.qos'{prefetch_count = N}),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = QName,
                                                    arguments = [{<<"x-stream-offset">>, longstr, <<"first">>}]},
                               self()),
    Frames = [receive {#'basic.deliver'{}, #amqp_msg{payload = P}} -> json:decode(P)
              after 10000 -> ct:fail({stream_delivery_missing, I})
              end || I <- lists:seq(1, N)],
    amqp_connection:close(Conn),
    Frames.

%% Stands in for a connection with the same client ID on the other side of
%% a healed network partition. It records what it is sent.
spawn_older_peer() ->
    spawn(fun() -> older_peer([]) end).

older_peer(Msgs) ->
    receive
        {report, To} ->
            To ! {older_peer_got, lists:reverse(Msgs)},
            older_peer(Msgs);
        Msg ->
            older_peer([Msg | Msgs])
    end.
