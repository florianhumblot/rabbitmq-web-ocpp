%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2025 VAMPIRE BYTE SRL. All Rights Reserved.
%%

-module(ocpp_cluster_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(rabbit_web_ocpp_test_util,
        [connect/3, try_connect/3, recv/1, assert_closed/2, publish_to_cp/3, set_env/3]).

all() ->
    [f9_quorum_cp_queue_survives_node_loss].

suite() ->
    [{timetrap, {minutes, 5}}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodename_suffix, ?MODULE},
                                                    {rmq_nodes_count, 3},
                                                    {rmq_nodes_clustered, true}]),
    Config2 = rabbit_ct_helpers:run_setup_steps(
                rabbit_web_ocpp_test_util:merge_app_env(Config1),
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    rabbit_web_ocpp_test_util:init_ocpp_listeners(Config2).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% A classic charge point queue lives on the node the charge point first
%% connected to. While that node is down, the charge point cannot connect
%% anywhere else. A quorum queue stays available.
f9_quorum_cp_queue_survives_node_loss(Config) ->
    set_env(Config, queue_type, quorum),
    Cid = <<"cp-ha">>,
    WS1 = connect(Config, Cid, #{node => 1}),
    ok = rabbit_ct_broker_helpers:stop_node(Config, 1),
    _ = assert_closed(WS1, 10000),
    %% The charge point reconnects to another node.
    WS2 = reconnect(Config, Cid, #{node => 2}, 30),
    publish_to_cp(Config, Cid, [2, <<"csms-1">>, <<"Reset">>, #{<<"type">> => <<"Soft">>}]),
    ?assertMatch([2, <<"csms-1">>, <<"Reset">>, _], recv(WS2)),
    ok = rabbit_ct_broker_helpers:start_node(Config, 1).

reconnect(_Config, Cid, Opts, 0) ->
    ct:fail({could_not_reconnect, Cid, Opts});
reconnect(Config, Cid, Opts, Attempts) ->
    case try_connect(Config, Cid, Opts) of
        {WS, 101} ->
            %% A connection whose consumer could not be set up is closed
            %% right after the upgrade.
            case rfc6455_client:recv(WS, 1000) of
                {error, timeout} -> WS;
                _ -> timer:sleep(1000), reconnect(Config, Cid, Opts, Attempts - 1)
            end;
        _ ->
            timer:sleep(1000),
            reconnect(Config, Cid, Opts, Attempts - 1)
    end.
