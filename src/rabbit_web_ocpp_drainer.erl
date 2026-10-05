%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2025 VAMPIRE BYTE SRL. All Rights Reserved.
%%

%% Closes the Web OCPP connections of this node when the broker shuts down,
%% while the vhosts are still running.
%%
%% Connections announce their charge point offline when they terminate (see
%% rabbit_web_ocpp_processor:terminate/3). On a broker shutdown (e.g. SIGTERM)
%% the rabbit application stops before this plugin, and its vhosts stop
%% their queues and message stores without waiting for connections to finish:
%% the offline status notifications would be lost. This process is a child of
%% rabbit_sup started after the vhosts, so it is terminated before them.
-module(rabbit_web_ocpp_drainer).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start/0, stop/0, start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(DRAIN_TIMEOUT_MS, 10_000).

-spec start() -> ok | {error, term()}.
start() ->
    _ = stop(),
    rabbit_sup:start_child(?MODULE).

-spec stop() -> ok | {error, term()}.
stop() ->
    rabbit_sup:stop_child(?MODULE).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    case rabbit_web_ocpp_app:list_connections() of
        [] ->
            ok;
        Pids ->
            ?LOG_INFO("Web OCPP closing ~b connection(s) before the broker stops",
                      [length(Pids)]),
            MRefs = [begin
                         MRef = erlang:monitor(process, Pid),
                         Pid ! {shutdown, <<"broker is shutting down">>},
                         MRef
                     end || Pid <- Pids],
            await_down(MRefs, erlang:monotonic_time(millisecond) + ?DRAIN_TIMEOUT_MS)
    end.

await_down([], _Deadline) ->
    ok;
await_down(MRefs, Deadline) ->
    Timeout = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {'DOWN', MRef, process, _, _} ->
            await_down(lists:delete(MRef, MRefs), Deadline)
    after Timeout ->
              ?LOG_WARNING("Web OCPP: ~b connection(s) did not close before the broker stopped",
                           [length(MRefs)])
    end.
