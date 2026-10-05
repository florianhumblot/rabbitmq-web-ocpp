%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2025 VAMPIRE BYTE SRL. All Rights Reserved.
%%

%% Keeps the consumers gauge of the global counters: every established
%% connection consumes from its charge point queue. The gauge is decremented
%% when the connection process goes down, exactly once, and also when it is
%% killed (see rabbit_web_ocpp_processor:register_client_id/2) and its
%% terminate callback does not run.
-module(rabbit_web_ocpp_tracker).

-behaviour(gen_server).

-export([start_link/0, register_connection/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Called by the connection process once it consumes.
-spec register_connection(atom()) -> ok.
register_connection(ProtoVer) ->
    gen_server:call(?MODULE, {register, self(), ProtoVer}).

init([]) ->
    %% So that terminate/2 runs when the plugin stops.
    process_flag(trap_exit, true),
    {ok, #{}}.

handle_call({register, Pid, ProtoVer}, _From, Monitors) ->
    MRef = erlang:monitor(process, Pid),
    rabbit_global_counters:consumer_created(ProtoVer),
    {reply, ok, Monitors#{MRef => ProtoVer}}.

handle_cast(_Msg, Monitors) ->
    {noreply, Monitors}.

handle_info({'DOWN', MRef, process, _Pid, _Reason}, Monitors0) ->
    case maps:take(MRef, Monitors0) of
        {ProtoVer, Monitors} ->
            rabbit_global_counters:consumer_deleted(ProtoVer),
            {noreply, Monitors};
        error ->
            {noreply, Monitors0}
    end;
handle_info(_Info, Monitors) ->
    {noreply, Monitors}.

%% The plugin stops: its listeners stop next, which closes the connections.
terminate(_Reason, Monitors) ->
    maps:foreach(fun(_MRef, ProtoVer) ->
                         rabbit_global_counters:consumer_deleted(ProtoVer)
                 end, Monitors).
