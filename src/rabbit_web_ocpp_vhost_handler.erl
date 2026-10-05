%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2025 VAMPIRE BYTE SRL. All Rights Reserved.
%%

%% Declares the exchange of the plugin in vhosts created while it runs, so
%% that workers can bind to it before the first charge point connects (unlike
%% amq.topic, it does not exist by default).
-module(rabbit_web_ocpp_vhost_handler).

-behaviour(gen_event).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([add/0, remove/0]).
-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-spec add() -> ok.
add() ->
    _ = remove(),
    gen_event:add_handler(rabbit_event, ?MODULE, []).

-spec remove() -> term().
remove() ->
    gen_event:delete_handler(rabbit_event, ?MODULE, []).

init([]) ->
    {ok, []}.

handle_event(#event{type = vhost_created, props = Props}, State) ->
    Vhost = proplists:get_value(name, Props),
    %% Not from within the event manager: declaring an exchange emits events.
    _ = spawn(fun() -> rabbit_web_ocpp_util:ensure_exchanges([Vhost]) end),
    {ok, State};
handle_event(_Event, State) ->
    {ok, State}.

handle_call(_Request, State) ->
    {ok, ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
