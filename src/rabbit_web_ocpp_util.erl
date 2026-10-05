%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2025 VAMPIRE BYTE SRL. All Rights Reserved.
%%
-module(rabbit_web_ocpp_util).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_web_ocpp.hrl").

-export([get_env/1,
         exchange/0,
         ensure_exchange/2,
         ensure_exchanges/0,
         ensure_exchanges/1,
         validate_client_id/1,
         allowed_protocols/0]).

%% Defaults of settings that are read at runtime. Also declared in the
%% application environment (see PROJECT_ENV in the Makefile).
-spec get_env(atom()) -> term().
get_env(Key) ->
    application:get_env(?APP_NAME, Key, default(Key)).

default(exchange) -> <<"ocpp">>;
default(prefetch_count) -> 10;
default(max_held_calls) -> 100;
default(call_timeout) -> 30_000;
default(queue_type) -> classic;
default(queue_message_ttl) -> 300_000;
default(queue_expires) -> 604_800_000;
default(permission_cache_ttl) -> 60_000;
default(username_must_match_client_id) -> true;
default(max_client_id_length) -> 48;
default(protocols) -> [<<"ocpp1.6">>, <<"ocpp2.0">>, <<"ocpp2.0.1">>, <<"ocpp2.1">>];
default(ws_ping_interval) -> undefined;
default(_) -> undefined.

-spec exchange() -> binary().
exchange() ->
    rabbit_data_coercion:to_binary(get_env(exchange)).

%% The exchange is declared (as a durable topic exchange) when it does not
%% exist yet: in all vhosts when the plugin starts, so that workers can bind
%% to it, and in the vhost of a connecting charge point.
-spec ensure_exchange(rabbit_exchange:name(), rabbit_types:username()) -> ok | {error, term()}.
ensure_exchange(XName, Username) ->
    case rabbit_exchange:lookup(XName) of
        {ok, _} ->
            ok;
        {error, not_found} ->
            try rabbit_exchange:declare(XName, topic, true, false, false, [], Username) of
                {ok, _} -> ok;
                {error, Reason} -> {error, {exchange_declare_failed, Reason}}
            catch exit:Reason ->
                      {error, {exchange_declare_failed, Reason}}
            end
    end.

-spec ensure_exchanges() -> ok.
ensure_exchanges() ->
    ensure_exchanges(rabbit_vhost:list_names()).

-spec ensure_exchanges([rabbit_types:vhost()]) -> ok.
ensure_exchanges(Vhosts) ->
    lists:foreach(
      fun(Vhost) ->
              XName = rabbit_misc:r(Vhost, exchange, exchange()),
              case ensure_exchange(XName, ?INTERNAL_USER) of
                  ok ->
                      ok;
                  {error, Reason} ->
                      ?LOG_WARNING("Web OCPP could not declare ~ts: ~p",
                                   [rabbit_misc:rs(XName), Reason])
              end
      end, Vhosts).

%% The client ID is the binding key of the charge point queue on a topic
%% exchange and part of the routing key of everything the CSMS sends it. It
%% must therefore not contain topic separators ('.') or wildcards ('*', '#'):
%% a charge point called '#' would receive the traffic of all charge points.
%%
%% OCPP 2.0.1 [Part 4, 3.1.1] restricts the identity to at most 48 characters
%% of the unreserved set of RFC 3986. This accepts that set without the '.',
%% plus the RFC 3986 sub-delims and ':' and '@' (allowed in a path segment)
%% without the '*', which OCPP 1.6 charge points are known to use.
-spec validate_client_id(binary()) -> ok | {error, binary()}.
validate_client_id(ClientId) ->
    MaxLen = get_env(max_client_id_length),
    case byte_size(ClientId) of
        Len when Len > MaxLen ->
            {error, <<"client ID is too long">>};
        _ ->
            case re:run(ClientId, "^[A-Za-z0-9_~!$&'()+,;=:@-]+$", [{capture, none}]) of
                match -> ok;
                nomatch -> {error, <<"client ID contains invalid characters">>}
            end
    end.

%% OCPP versions with a JSON (OCPP-J) flavour. OCPP 1.2 and 1.5 are SOAP only.
-spec allowed_protocols() -> [binary()].
allowed_protocols() ->
    [rabbit_data_coercion:to_binary(P) || P <- get_env(protocols)].
