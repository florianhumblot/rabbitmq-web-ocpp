PROJECT = rabbitmq_web_ocpp
PROJECT_DESCRIPTION = RabbitMQ OCPP-J-to-AMQP adapter
PROJECT_MOD = rabbit_web_ocpp_app

define PROJECT_ENV
[
	    {tcp_config, [{port, 19520}]},
	    {ssl_config, []},
	    {num_tcp_acceptors, 10},
	    {num_ssl_acceptors, 10},
	    {cowboy_opts, []},
	    {proxy_protocol, false},
	    {allow_anonymous, false},
	    {exchange, <<"ocpp">>},
	    {prefetch_count, 10},
	    {call_timeout, 30000},
	    {queue_type, classic},
	    {queue_message_ttl, 300000},
	    {queue_expires, 604800000},
	    {permission_cache_ttl, 60000},
	    {username_must_match_client_id, true},
	    {max_client_id_length, 48},
	    {protocols, [<<"ocpp1.6">>, <<"ocpp2.0">>, <<"ocpp2.0.1">>, <<"ocpp2.1">>]}
	  ]
endef

LOCAL_DEPS = ssl
DEPS = rabbit cowboy
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client amqp10_client

PLT_APPS += rabbitmqctl elixir cowlib

# FIXME: Add Ranch as a BUILD_DEPS to be sure the correct version is picked.
# See rabbitmq-components.mk.
BUILD_DEPS += ranch

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk

CT_HOOKS = rabbit_ct_hook
