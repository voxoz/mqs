%%%-------------------------------------------------------------------
%%% @author Max Davidenko
%%% @copyright (C) 2013, PrivatBank
%%% @doc
%%%
%%% @end
%%% Created : 15. Nov 2013 4:02 PM
%%%-------------------------------------------------------------------

-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------
%% @doc
%% Defines RabbitMQ message handler specification
%% exchange = "" | <exchange_name> - name of exchange to work trough
%% exchange_type = one of available exchange types (direct, topic, fanout, headers) - atom()
%% routing_key = "" | <key> - name of queue or exchange binding key to work with
%% durable = true | false - message durability
%% enable_qos = true | false - enable message distributing among workers (for time consuming tasks)
%% broker_settings = #amqp_params_network record with messages broker server options
%% @end
%%--------------------------------------------------------------------
-record(rabbitmq_msg_handler_spec,
        {
          exchange_name = <<"">>,
          exchange_type = <<"direct">>,
          routing_key = <<"">>,
          durable = true,
          prefetch_count = 1,
          broker_settings = #amqp_params_network{},
          message_processing_mod = [],
          message_processing_fun = [],
          message_processing_args = null
        }
  ).
