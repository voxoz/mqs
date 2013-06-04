-module(mqs).
-author('Vladimir Baranov').
-author('Maxim Sokhatsky').
-copyright('Synrc Research Center s.r.o.').
-behaviour(gen_server).
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("mqs/include/mqs.hrl").
-export([start_link/0]).
-export([open/1, node_name/0, publish/3, publish/4, notify/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-record(state, {connection,
                channel, %% this channel should be used only for publishing to static exchanges
                connection_options,
                node}).

-define(STATIC_EXCHANGES, [?USER_EVENTS_EX, ?DB_EX, ?NOTIFICATIONS_EX]).

open(Options) when is_list(Options) -> gen_server:call(?MODULE, {open, Options, self()}).
publish(Exchange, RoutingKey, Payload) -> publish(Exchange, RoutingKey, Payload, []).
publish(Exchange, RoutingKey, Payload, Options) -> gen_server:call(?MODULE, {publish, Exchange, RoutingKey, Payload, Options}).
node_name() -> gen_server:call(?MODULE, node_name).
start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

make_connection(State = #state{}) ->
    case amqp_connection:start(State#state.connection_options) of
        {ok, Connection} ->
            {ok, Ch} = mqs_channel_sup:start_channel(Connection, [{consumer, self()}]),
            init_static_entries(Ch, State#state.node),
            erlang:monitor(process, Connection),
            erlang:monitor(process, Ch),
            #state{connection = Connection, channel = Ch};
        {error, Reason} -> State end.

init([]) ->
    process_flag(trap_exit, true),
    Host     = mqs_lib:get_env(amqp_host, "localhost"),
    Port     = mqs_lib:get_env(amqp_port, 5672),
    UserStr  = mqs_lib:get_env(amqp_user, "guest"),
    PassStr  = mqs_lib:get_env(amqp_pass, "guest"),
    VHostStr = mqs_lib:get_env(amqp_vhost, "/"),
    User  = list_to_binary(UserStr),
    Pass  = list_to_binary(PassStr),
    VHost = list_to_binary(VHostStr),
    NodeName = mqs_lib:get_env(node, atom_to_list(node())),
    NodeName == node_undefined andalso throw({stop, node_undefined}),
    ConnectionOptions =   #amqp_params_network{host = Host,
                                               port = Port,
                                               username = User,
                                               password = Pass,
                                               virtual_host = VHost},

    {ok,#state{node = NodeName, connection_options = ConnectionOptions}}.

handle_call(node_name, _From,  #state{node = Node} = State) -> {reply, {ok, Node}, State};
handle_call({open, Options, DefaultConsumer}, _From, #state{connection = Connection} = State) ->
    case make_connection(State) of
        #state{connection=undefined} -> {reply, {error, cant_establish_mq_connection}, State};
        S -> Consumer = mqs_lib:opt(consumer, Options, DefaultConsumer),
             Options1 = mqs_lib:override_opt(consumer, Consumer, Options),
             Reply = mqs_channel_sup:start_channel(Connection,  Options1),
             {reply, Reply, S} end;
handle_call({publish, Exchange, RoutingKey, Payload, Options}, _From, #state{channel = Channel} = State) ->
    case make_connection(State) of
        #state{connection=undefined} -> {stop, cant_establish_mq_connection, State}; 
        S -> {reply, mqs_channel:publish(Channel, Exchange, RoutingKey, Payload, Options), S} end.

handle_cast(_Info, State) -> {noreply, State}.

handle_info(connect, State) ->
    case make_connection(State) of
         #state{connection=undefined} -> {stop, cant_establish_mq_connection, State};
         S -> {noreply, S} end;
handle_info({'DOWN', _MonitorRef, process, Connection, Reason}, #state{connection = Connection} = State) ->
    error_logger:error_msg("connection closed: ~p. Reason: ~p", [Connection, Reason]),
    {stop, connection_to_broker_failed, State};
handle_info({'DOWN', _MRef, process, Channel, Reason}, #state{connection = Connection, channel = Channel} = State) ->
    error_logger:warning_msg("channel ~p closed with reason: ~p", [Channel, Reason]),
    {ok, NewChannel} = mqs_channel_sup:start_channel(Connection, [{consumer, self()}]),
    {noreply, State#state{channel = NewChannel}};
handle_info(_Unexpected, State) -> {noreply, State}.

terminate(_Reason, #state{connection = Connection}) -> catch amqp_connection:close(Connection), ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

init_static_entries(Ch, Node) ->
    ExOptions = [durable, {auto_delete, false}],
    [ mqs_channel:create_exchange(Ch, Ex, ExOptions) || Ex <- ?STATIC_EXCHANGES].

routing_key(List) -> mqs_lib:list_to_key(List).

notify(EventPath, Data) -> RoutingKey = routing_key(EventPath), mqs:publish(?NOTIFICATIONS_EX, RoutingKey, Data).
