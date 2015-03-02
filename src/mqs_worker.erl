-module(mqs_worker).
-author('Max Davidenko').
-behaviour(gen_server).
-compile([{parse_transform, lager_transform}]).
-include_lib("mqs/include/mqs.hrl").

-export([start/1, start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-record(state, {
    connection::pid(), % rabbitMQ connection
    channel:: pid(),   % rabbitMQ channel
    args = null,       % Start args (for restarting after broken channel or connection)
    consumer_tag = null,
    consume_state = active }).

start_link(Args) -> gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).
start(Args) -> gen_server:start(?MODULE, Args, []).

init(Args) -> process_flag(trap_exit, true), initMsgHandler(Args).

initMsgHandler(Args) ->

    % 1. Connect to the broker
    BrokerOpts = Args#rabbitmq_msg_handler_spec.broker_settings,
    {ok, BrokerConnection} = amqp_connection:start(BrokerOpts),
    link(BrokerConnection),

    % 2. Create channel
    {ok, BrokerChannel} = amqp_connection:open_channel(BrokerConnection),
    amqp_channel:call(BrokerChannel, #'basic.qos'{prefetch_count = Args#rabbitmq_msg_handler_spec.prefetch_count}),
    State = #state{connection = BrokerConnection, channel = BrokerChannel, args = Args},

    % 3. Set up messages routing
    case Args#rabbitmq_msg_handler_spec.exchange_name of
        <<"">> -> 

            Queue = Args#rabbitmq_msg_handler_spec.routing_key,

                if Queue == <<"">> -> 
                        RouteCreated = {stop, "No exchange and queue name specified"};
                    true ->
                        QueueSpec = #'queue.declare'{
                            queue = Queue, 
                            durable = Args#rabbitmq_msg_handler_spec.durable },

                        #'queue.declare_ok'{} = amqp_channel:call(BrokerChannel, QueueSpec),

                        RouteCreated = ok end;

         _ ->
                if Args#rabbitmq_msg_handler_spec.routing_key == <<"">> ->
                        Queue = <<"">>,
                        RouteCreated = {stop, "No routing key for exchange specified"};
                    true ->
                        ExchangeSpec = #'exchange.declare' {
                            exchange = Args#rabbitmq_msg_handler_spec.exchange_name,
                            type = Args#rabbitmq_msg_handler_spec.exchange_type,
                            durable = Args#rabbitmq_msg_handler_spec.durable },

                        #'exchange.declare_ok'{} = amqp_channel:call(BrokerChannel, ExchangeSpec),
                        #'queue.declare_ok'{queue = Queue} = amqp_channel:call(BrokerChannel, #'queue.declare'{}),

                        Binding = #'queue.bind'{
                            queue = Queue, 
                            exchange = Args#rabbitmq_msg_handler_spec.exchange_name,
                            routing_key = Args#rabbitmq_msg_handler_spec.routing_key },

                        #'queue.bind_ok'{} = amqp_channel:call(BrokerChannel, Binding),

                        RouteCreated = ok end
  end,

    % 4. Subscribe to queue (if needed)
    HandlerType = Args#rabbitmq_msg_handler_spec.handler_type,
    if RouteCreated == ok ->
      case HandlerType of
        duplex ->
          QueueSubscription = #'basic.consume'{queue = Queue},
          #'basic.consume_ok'{consumer_tag = Tag} =
            amqp_channel:call(BrokerChannel, QueueSubscription), %% caller process is a consumer
          NewState = State#state{consumer_tag = Tag},
          {ok, NewState};
        _ ->
          % Do not register as consumer, handler just for sending messages
          {ok, State}
      end;
    true -> RouteCreated end.

handle_call({pub, Message}, _From, State) ->
    Channel = State#state.channel,
    MsgPublish = #'basic.publish'{
        exchange = State#state.args#rabbitmq_msg_handler_spec.exchange_name,
        routing_key = State#state.args#rabbitmq_msg_handler_spec.routing_key},
    amqp_channel:cast(Channel, MsgPublish, #amqp_msg{payload = Message}),
    Reply = {ok, "Message successfuly sent"},
    {reply, Reply, State};

handle_call(stop, _From, State) ->
    cleanUpBeforeDie(State),
    {stop, normal, "Normal shutdown of messages handler", State};

handle_call(_Request, _From, State) -> {reply, ok, State}.
handle_cast(_Msg, State) -> {noreply, State}.
handle_info(accepting, State) ->
  NewState = case State#state.consume_state of
    suspended ->
      lager:info("Consumer resumed"),
      #'basic.consume_ok'{consumer_tag = Tag} = amqp_channel:call(State#state.channel, #'basic.consume'{consumer_tag = State#state.consumer_tag }),
      State#state{ consume_state = active, consumer_tag = Tag };
    _ ->
      State
  end,
  {noreply, NewState};
handle_info(rejecting, State) ->
  NewState = case State#state.consume_state of
    active ->
      lager:info("Suspend request received"),
      State#state{ consume_state = suspended };
    _ ->
      State
  end,
  {noreply, NewState};
handle_info(#'basic.consume_ok'{}, State) -> {noreply, State};
handle_info(#'basic.cancel_ok'{}, State) ->  {noreply, State};
handle_info({#'basic.deliver'{delivery_tag = Tag}, Message}, State) ->
  Payload = Message#amqp_msg.payload,
  HandlerSpec = State#state.args,
  InvokeResult = try
    invoke(
      HandlerSpec#rabbitmq_msg_handler_spec.message_processing_mod,
      HandlerSpec#rabbitmq_msg_handler_spec.message_processing_fun,
      {Payload, HandlerSpec#rabbitmq_msg_handler_spec.message_processing_args})
                 catch
                   _:_ -> {error, msg_callback_failed}
                 end,
  case InvokeResult of
    {ok, _} -> amqp_channel:cast(State#state.channel, #'basic.ack'{delivery_tag = Tag});
    {error, Reason} ->
      lager:error("Error while processing received message. Reason: ~p", [Reason]),
      amqp_channel:cast(State#state.channel, #'basic.reject'{delivery_tag = Tag, requeue = true});
    _ ->
      lager:warning("Unknown message processing result"),
      amqp_channel:cast(State#state.channel, #'basic.reject'{delivery_tag = Tag, requeue = true})
  end,
  case State#state.consume_state of
    suspended ->
      #'basic.cancel_ok'{} = amqp_channel:call(State#state.channel, #'basic.cancel'{consumer_tag = State#state.consumer_tag, nowait = false }),
      lager:info("Suspend request performed");
    _ ->
      ok
  end,
  {noreply, State};

handle_info({'EXIT', From, Reason}, State) ->
    unlink(From),
    if Reason /= normal -> mqs:terminated(self());
       true -> ok end,
    {noreply, State};

handle_info(Info, State) ->
    lager:info("Unknown message received at amqp handler. Details: [~p]", [Info]),
    {noreply, State}.

terminate(Reason, State) ->
    lager:info("Amqp messages handler [~p] terminating, reason: [~p]", [self(), Reason]),
    if Reason /= normal -> cleanUpBeforeDie(State), ok;
       true -> ok end.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

cleanUpBeforeDie(State) ->
    if is_pid(State#state.channel) -> amqp_channel:close(State#state.channel); true -> ok end,
    if is_pid(State#state.connection) -> amqp_connection:close(State#state.connection); true -> ok end,
    {ok, "Messages handler successfuly cleaned resources and ready to shutdown"}.

invoke([], [])             -> nothing_to_invoke;
invoke([], Func)           -> FunName = list_to_atom(Func), ?MODULE:FunName();
invoke(Module, Func)       -> FunName = list_to_atom(Func),
                              ModuleName = list_to_atom(Module),
                              ModuleName:FunName().

invoke([], [], _Args)      -> nothing_to_invoke;
invoke([], Func, [])       -> invoke([], Func);
invoke(Module, Func, [])   -> invoke(Module, Func);
invoke([], Func, Args)     -> FunName = list_to_atom(Func), ?MODULE:FunName(Args);
invoke(Module, Func, Args) -> FunName = list_to_atom(Func),
                              ModuleName = list_to_atom(Module),
                              ModuleName:FunName(Args).
