-module(mqs_worker).
-author('Max Davidenko').
-behaviour(gen_server).
-include("mqs.hrl").

-export([start/1, start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state,{connection,channel,args=null,consumer_tag=null,consume_state=active}).

start_link(Args) -> gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).
start(Args) -> gen_server:start(?MODULE, Args, []).

init(Args) ->
    process_flag(trap_exit, true),

    % 1. Connect to the broker
    BrokerOpts = Args#mqs.broker,
    {ok, BrokerConnection} = amqp_connection:start(BrokerOpts),
    link(BrokerConnection),

    % 2. Create channel
    {ok, BrokerChannel} = amqp_connection:open_channel(BrokerConnection),
    amqp_channel:call(BrokerChannel, #'basic.qos'{prefetch_count = Args#mqs.prefetch_count}),
    State = #state{connection = BrokerConnection, channel = BrokerChannel, args = Args},

    % 3. Set up messages routing
    case {Args#mqs.name,Args#mqs.key} of
         {<<"">>,<<"">>} -> RouteCreated = {stop, "No exchange and queue name specified"};
         {<<"">>,_}      -> QueueSpec = #'queue.declare'{queue=Args#mqs.key,durable=Args#mqs.durable},
                            #'queue.declare_ok'{} = amqp_channel:call(BrokerChannel, QueueSpec),
                            RouteCreated = ok;
         {_,<<"">>}      -> RouteCreated = {stop, "No routing key for exchange specified"};
         {_,_}           -> ExchangeSpec = #'exchange.declare'{exchange=Args#mqs.name,type=Args#mqs.type,durable=Args#mqs.durable},
                            #'exchange.declare_ok'{} = amqp_channel:call(BrokerChannel, ExchangeSpec),
                            QueueSpec = #'queue.declare'{queue=Args#mqs.key,durable=Args#mqs.durable},
                            #'queue.declare_ok'{} = amqp_channel:call(BrokerChannel, QueueSpec),
                            Binding = #'queue.bind'{queue=Args#mqs.key,exchange=Args#mqs.name,routing_key=Args#mqs.key},
                            #'queue.bind_ok'{} = amqp_channel:call(BrokerChannel, Binding),
                            RouteCreated = ok end,

    % 4. Subscribe to queue (if needed)
    HandlerType = Args#mqs.connection,
    case RouteCreated of
         ok -> case HandlerType of
               duplex ->
                    QueueSubscription = #'basic.consume'{queue = Args#mqs.key},
                    #'basic.consume_ok'{consumer_tag = Tag} =
                    amqp_channel:call(BrokerChannel, QueueSubscription),
                    amqp_selective_consumer:register_default_consumer(BrokerChannel, self()),
                    NewState = State#state{consumer_tag = Tag},
                    {ok, NewState};
               _ -> {ok, State} end;
          _ -> RouteCreated end.

handle_call({pub, Message}, _From, State) ->
    Channel = State#state.channel,
    MsgPublish = #'basic.publish'{exchange=State#state.args#mqs.name,routing_key=State#state.args#mqs.key},
    amqp_channel:cast(Channel, MsgPublish, #amqp_msg{payload = Message}),
    Reply = {ok, "Message successfuly sent"},
    {reply, Reply, State};

handle_call(stop, _From, State) ->
    cleanUpBeforeDie(State),
    {stop, normal, "Normal shutdown of messages handler", State};

handle_call(_Request, _From, State) -> {reply, ok, State}.
handle_cast(_Msg, State) -> {noreply, State}.
handle_info(#'basic.consume_ok'{}, State) -> {noreply, State};
handle_info(#'basic.cancel_ok'{}, State) ->  {noreply, State};

handle_info(accepting, State) ->
    NewState = case State#state.consume_state of
                    suspended -> mqs:info(?MODULE,"Consumer resumed ~p~n",[self()]),
                                 #'basic.consume_ok'{consumer_tag=Tag} =
                                 amqp_channel:call(State#state.channel,
                                      #'basic.consume'{consumer_tag=State#state.consumer_tag}),
                                 State#state{ consume_state=active,consumer_tag=Tag};
                            _ -> State end,
    {noreply, NewState};

handle_info(rejecting, State) ->
    NewState = case State#state.consume_state of
                    active -> mqs:info(?MODULE,"Suspend request received ~p~n",[self()]),
                              State#state{ consume_state = suspended };
                         _ -> State end,
    {noreply, NewState};

handle_info({#'basic.deliver'{delivery_tag = Tag}, Message}, State) ->
    Payload = Message#amqp_msg.payload,
    Spec = State#state.args,
    Mod = Spec#mqs.'mod',
    Fun = Spec#mqs.'fun',
    InvokeResult = try Mod:Fun({Payload,Spec#mqs.arg})
                 catch  _:_ -> {error, msg_callback_failed} end,
    case InvokeResult of
         {ok, _} -> amqp_channel:cast(State#state.channel,#'basic.ack'{delivery_tag=Tag});
         {error, Reason} ->
                mqs:error(?MODULE,"Error while processing received message. Reason: ~p", [Reason]),
                amqp_channel:cast(State#state.channel,#'basic.reject'{delivery_tag=Tag,requeue=true});
         Res -> mqs:warning(?MODULE,"Unknown message processing result ~p~n",[Res]),
                amqp_channel:cast(State#state.channel,#'basic.reject'{delivery_tag=Tag,requeue=true}) end,
    case State#state.consume_state of
         suspended ->
                #'basic.cancel_ok'{} = amqp_channel:call(State#state.channel,#'basic.cancel'{consumer_tag=State#state.consumer_tag,nowait=false}),
                mqs:info(?MODULE,"Suspend request performed",[]);
         _ -> ok end,
    {noreply, State};

handle_info({'EXIT', From, Reason}, State) ->
    unlink(From),
    if Reason /= normal -> mqs:terminated(self());
       true -> ok end,
    {noreply, State};

handle_info(Info, State) ->
    mqs:info(?MODULE,"Unknown message received at amqp handler. Details: [~p]", [Info]),
    {noreply, State}.

terminate(Reason, State) ->
    mqs:info(?MODULE,"Amqp messages handler [~p] terminating, reason: [~p]", [self(), Reason]),
    if Reason /= normal -> cleanUpBeforeDie(State), ok;
       true -> ok end.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

cleanUpBeforeDie(State) ->
    if is_pid(State#state.channel) -> amqp_channel:close(State#state.channel); true -> ok end,
    if is_pid(State#state.connection) -> amqp_connection:close(State#state.connection); true -> ok end,
    {ok, "Messages handler successfuly cleaned resources and ready to shutdown"}.