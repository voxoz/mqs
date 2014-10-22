%%%-------------------------------------------------------------------
%%% @author Max Davidenko
%%% @copyright (C) 2013, PrivatBank
%%% @doc
%%%
%%% @end
%%% Created : 15. Nov 2013 3:39 PM
%%%-------------------------------------------------------------------
-module(mqs_worker).
-behaviour(gen_server).

-compile([{parse_transform, lager_transform}]).

-include_lib("mqs/include/mqs.hrl").

%% API
-export([start/1, start_link/1]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).
-record(state,
{
  connection::pid(), % rabbitMQ connection
  channel:: pid(), % rabbitMQ channel
  args = null, % Start args (for restarting after broken channel or connection)
  processing_state = null
}).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server which not a part of supervision tree
%%
%% @spec start(Args) -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start(Args) ->
  gen_server:start(?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% Args - rabbitmq_msg_handler_spec instance
%% @end
%%--------------------------------------------------------------------
init(Args) ->
  process_flag(trap_exit, true),
  initMsgHandler(Args).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes connection the rabbit server by supplied specification
%%
%% @spec initMsgHandler(Args) -> {ok, State} | {stop, Reason}
%% @end
%%--------------------------------------------------------------------
initMsgHandler(Args) ->

% 1. Connect to the broker
  BrokerOpts = Args#rabbitmq_msg_handler_spec.broker_settings,
  {ok, BrokerConnection} = amqp_connection:start(BrokerOpts),
  link(BrokerConnection),

% 2. Create channel
  {ok, BrokerChannel} = amqp_connection:open_channel(BrokerConnection),
  amqp_channel:call(BrokerChannel, #'basic.qos'{prefetch_count = Args#rabbitmq_msg_handler_spec.prefetch_count}),
  State = #state{connection = BrokerConnection, channel = BrokerChannel, args = Args, processing_state = accepting},

% 3. Set up messages routing
  case Args#rabbitmq_msg_handler_spec.exchange_name of
    <<"">> ->
      Queue = Args#rabbitmq_msg_handler_spec.routing_key,
      if
        Queue == <<"">> ->
          RouteCreated = {stop, "No exchange and queue name specified"};
        true ->
          QueueSpec = #'queue.declare'{queue = Queue, durable = Args#rabbitmq_msg_handler_spec.durable},
          #'queue.declare_ok'{} = amqp_channel:call(BrokerChannel, QueueSpec),
          RouteCreated = ok
      end;
    _ ->
      if
        Args#rabbitmq_msg_handler_spec.routing_key == <<"">> ->
          Queue = <<"">>,
          RouteCreated = {stop, "No routing key for exchange specified"};
        true ->
          ExchangeSpec = #'exchange.declare'{exchange = Args#rabbitmq_msg_handler_spec.exchange_name, type = Args#rabbitmq_msg_handler_spec.exchange_type, durable = Args#rabbitmq_msg_handler_spec.durable},
          #'exchange.declare_ok'{} = amqp_channel:call(BrokerChannel, ExchangeSpec),
          #'queue.declare_ok'{queue = Queue} = amqp_channel:call(BrokerChannel, #'queue.declare'{}),
          Binding = #'queue.bind'{queue = Queue, exchange = Args#rabbitmq_msg_handler_spec.exchange_name, routing_key = Args#rabbitmq_msg_handler_spec.routing_key},
          #'queue.bind_ok'{} = amqp_channel:call(BrokerChannel, Binding),
          RouteCreated = ok
      end
  end,

% 4. Subscribe to queue
  if
    RouteCreated == ok ->
      QueueSubscription = #'basic.consume'{queue = Queue},
      #'basic.consume_ok'{consumer_tag = _Tag} = amqp_channel:call(BrokerChannel, QueueSubscription), %% caller process is a consumer

      Result = {ok, State};
    true ->
      Result = RouteCreated
  end,
  Result.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sending specified message to handler through broker
%%
%% @spec handle_call(stop, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({send_message, Message}, _From, State) ->
  Channel = State#state.channel,
  MsgPublish = #'basic.publish'{exchange = State#state.args#rabbitmq_msg_handler_spec.exchange_name,
    routing_key = State#state.args#rabbitmq_msg_handler_spec.routing_key},
  amqp_channel:cast(Channel, MsgPublish, #amqp_msg{payload = Message}),
  Reply = {ok, "Message successfuly sent"},
  {reply, Reply, State};

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle stop message from monitor
%%
%% @spec handle_call(stop, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(stop, _From, State) ->
  cleanUpBeforeDie(State),
  {stop, normal, "Normal shutdown of messages handler", State};

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_info(accepting, State) ->
  NewState = State#state{ processing_state = accepting },
  {noreply, NewState};

handle_info(rejecting, State) ->
  NewState = State#state{ processing_state = rejecting },
  {noreply, NewState};

handle_info(#'basic.consume_ok'{}, State) ->
  {noreply, State};

handle_info(#'basic.cancel_ok'{}, State) ->
  {noreply, State};

handle_info({#'basic.deliver'{delivery_tag = Tag}, Message}, State) ->
  case State#state.processing_state of
    accepting ->
      Payload = Message#amqp_msg.payload,
      HandlerSpec = State#state.args,
      InvokeResult = invoke(HandlerSpec#rabbitmq_msg_handler_spec.message_processing_mod, HandlerSpec#rabbitmq_msg_handler_spec.message_processing_fun,
        {Payload, HandlerSpec#rabbitmq_msg_handler_spec.message_processing_args}),
      case InvokeResult of
        {ok, _} ->
          amqp_channel:cast(State#state.channel, #'basic.ack'{delivery_tag = Tag});
        {error, Reason} ->
          lager:error("Error while processing received message. Reason: ~p", [Reason]),
          amqp_channel:cast(State#state.channel, #'basic.reject'{delivery_tag = Tag, requeue = true});
        _ ->
          lager:warning("Unknown message processing result"),
          amqp_channel:cast(State#state.channel, #'basic.reject'{delivery_tag = Tag, requeue = true})
      end;
    rejecting ->
      amqp_channel:cast(State#state.channel, #'basic.reject'{delivery_tag = Tag, requeue = true})
  end,
  {noreply, State};

handle_info({'EXIT', From, Reason}, State) ->
  unlink(From),
  if
    Reason /= normal ->
      mqs_manager:handlerTerminated(self());
    true ->
      ok
  end,
  {noreply, State};

handle_info(Info, State) ->
  lager:info("Unknown message received at amqp handler. Details: [~p]", [Info]),
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(Reason, State) ->
  lager:info("Amqp messages handler [~p] terminating, reason: [~p]", [self(), Reason]),
  if
    Reason /= normal ->
      cleanUpBeforeDie(State),
      ok;
    true ->
      ok
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

cleanUpBeforeDie(State) ->
  if
    is_pid(State#state.channel) -> amqp_channel:close(State#state.channel)
  end,
  if
    is_pid(State#state.connection) -> amqp_connection:close(State#state.connection)
  end,
  {ok, "Messages handler successfuly cleaned resources and ready to shutdown"}.

invoke([], []) ->
  nothing_to_invoke;
invoke([], Func) ->
  FunName = list_to_atom(Func),
  ?MODULE:FunName();
invoke(Module, Func) ->
  FunName = list_to_atom(Func),
  ModuleName = list_to_atom(Module),
  ModuleName:FunName().

invoke([], [], _Args) ->
  nothing_to_invoke;
invoke([], Func, []) ->
  invoke([], Func);
invoke(Module, Func, []) ->
  invoke(Module, Func);
invoke([], Func, Args) ->
  FunName = list_to_atom(Func),
  ?MODULE:FunName(Args);
invoke(Module, Func, Args) ->
  FunName = list_to_atom(Func),
  ModuleName = list_to_atom(Module),
  ModuleName:FunName(Args).