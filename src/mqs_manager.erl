%%%-------------------------------------------------------------------
%%% @author Max Davidenko
%%% @copyright (C) 2013, PrivatBank
%%% @doc
%%%
%%% @end
%%% Created : 10. Dec 2013 9:39 AM
%%%-------------------------------------------------------------------
-module(mqs_manager).

-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

%% API
-export([start_link/0, startHandler/2, sendMessageToHandler/2, stopHandler/1, stopManager/0, suspendMessagesHandler/1,
    resumeMessagesHandler/1, handlerTerminated/1]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, { messages_handlers = null, handlers_pids = null}).

%%%===================================================================
%%% API
%%%===================================================================

startHandler(Name, Args) ->
  gen_server:call(?SERVER, {start_handler, Name, Args}, infinity).

sendMessageToHandler(Name, Message) ->
  gen_server:call(?SERVER, {send_message, Name, Message}, infinity).

stopHandler(Name) ->
  gen_server:call(?SERVER, {stop, Name}, infinity).

stopManager() ->
  gen_server:call(?SERVER, stop, infinity).

suspendMessagesHandler(Name) ->
  gen_server:call(?SERVER, {suspend, Name}, infinity).

resumeMessagesHandler(Name) ->
  gen_server:call(?SERVER, {resume, Name}, infinity).

handlerTerminated(Pid) ->
  gen_server:cast(?SERVER, {terminated, Pid}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

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
%% @end
%%--------------------------------------------------------------------
init([]) ->
  {ok, #state{ messages_handlers = dict:new(), handlers_pids = dict:new() }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling start of new messages worker
%%
%% @spec handle_call({start_handler, Args}, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({start_handler, Name, Args}, _From, State) ->
  StartResult = mqs_worker:start(Args),
  case StartResult of
    {ok, Pid} ->
      NewState = State#state{messages_handlers = dict:store(Name, {Pid, Args}, State#state.messages_handlers),
        handlers_pids = dict:store(Pid, Name, State#state.handlers_pids)},
      lager:info("Messages handler instance successfuly started: ~p", [Name]),
      Reply = {ok, "Messages handler instance successfuly started"};
    _ ->
      Reply = {error, "Error while starting messages handler instance with specified args"},
      lager:error("Error while starting message handler instance: Name - ~p", [Name]),
      NewState = State
  end,
  {reply, Reply, NewState};

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sending specified message to specified handler
%%
%% @spec handle_call({send_message, Name, Message}, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({send_message, Name, Message}, _From, State) ->
  HandlerPid = dict:find(Name, State#state.messages_handlers),
  case HandlerPid of
    error ->
      Reply = {error, "Unable to send message. Handler with specified name not found"},
      lager:error("Unable to send message. Handler with specified name not found. Name - ~p", [Name]);
    {ok, {Pid, _}} ->
      Reply = gen_server:call(Pid, {send_message, term_to_binary(Message)})
  end,
  {reply, Reply, State};

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling stop of specified messages handler
%%
%% @spec handle_call({stop, Name}, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({stop, Name}, _From, State) ->
  HandlerPid = dict:find(Name, State#state.messages_handlers),
  case HandlerPid of
    error ->
      Reply = {error, "Messages handler with specified name not found"},
      lager:error("Unable to stop messages handler. Specified handler not found. Name - ~p", [Name]),
      NewState = State;
    {ok, {Pid, _}} ->
      Reply = gen_server:call(Pid, stop),
      NewState = State#state{messages_handlers = dict:erase(Name, State#state.messages_handlers),
        handlers_pids = dict:erase(Pid, State#state.handlers_pids)}
  end,
  {reply, Reply, NewState};

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling suspending of specified messages handler
%%
%% @spec handle_call({suspend, Name}, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({suspend, Name}, _From, State) ->
  HandlerPid = dict:find(Name, State#state.messages_handlers),
  case HandlerPid of
    error ->
      Reply = {error, "Messages handler with specified name not found"},
      lager:error("Unable to suspend messages handler. Specified handler not found. Name - ~p", [Name]);
    {ok, {Pid, _}} ->
      Pid ! rejecting,
      Reply = {ok, "Messages handler successfuly suspended"}
  end,
  {reply, Reply, State};

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling resuming of specified messages handler
%%
%% @spec handle_call({resume, Name}, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({resume, Name}, _From, State) ->
  HandlerPid = dict:find(Name, State#state.messages_handlers),
  case HandlerPid of
    error ->
      Reply = {error, "Messages handler with specified name not found"},
      lager:error("Unable to resume messages handler. Specified handler not found. Name - ~p", [Name]);
    {ok, {Pid, _}} ->
      Pid ! accepting,
      Reply = {ok, "Messages handler successfuly resumed"}
  end,
  {reply, Reply, State};

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling stop of messages handlers monitors and performs its shutdown
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
  Workers = dict:fetch_keys(State#state.messages_handlers),
  terminateWorkers(Workers, State#state.messages_handlers),
  {stop, normal, "Normal shutdown of messages handlers monitor", State};

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
%% Handling terminate message from specified handler
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({terminated, Pid}, State) ->
  NewHandlersPids = dict:erase(Pid, State#state.handlers_pids),
  PidsLeft = dict:size(NewHandlersPids),
  if
    PidsLeft == 0 ->
      lager:error("All messages handlers abnormally terminated, trying to restart"),
      Handlers = dict:fetch_keys(State#state.messages_handlers),
      Restarted = restartHandlers(Handlers, State#state.messages_handlers, dict:new(), dict:new()),
      case Restarted of
        {ok, NewHandlers, NewPids} ->
          NewState = State#state{messages_handlers = NewHandlers, handlers_pids = NewPids},
          Reply = {noreply, NewState};
        _ ->
          Reply = {stop, Restarted, State}
      end;
    true ->
      lager:error("Messages handler [~p] was abnormally terminated", [Pid]),
      NewState = State#state{handlers_pids = NewHandlersPids},
      Reply = {noreply, NewState}
  end,
  Reply;

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
handle_info(_Info, State) ->
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
  if
    Reason /= normal ->
      WorkersNames = dict:fetch_keys(State#state.messages_handlers),
      terminateWorkers(WorkersNames, State#state.messages_handlers);
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

terminateWorkers([], _WorkersPids) ->
  {ok, "Messages handlers successfuly terminated"};
terminateWorkers([Name | Names], WorkersPids) ->
  HandlerPid = dict:find(Name, WorkersPids),
  case HandlerPid of
    error ->
      terminateWorkers(Names, WorkersPids);
    {ok, {Pid, _}} ->
      gen_server:call(Pid, stop),
      WorkersLeft = dict:erase(Name, WorkersPids),
      terminateWorkers(Names, WorkersLeft)
  end,
  {ok, "Messages handlers successfuly terminated"}.

restartHandlers([], _HandlersInfo, NewHandlers, NewHandlersPids) ->
  {ok, NewHandlers, NewHandlersPids};
restartHandlers([Handler | Handlers], HandlersInfo, NewHandlers, NewHandlersPids) ->
  HandlerInfo = dict:find(Handler, HandlersInfo),
  case HandlerInfo of
    error ->
      lager:error("Unable to find handler [~p] specs for restarting", [Handler]),
      restartHandlers(Handlers, HandlersInfo, NewHandlers, NewHandlersPids);
    {ok, {_Pid, Args}} ->
      StartResult = mqs_worker:start(Args),
      case StartResult of
        {ok, NewPid} ->
          RestartedHandlers = dict:store(Handler, {NewPid, Args}, NewHandlers),
          RestartedPids = dict:store(NewPid, Handler, NewHandlersPids),
          lager:info("Messages handler instance successfuly restarted: ~p", [Handler]),
          NewHandlersInfo = dict:erase(Handler, HandlersInfo),
          restartHandlers(Handlers, NewHandlersInfo, RestartedHandlers, RestartedPids);
        _ ->
          lager:error("Error while restarting message handler instance: Name - ~p", [Handler]),
          {error, failed_to_restart_handlers}
      end
  end.


