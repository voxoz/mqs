-module(mqs_manager).
-author('Max Davidenko').
-behaviour(gen_server).
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-define(SERVER, ?MODULE).
-compile(export_all).

-record(state, { messages_handlers = null, handlers_pids = null}).

init([]) -> {ok, #state{ messages_handlers = dict:new(), handlers_pids = dict:new() }}.
start_link()           -> gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

handle_call({sub, Name, Args}, _From, State) ->
    StartResult = mqs_worker:start(Args),
    {Reply,NewState} = case StartResult of
        {ok, Pid} ->
            mqs:info(?MODULE,"Messages handler instance successfuly started: ~p", [Name]),
            { {ok, consumer_started},
              State#state { 
                  messages_handlers = dict:store(Name, {Pid, Args}, State#state.messages_handlers),
                  handlers_pids = dict:store(Pid, Name, State#state.handlers_pids) } };
        _ ->
            mqs:error(?MODULE,"Error while starting message handler instance: Name - ~p", [Name]),
            { {error, cant_start_consumer},
              State }
    end,
    {reply, Reply, NewState};

handle_call({pub, Name, Message}, _From, State) ->
    HandlerPid = dict:find(Name, State#state.messages_handlers),
    Reply = case HandlerPid of
        error ->
            mqs:error(?MODULE,"Unable to send message. Handler with specified name not found. Name - ~p", [Name]),
            {error, cant_send_message};
        {ok, {Pid, _}} ->
            gen_server:call(Pid, {pub, term_to_binary(Message)})
    end,
    {reply, Reply, State};

handle_call({unsub, Name}, _From, State) ->
    HandlerPid = dict:find(Name, State#state.messages_handlers),
    {Reply,NewState} = case HandlerPid of
         error ->
             mqs:error(?MODULE,"Unable to stop messages handler. Specified handler not found. Name - ~p", [Name]),
             { {error, consumer_not_found}, State };
         {ok, {Pid, _}} ->
             { gen_server:call(Pid, stop),
               State#state { messages_handlers = dict:erase(Name, State#state.messages_handlers),
                             handlers_pids = dict:erase(Pid, State#state.handlers_pids) } }
    end,
    {reply, Reply, NewState};

handle_call({suspend, Name}, _From, State) ->
    HandlerPid = dict:find(Name, State#state.messages_handlers),
    Reply = case HandlerPid of
        error ->
            mqs:error(?MODULE,"Unable to suspend messages handler. Specified handler not found. Name - ~p", [Name]),
            {error, consumer_not_found};
       {ok, {Pid, _}} ->
            Pid ! rejecting,
            {ok, consumer_suspended} end,
    {reply, Reply, State};

handle_call({resume, Name}, _From, State) ->
    HandlerPid = dict:find(Name, State#state.messages_handlers),
    Reply = case HandlerPid of
        error ->
            mqs:error(?MODULE,"Unable to resume messages handler. Specified handler not found. Name - ~p", [Name]),
            {error, consumer_not_found};
        {ok, {Pid, _}} ->
            Pid ! accepting,
            {ok, consumer_resumed} end,
    {reply, Reply, State};

handle_call(stop, _From, State) ->
    Workers = dict:fetch_keys(State#state.messages_handlers),
    terminateWorkers(Workers, State#state.messages_handlers),
    {stop, normal, "Normal shutdown of messages handlers monitor", State};

handle_call(_Request, _From, State) -> {reply, ok, State}.

handle_cast({terminated, Pid}, State) ->
  NewHandlersPids = dict:erase(Pid, State#state.handlers_pids),
  PidsLeft = dict:size(NewHandlersPids),
  if
    PidsLeft == 0 ->
      mqs:error(?MODULE,"All messages handlers abnormally terminated, trying to restart"),
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
      mqs:error(?MODULE,"Messages handler [~p] was abnormally terminated", [Pid]),
      NewState = State#state{handlers_pids = NewHandlersPids},
      Reply = {noreply, NewState}
  end,
  Reply;

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(Reason, State) ->
    if Reason /= normal ->
       WorkersNames = dict:fetch_keys(State#state.messages_handlers),
       terminateWorkers(WorkersNames, State#state.messages_handlers);
       true -> ok end.

terminateWorkers([], _WorkersPids) -> {ok, "Messages handlers successfuly terminated"};
terminateWorkers([Name | Names], WorkersPids) ->
    HandlerPid = dict:find(Name, WorkersPids),
    case HandlerPid of
         error -> terminateWorkers(Names, WorkersPids);
         {ok, {Pid, _}} ->
              gen_server:call(Pid, stop),
              WorkersLeft = dict:erase(Name, WorkersPids),
              terminateWorkers(Names, WorkersLeft) end,
    {ok, "Messages handlers successfuly terminated"}.

restartHandlers([], _HandlersInfo, NewHandlers, NewHandlersPids) -> {ok, NewHandlers, NewHandlersPids};
restartHandlers([Handler | Handlers], HandlersInfo, NewHandlers, NewHandlersPids) ->
  HandlerInfo = dict:find(Handler, HandlersInfo),
  case HandlerInfo of
    error ->
      mqs:error(?MODULE,"Unable to find handler [~p] specs for restarting", [Handler]),
      restartHandlers(Handlers, HandlersInfo, NewHandlers, NewHandlersPids);
    {ok, {_Pid, Args}} ->
      StartResult = mqs_worker:start(Args),
      case StartResult of
        {ok, NewPid} ->
          RestartedHandlers = dict:store(Handler, {NewPid, Args}, NewHandlers),
          RestartedPids = dict:store(NewPid, Handler, NewHandlersPids),
          mqs:info(?MODULE,"Messages handler instance successfuly restarted: ~p", [Handler]),
          NewHandlersInfo = dict:erase(Handler, HandlersInfo),
          restartHandlers(Handlers, NewHandlersInfo, RestartedHandlers, RestartedPids);
        _ ->
          mqs:error(?MODULE,"Error while restarting message handler instance: Name - ~p", [Handler]),
          {error, failed_to_restart_handlers} end
  end.


