-module(mqs).

-behaviour(application).

-include_lib("mqs/include/mqs.hrl").

%% Application callbacks
-export([start/0, start/2, stop/0, stop/1]).

%% API
-export([startMessagesHandler/2, sendMessageToHandler/2, stopMessagesHandler/1, suspendMessagesHandler/1,
  resumeMessagesHandler/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start() ->
  start(normal, []).

start(_StartType, _StartArgs) ->
  lager:start(),
  mqs_sup:start_link().

stop() ->
  stop([]).

stop(_State) ->
  IsManagerRunning = whereis(mqs_manager),
  if
    IsManagerRunning /= undefined ->
      mqs_manager:stopManager();
    true ->
      ok
  end,
  IsSupRunning = whereis(mqs_sup),
  if
    IsSupRunning /= undefined ->
      exit(IsSupRunning, shutdown);
    true ->
      ok
  end,
  ok.

%% ===================================================================
%% API
%% ===================================================================

startMessagesHandler(Name, Args) ->
  mqs_manager:startHandler(Name, Args).

sendMessageToHandler(Name, Message) ->
  mqs_manager:sendMessageToHandler(Name, Message).

stopMessagesHandler(Name) ->
  mqs_manager:stopHandler(Name).

suspendMessagesHandler(Name) ->
  mqs_manager:suspendMessagesHandler(Name).

resumeMessagesHandler(Name) ->
  mqs_manager:resumeMessagesHandler(Name).