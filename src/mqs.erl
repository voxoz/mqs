-module(mqs).
-author('Max Davidenko').
-behaviour(application).
-export([start/0, start/2, stop/0, stop/1]).
-export([startMessagesHandler/2, 
         sendMessageToHandler/2, 
         stopMessagesHandler/1, 
         suspendMessagesHandler/1,
         resumeMessagesHandler/1]).

start() -> start(normal, []).
start(_StartType, _StartArgs) -> mqs_sup:start_link().
stop() -> stop([]).

stop(_State) ->
    IsManagerRunning = whereis(mqs_manager),
    IsSupRunning = whereis(mqs_sup),
    if IsManagerRunning /= undefined -> mqs_manager:stop();
                                true -> ok end,
    if IsSupRunning /= undefined     -> exit(IsSupRunning, shutdown);
                                true -> ok end,
    ok.

startMessagesHandler(Name, Args)    -> mqs_manager:subscribe(Name, Args).
sendMessageToHandler(Name, Message) -> mqs_manager:publish(Name, Message).
stopMessagesHandler(Name)           -> mqs_manager:close(Name).
suspendMessagesHandler(Name)        -> mqs_manager:suspend(Name).
resumeMessagesHandler(Name)         -> mqs_manager:resume(Name).
