-module(mqs).
-author('Max Davidenko').
-behaviour(application).
-export([start/0, start/2, stop/0, stop/1]).
-compile(export_all).
-define(SERVER, mqs_manager).

start() -> start(normal, []).
start(_StartType, _StartArgs) -> mqs_sup:start_link().
stop() -> stop([]).

stop(_State) ->
    IsManagerRunning = whereis(mqs_manager),
    IsSupRunning = whereis(mqs_sup),
    if IsManagerRunning /= undefined -> shutdown();
                                true -> ok end,
    if IsSupRunning /= undefined     -> exit(IsSupRunning, shutdown);
                                true -> ok end,
    ok.

subscribe(Name, Args)  -> gen_server:call(?SERVER, {sub, Name, Args}, infinity).
publish(Name, Message) -> gen_server:call(?SERVER, {pub, Name, Message}, infinity).
close(Name)            -> gen_server:call(?SERVER, {unsub, Name}, infinity).
suspend(Name)          -> gen_server:call(?SERVER, {suspend, Name}, infinity).
resume(Name)           -> gen_server:call(?SERVER, {resume, Name}, infinity).

shutdown()             -> gen_server:call(?SERVER, stop, infinity).
terminated(Pid)        -> gen_server:cast(?SERVER, {terminated, Pid}).
