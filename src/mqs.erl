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

log_modules() -> [mqs_worker,mqs,mqs_manager].
-define(ALLOWED, (application:get_env(mqs,log_modules,mqs))).

log(Module, String, Args, Fun) ->
    case lists:member(Module,?ALLOWED:log_modules()) of
         true -> error_logger:Fun("~p:"++String, [Module|Args]);
         false -> skip end.

info(Module,String, Args) ->  log(Module,String, Args, info_msg).
info(String, Args) -> log(?MODULE, String, Args, info_msg).
info(String) -> log(?MODULE, String, [], info_msg).

warning(Module,String, Args) -> log(Module, String, Args, warning_msg).
warning(String, Args) -> log(?MODULE, String, Args, warning_msg).
warning(String) -> log(?MODULE,String, [], warning_msg).

error(Module,String, Args) -> log(Module, String, Args, error_msg).
error(String, Args) -> log(?MODULE, String, Args, error_msg).
error(String) -> log(?MODULE, String, [], error_msg).
