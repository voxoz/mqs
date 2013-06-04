-module(mqs_app).
-behaviour(application).
-export([start/0, start/2, stop/1]).

start() -> application:start(mqs).
start(_StartType, _StartArgs) -> mqs_sup:start_link().
stop(_State) -> ok.
