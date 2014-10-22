-module(mqs_sup).
-author('Max Davidenko').
-behaviour(supervisor).
-export([start_link/0]).
-export([init/1]).
-define(CHILD(I, Type), {I, {I, start_link, []}, transient, 5000, Type, [I]}).

start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    HandlersMonitor = ?CHILD(mqs_manager, worker),
    RestartStrategy = {one_for_one, 5, 60},
    Childs = [HandlersMonitor],
    {ok, { RestartStrategy, Childs} }.
