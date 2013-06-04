-module(mqs_channel_sup).
-author('Vladimir Baranov <baranoff.vladimir@gmail.com>').
-behaviour(supervisor).
-export([start_link/0]).
-export([start_channel/2]).
-export([init/1]).

start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).
start_channel(Conn, Options) -> supervisor:start_child(?MODULE, [Conn, Options]).

init([]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{mqs_channel, {mqs_channel, start_link, []},
            temporary, brutal_kill, worker, [mqs_channel]}]}}.

