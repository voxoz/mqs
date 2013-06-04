-module(nsm_mq_rpc_test).

-include("nsm_mq.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

general_test_() ->
    {setup,
     fun() ->

             ok = application:load(nsm_mq),
             ok = application:start(nsm_mq)
     end,
     fun(_) ->
             application:stop(nsm_mq)
     end,

     [
      {"basic",
       fun() ->
               %% create servers
               RPCQueue = <<"test queue 1">>,
               [start_server(RPCQueue, N) || N <- lists:seq(1, 10)],

               %% make requests via nsm_mq_rpc_srv
               [?assertMatch({ok, {test, N}, _}, nsm_mq_rpc:call(RPCQueue, {test, N}))
                || N <- lists:seq(1, 10)]
       end
      },
      {"serve exclusive",
       {timeout, 15,
        fun() ->
                RPCQueue = <<"test111">>,
                S1 = start_exclusive_server(),
                S2 = start_exclusive_server(),
                ?debugFmt("S1: ~p, S2: ~p", [S1, S2]),


                %% make S1 serve on RPCQueue
                ?assertMatch(ok, call(S1, {serve, RPCQueue})),

                ?debugFmt("After serve", []),


                Ref = make_ref(),
                ?assertEqual({ok, Ref}, nsm_mq_rpc:call(RPCQueue, Ref)),

                %% stop S1 and start to serve on queue with S2 after delay
                ok = call(S1,  stop),
                timer:sleep(100),

                spawn_link(
                  fun() ->
                          receive
                              after 500 ->
                                  ?debugFmt("Start consume: ~p, Queue: ~p", [S2, RPCQueue]),
                                  ?assertEqual(ok, call(S2, {serve, RPCQueue}))
                          end
                  end),

                %% we will get reply when server S2 will start serve on RPCQueue
                Ref1 = make_ref(),
                ?assertEqual({ok, Ref1}, nsm_mq_rpc:call(RPCQueue, Ref1))

        end}
      }
     ]
    }.


start_server(Queue, N)  ->
    spawn_link(
      fun() ->
              Callback = fun(Request) ->
                                 ?debugFmt("Request: ~p, Worker: ~p", [Request, N]),
                                 {ok, Request, N}
                         end,
              ok = nsm_mq_rpc:join(Queue, Callback),
              loop()
      end).

start_exclusive_server() ->
    spawn_link(
      fun() ->
              Self = self(),
              EchoCallback =
                  fun(Request) ->
                          ?debugFmt("Request: ~p, Worker: ~p", [Request, Self]),
                          {ok, Request}
                  end,
              loop(EchoCallback)
      end).

call(Server, Req) ->
    Ref = make_ref(),
    Server!{self(), Ref, Req},
    receive
        {Ref, Reply} ->
            Reply
        after 1000 ->
            {error, call_timeout}
    end.


loop(Callback) ->
    receive
        {From, Ref, {serve, Queue}} ->
            ?debugFmt("~p: got serve request: ~p", [self(), Queue]),
            case nsm_mq_rpc:serve(Queue, Callback) of
                {ok, Queue} ->
                    From!{Ref, ok};
                E ->
                    From!{Ref, E}
            end,
            loop(Callback);
        {From, Ref, stop} ->
            ?debugFmt("stop: ~p", [self()]),
            From!{Ref, ok}
    end.

loop() ->
    timer:sleep(1000),
    loop().
