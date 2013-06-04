-module(nsm_mq_test).

-include("nsm_mq.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MSG, "message").
-define(RECEIVE_TIMEOUT, 1000).
-define(ACK_TIMEOUT, 5000).

temp_stuff_test_() ->
    {setup,
     fun() ->

             ok = application:load(nsm_mq),
             ok = application:start(nsm_mq)
     end,
     fun(_) ->
             application:stop(nsm_mq)
     end,

     [{"create channel",
       ?_assertMatch({ok, _Pid}, nsm_mq:open([]))
       },

      {"default channel publish",
       ?assertMatch(ok, nsm_mq:publish(?USER_EVENTS_EX, <<"test">>, test))
       },

      {"basic operations",
       fun() ->
               {ok, Ch} = nsm_mq:open([]),
               E1 = <<"eunit.test1">>,
               E2 = <<"eunit.test2">>,
               RK = <<"routing.key.1">>,
               Q = <<"queue.name">>,

               ?assertEqual(ok, nsm_mq_channel:create_exchange(Ch, E1, [])),
               ?assertEqual(ok, nsm_mq_channel:create_exchange(Ch, E2, [])),

               %% try to create queue with automatic name
               ?assertNot({ok, <<"">>} == nsm_mq_channel:create_queue(Ch, <<"">>, [])),

               ?assertMatch({ok, Q}, nsm_mq_channel:create_queue(Ch, Q, [])),

               ?assertEqual(ok, nsm_mq_channel:bind_queue(Ch, Q, E1, RK)),
               ?assertEqual(ok, nsm_mq_channel:bind_exchange(Ch, E2, E1, RK)),

               ?assertEqual(ok, nsm_mq_channel:unbind_queue(Ch, Q, E1, RK)),
               ?assertEqual(ok, nsm_mq_channel:unbind_exchange(Ch, E2, E1, RK)),

               ?assertEqual(ok, nsm_mq_channel:create_exchange(Ch, E1, [])),
               ?assertEqual(ok, nsm_mq_channel:create_exchange(Ch, E2, [])),

               ?assertEqual(ok, nsm_mq_channel:delete_exchange(Ch, E1)),
               ?assertEqual(ok, nsm_mq_channel:delete_exchange(Ch, E2)),

               ?assertEqual(ok, nsm_mq_channel:delete_queue(Ch, Q))
       end
      },

      {"basic subscribe",
       fun() ->
               E1 = <<"eunit.test1">>,
               E2 = <<"eunit.test2">>,
               RK = <<"routing.key.1">>,
               Q = <<"queue.name">>,

               {ok, Ch} = nsm_mq:open([]),

               ok = nsm_mq_channel:create_exchange(Ch, E1, []),
               ok = nsm_mq_channel:create_exchange(Ch, E2, []),

               {ok, Q} = nsm_mq_channel:create_queue(Ch, Q, []),
               {ok, _CTag} = nsm_mq_channel:consume(Ch, Q, []),
               ok = nsm_mq_channel:bind_queue(Ch, Q, E1, RK),

               nsm_mq_channel:publish(Ch, E1, RK, hello_there),

               receive
                   {message,  #envelope{payload = hello_there}} ->
                       ok
                   after 1000 ->
                       throw(receive_timeout)
               end
       end
      },
      {"exchange bindings, fanout",
       {timeout, 5, fun() ->
               E1 = <<"eunit.test3.1">>,
               E2 = <<"eunit.test3.2">>,
               Q1 = <<"queue.test3.1">>,
               Q2 = <<"queue.test3.2">>,
               Q3 = <<"queue.test3.3">>,

               {ok, Ch} = nsm_mq:open([]),

               ok = nsm_mq_channel:create_exchange(Ch, E1, [{type, <<"fanout">>}]),
               ok = nsm_mq_channel:create_exchange(Ch, E2, []),

               %% create queues and start consuming
               {ok, Q1} = nsm_mq_channel:create_queue(Ch, Q1, []),
               {ok, CTag1} = nsm_mq_channel:consume(Ch, Q1, []),
               {ok, Q2} = nsm_mq_channel:create_queue(Ch, Q2, []),
               {ok, CTag2} = nsm_mq_channel:consume(Ch, Q2, []),
               {ok, Q3} = nsm_mq_channel:create_queue(Ch, Q3, []),
               {ok, CTag3} = nsm_mq_channel:consume(Ch, Q3, []),

               ok = nsm_mq_channel:bind_queue(Ch, Q1, E1, <<"">>),
               ok = nsm_mq_channel:bind_queue(Ch, Q2, E1, <<"">>),
               ok = nsm_mq_channel:bind_queue(Ch, Q3, E1, <<"">>),

               ok = nsm_mq_channel:bind_exchange(Ch, E1, E2, <<"x.*.*">>),

               nsm_mq_channel:publish(Ch, E2, <<"x.y.z">>, hello_there),

               ?assertEqual([CTag1, CTag2, CTag3],
                            [receive
                   {message, #envelope{payload = hello_there, consumer_tag = CT}} ->
                       CT
                   after 2000 ->
                       throw(receive_timeout)
               end || CT <- [CTag1, CTag2, CTag3]])

       end}
      },

       {"add custom callback",
        fun()->
                {ok, Ch} = nsm_mq:open([]),
                E1 = <<"eunit.test4.1">>,
                Q1 = <<"eunit.test4.1">>,

                ok = nsm_mq_channel:create_exchange(Ch, <<"eunit.test4.1">>, []),

                {Ref, Callback} = make_callback(),

                {ok, Q1} = nsm_mq_channel:create_queue(Ch, Q1, []),
                {ok, _Ctag} = nsm_mq_channel:consume(Ch, Q1, [{callback, Callback},
                                                             {state, init_state}]),
                ok = nsm_mq_channel:bind_queue(Ch, Q1, E1, <<"">>),

                ok = nsm_mq_channel:publish(Ch, E1, <<"">>, hello),

                ?assertMatch(#envelope{payload = hello}, receive_from_callback(Ref))
        end
       },

      {"publish with confirmation",
       fun() ->
               {ok, Ch} = nsm_mq:open([confirm_mode]),

               E1 = <<"eunit.test5">>,
               Q = <<"eunit.queue.test5">>,
               RK = <<"test5">>,

               ok = nsm_mq_channel:create_exchange(Ch, E1, [{auto_delete, false}]),
               {ok, Q} = nsm_mq_channel:create_queue(Ch, Q, [{auto_delete, false}]),
               ok = nsm_mq_channel:bind_queue(Ch, Q, E1, RK),

               ?assertEqual(ok, nsm_mq_channel:publish(Ch, E1, RK, "Hi")),
               ?assertMatch({error, _}, nsm_mq_channel:publish(Ch, E1, <<"dummy.route">>, "Hi, dummy!", [immediate])),
               ?assertEqual(ok, nsm_mq_channel:publish(Ch, E1, RK, "Hi")),

               nsm_mq_channel:delete_exchange(Ch, E1),
               nsm_mq_channel:delete_queue(Ch, Q),
               nsm_mq_channel:close(Ch)
       end
      }
     ]
    }.


make_callback() ->
    Parent = self(),
    Ref = make_ref(),
    Callback = fun(#envelope{} = E, init_state) ->
                       Parent!{Ref, E},
                       ok;
                  (_Type, _Any) ->
                       ok
               end,
    {Ref, Callback}.

receive_from_callback(Ref) ->
    receive
        {Ref, #envelope{} = Envelope}->
            Envelope
    after 1000 ->
            throw(receive_timeout)
    end.




