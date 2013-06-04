-module(nsm_mq_db_test).

-include("nsm_mq.hrl").
-include_lib("eunit/include/eunit.hrl").


-compile(export_all).

-define(APP, nsm_mq).

-record(put_test_state, {publisher, worker}).
-record(test_data, {id, payload}).

start_basic_test_() ->
    {setup,
     fun start_application/0,
     fun(_) ->
             application:stop(nsm_mq)
     end,
     [{"start worker, start publisher",[
       ?_assertMatch({ok, _}, nsm_mq_lib_db:start_worker()),
       ?_assertMatch({ok, _}, nsm_mq_lib_db:start_publisher())]
      }]
    }.

put_test_() ->
    {setup,
     fun() ->
             start_application(),
             {ok, Publisher} = nsm_mq_lib_db:start_publisher(),
             {ok, Worker} = nsm_mq_lib_db:start_worker(),
             #put_test_state{publisher = Publisher,
                             worker = Worker}
     end,
     fun(#put_test_state{publisher = Publisher,
                         worker = Worker}) ->
             timer:sleep(1000),
             ok = nsm_mq_lib_db:stop_worker(Worker),
             ok = nsm_mq_lib_db:stop_publisher(Publisher),
             application:stop(nsm_mq)
     end,
     fun(#put_test_state{publisher = Pub}) ->
             [{"all type put",
               fun()->
                       PutData = #test_data{id = 1, payload = "PUT: test data"},
                       SeqData = #test_data{id = 2, payload = "SEQ: test data"},
                       DirtyData = #test_data{id = 3, payload = "DIRTY: test sata"},

                       nsm_mq_lib_db:write(Pub, PutData),
                       nsm_mq_lib_db:seq_write(Pub, SeqData),
                       nsm_mq_lib_db:dirty_write(Pub, DirtyData),

                       %% wait until data arrive to writer and will be stored
                       timer:sleep(300),

                       %% this function is only for testing purposes and
                       %% not exported when compiled not for tests.
                       ?assertEqual({ok, PutData}, nsm_mq_lib_db:get_data(PutData#test_data.id)),
                       ?assertEqual({ok, SeqData}, nsm_mq_lib_db:get_data(SeqData#test_data.id)),
                       ?assertEqual({ok, DirtyData}, nsm_mq_lib_db:get_data(DirtyData#test_data.id))
               end}]
     end
    }.


%%
%% Local functions
%%

start_application() ->
    %% change message queue parameters to prevent damage of the real data
    application:set_env(?APP, amqp_vhost, "test"),
    application:load(?APP),
    application:start(?APP).


