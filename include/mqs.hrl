-author('Max Davidenko').
-include_lib("amqp_client/include/amqp_client.hrl").

-record(mqs, {
        connection = duplex,
        name = <<"">>,
        type = <<"direct">>,
        key = <<"">>,
        durable = true,
        prefetch_count = 1,
        broker = #amqp_params_network{},
        'mod' = [],
        'fun' = [],
        'arg' = null,
        'pid' = null}).
