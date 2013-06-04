-define(NOTIFICATIONS_EX, <<"ns.notifications.topic">>).
-define(USER_EVENTS_EX, <<"ns.user_events.topic">>).
-define(DB_EX, <<"ns.db.topic">>).
-define(DB_WORKERS_QUEUE, <<"ns.db.collective.write">>).
-define(DB_WORKERS_COLLECTIVE_KEY, <<"collective.write">>).
-define(DB_WORKERS_EXCLUSIVE_PREFIX, "exclusive.").

-record(envelope, {
    payload,
    exchange,
    routing_key,
    consumer_tag,
    props}).

-record(msg_props, {
    reply_to,
    correlation_id}).

-type channel_pid() :: pid().
-type consumer_callback() :: function() | {Mod::atom(), Fun::atom()}.
-type consumer_tag() :: binary().
-type envelope() :: #envelope{}.
-type handler_response() :: ok | reject.
-type routing_key() :: binary().
