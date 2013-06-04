-module(mqs_lib).
-author('Vladimir Baranov <baranoff.vladimir@gmail.com>').
-include_lib("mqs/include/mqs.hrl").
-export([encode/1, decode/1, list_to_key/1, key_to_list/1]).
-export([opt/3, override_opt/3, get_env/2]).

encode(Term) -> term_to_binary(Term).
decode(Binary) when is_binary(Binary) -> binary_to_term(Binary).

opt(Option, List, Default) ->
    case lists:member(Option, List) of
        true -> true;
        false -> proplists:get_value(Option, List, Default) end.

override_opt(Option, Value, Options) ->
    CleanOptions = lists:filter(
                     fun({O, _}) when O == Option -> false;
                        (O)      when O == Option -> false;
                        (_)                       -> true
                     end, Options),

    [{Option, Value} | CleanOptions].

get_env(Par, DefaultValue) ->
    case application:get_env(Par) of
        undefined -> DefaultValue;
        {ok, Value} -> Value end.

key_to_list(BKey) when is_binary(BKey) -> LKey = binary_to_list(BKey), string:tokens(LKey, ".");
key_to_list(Other) -> throw({unexpected_key_format, Other}).

list_to_key(Things) when is_list(Things) -> WithDots = divide(Things), list_to_binary(lists:concat(WithDots));
list_to_key(Other) -> throw({unexpected_key_format, Other}).

divide([])-> [];
divide([T|[]])-> [T];
divide([H|T]) -> [H, "."|divide(T)].
