%% @private
-module(pgc_string).
-export([
    to_binary/1
]).

-type t() :: unicode:latin1_chardata() | unicode:chardata() | unicode:external_chardata().


-spec to_binary(t()) -> unicode:unicode_binary().
to_binary(Input) ->
    case unicode:characters_to_binary(Input) of
        {error, _, _} ->
            erlang:error(badarg, [Input]);
        {incomplete, _, _} ->
            erlang:error(badarg, [Input]);
        UnicodeBinary ->
            UnicodeBinary
    end.