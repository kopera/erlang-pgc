-module(pgc_string).
-moduledoc false.
-export([
    characters_to_binary/1,
    characters_to_list/1
]).


-spec characters_to_binary(unicode:chardata()) -> unicode:unicode_binary().
characters_to_binary(Input) ->
    case unicode:characters_to_binary(Input) of
        {error, _, _} -> erlang:error(badarg, [Input]);
        {incomplete, _, _} -> erlang:error(badarg, [Input]);
        UnicodeBinary -> UnicodeBinary
    end.


-spec characters_to_list(unicode:chardata()) -> string().
characters_to_list(Input) ->
    case unicode:characters_to_list(Input) of
        {error, _, _} -> erlang:error(badarg, [Input]);
        {incomplete, _, _} -> erlang:error(badarg, [Input]);
        Unicodestring -> Unicodestring
    end.
