%% @private
-module(pgc_codec_name).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [namesend],
        decodes => [namerecv]
    },
    {Info, []}.


encode(Term, Options) ->
    case erlang:iolist_size(Term) of
        Length when Length < 64 -> Term;
        Length when Length >= 64 -> erlang:error(badarg, [Term, Options])
    end.


decode(Data, _Options) ->
    Data.