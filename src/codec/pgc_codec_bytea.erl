%% @private
-module(pgc_codec_bytea).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [byteasend, unknownsend],
        decodes => [bytearecv, unknownrecv]
    },
    {Info, []}.


encode(Term, _Options) ->
    _ = iolist_size(Term),
    Term.


decode(Data, _Options) when is_binary(Data) ->
    Data.