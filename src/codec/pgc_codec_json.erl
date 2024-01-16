%% @private
-module(pgc_codec_json).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(Options) ->
    Codec = case Options of
        #{json := #{codec := C}} when is_atom(C) -> {codec, C};
        #{json := #{codec := _}} -> erlang:error(badarg, [Options]);
        #{} -> binary
    end,
    Info = #{
        encodes => [json_send],
        decodes => [json_recv]
    },
    {Info, Codec}.


encode(Term, binary) ->
    _ = iolist_size(Term),
    Term;
encode(Term, {codec, CodecModule}) ->
    CodecModule:encode(Term).


decode(Data, binary) ->
    Data;
decode(Data, {codec, CodecModule}) ->
    CodecModule:decode(Data).

