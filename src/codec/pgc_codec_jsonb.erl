-module(pgc_codec_jsonb).

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
        #{} -> undefined
    end,
    Info = #{
        encodes => [jsonb_send],
        decodes => [jsonb_recv]
    },
    {Info, Codec}.


encode(Term, undefined) ->
    _ = iolist_size(Term),
    [1 | Term];
encode(Term, {codec, CodecModule}) ->
    [1 | CodecModule:encode(Term)].


decode(<<1, Data/binary>>, undefined) ->
    <<1, Data/binary>>;
decode(<<1, Data/binary>>, {codec, CodecModule}) ->
    CodecModule:decode(Data).