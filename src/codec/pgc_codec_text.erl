%% @private
-module(pgc_codec_text).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [bpcharsend, textsend, varcharsend, citextsend],
        decodes => [bpcharrecv, textrecv, varcharrecv, citextrecv]
    },
    {Info, []}.


encode(Term, _Options) ->
    _ = iolist_size(Term),
    Term.


decode(Data, _Options) ->
    Data.