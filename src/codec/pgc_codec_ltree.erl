%% @private
-module(pgc_codec_ltree).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [ltree_send, lquery_send],
        decodes => [ltree_recv, lquery_recv]
    },
    {Info, []}.


encode(Term, _Options) when is_binary(Term) ->
    <<1:8, Term/binary>>;
encode(Term, Options) ->
    error(badarg, [Term, Options]).


decode(<<1:8, Value/binary>>, _Options) ->
    Value.
