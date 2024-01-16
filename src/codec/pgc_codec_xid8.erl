%% @private
-module(pgc_codec_xid8).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [xid8send],
        decodes => [xid8recv]
    },
    {Info, []}.


encode(Term, _Options) when is_integer(Term), Term >= 0, Term =< 18_446_744_073_709_551_615 ->
    <<Term:64/integer>>;
encode(Term, Options) ->
    error(badarg, [Term, Options]).


decode(<<XID:64/integer>>, _Options) ->
    XID.