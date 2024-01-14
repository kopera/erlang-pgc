-module(pgc_codec_oid).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [
            oidsend, xidsend, cidsend, regprocsend, regproceduresend,
            regopersend, regoperatorsend, regclasssend, regtypesend
        ],
        decodes => [
            oidrecv, xidrecv, cidrecv, regprocrecv, regprocedurerecv,
            regoperrecv, regoperatorrecv, regclassrecv, regtyperecv
        ]
    },
    {Info, []}.


encode(Value, _Options) when is_integer(Value), Value >= 0, Value =< 4294967295 ->
    <<Value:32/integer>>;
encode(Value, Options) ->
    error(badarg, [Value, Options]).


decode(<<Value:32/integer>>, _Options) ->
    Value.