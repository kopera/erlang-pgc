-module(pgc_codec_oid).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).

-export([
    decode/1
]).


info(_Options) ->
    #{
        encodes => [
            oidsend, xidsend, cidsend, regprocsend, regproceduresend,
            regopersend, regoperatorsend, regclasssend, regtypesend
        ],
        decodes => [
            oidrecv, xidrecv, cidrecv, regprocrecv, regprocedurerecv,
            regoperrecv, regoperatorrecv, regclassrecv, regtyperecv
        ]
    }.


encode(_Type, Value, _Options) when is_integer(Value), Value >= 0, Value =< 4294967295 ->
    <<Value:32/integer>>;
encode(Type, Value, Options) ->
    error(badarg, [Type, Value, Options]).


decode(_Type, <<Value:32/integer>>, _Options) ->
    Value.


%% @private
%% Used internally in pgc_types
decode(<<Value:32/integer>>) ->
    Value.