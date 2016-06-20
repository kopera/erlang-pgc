-module(pgsql_codec_oid).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

-export([
    decode/1
]).

encodes(_Opts) ->
    [
        oidsend, xidsend, cidsend, regprocsend, regproceduresend,
        regopersend, regoperatorsend, regclasssend, regtypesend
    ].

encode(_Type, Value, _Codec, _Opts) when is_integer(Value), Value >= 0, Value =< 4294967295 ->
    <<Value:32/integer>>;
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [
        oidrecv, xidrecv, cidrecv, regprocrecv, regprocedurerecv,
        regoperrecv, regoperatorrecv, regclassrecv, regtyperecv
    ].

decode(_Type, <<Value:32/integer>>, _Codec, _Opts) ->
    Value.

%% @private
%% Used internally in pgsql_types
decode(<<Value:32/integer>>) ->
    Value.
