-module(pgc_client_codec_oid).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [
        ~"oidsend", ~"oidrecv",
        ~"xidsend", ~"xidrecv",
        ~"cidsend", ~"cidrecv",
        ~"regprocsend", ~"regprocrecv",
        ~"regproceduresend", ~"regprocedurerecv",
        ~"regopersend", ~"regoperrecv",
        ~"regoperatorsend", ~"regoperatorrecv",
        ~"regclasssend", ~"regclassrecv",
        ~"regtypesend", ~"regtyperecv"
    ].

encode(Value, _TypeDescriptor, _Codecs) when is_integer(Value), Value >= 0, Value =< 4294967295 ->
    <<Value:32/integer>>;
encode(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

decode(<<Value:32/integer>>, _TypeDescriptor, _Codecs) ->
    Value.
