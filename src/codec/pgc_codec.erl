-module(pgc_codec).
-export([
    format_error/2
]).

-callback init(pgc_connection:parameters(), options()) -> {Info, CodecConfig :: term()} when
    Info :: #{encodes := [atom()], decodes := [atom()]}.
-callback encode(Value :: term(), CodecConfig :: term()) -> Data :: iodata().
-callback encode(Value :: term(), CodecConfig :: term(), Type :: pgc_type:t(), fun((pgc_type:oid(), term()) -> iodata())) -> Data :: iodata().
-callback decode(Data :: binary(), CodecConfig :: term()) -> Value :: term().
-callback decode(Data :: binary(), CodecConfig :: term(), Type :: pgc_type:t(), fun((pgc_type:oid(), term()) -> term())) -> Value :: term().
-optional_callbacks([
    init/2,
    encode/2,
    encode/4,
    decode/2,
    decode/4
]).

-type options() :: #{}.


%% @private
format_error(_Reason, [{_M, _F, _As, Info}|_]) ->
    ErrorInfo = proplists:get_value(error_info, Info, #{}),
    maps:get(cause, ErrorInfo).