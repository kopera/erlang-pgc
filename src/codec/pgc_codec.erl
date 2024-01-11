%% @private
-module(pgc_codec).
-feature(maybe_expr, enable).
-export([
    init/3,
    get_type/2,
    encode/3,
    encode_many/3,
    decode/3,
    decode_many/3
]).
-export_type([
    t/0
]).

-callback init(pgc_connection:parameters(), options()) -> CodecConfig :: term().
-callback info(CodecConfig :: term()) -> #{encodes := [atom()], decodes := [atom()]}.
-callback encode(pgc_type:t(), Value :: term(), CodecConfig :: term()) -> Data :: iodata().
-callback encode(pgc_type:t(), Value :: term(), CodecConfig :: term(), fun((pgc_type:oid(), term()) -> iodata())) -> Data :: iodata().
-callback decode(pgc_type:t(), Data :: binary(), CodecConfig :: term()) -> Value :: term().
-callback decode(pgc_type:t(), Data :: binary(), CodecConfig :: term(), fun((pgc_type:oid(), term()) -> term())) -> Value :: term().
-optional_callbacks([
    init/2,
    encode/3,
    encode/4,
    decode/3,
    decode/4
]).

-include("../pgc_type.hrl").
-define(default_codecs, [
    % pgc_codec_array,
    pgc_codec_int4,
    pgc_codec_text,
    pgc_codec_uuid,
    pgc_codec_bool,
    pgc_codec_bytea,
    pgc_codec_json,
    % pgc_codec_date,
    pgc_codec_timestamp,
    pgc_codec_enum,
    pgc_codec_float4,
    pgc_codec_float8,
    % pgc_codec_hstore,
    pgc_codec_int2,
    pgc_codec_int8,
    % pgc_codec_interval,
    % pgc_codec_network,
    pgc_codec_oid,
    % pgc_codec_record,
    % pgc_codec_time,
    pgc_codec_void
]).

-record(codec, {
    types :: #{pgc_type:oid() => pgc_type:t()},
    encoders :: #{pgc_type:oid() => {module(), term()}},
    decoders :: #{pgc_type:oid() => {module(), term()}}
}).
-opaque t() :: #codec{}.


-spec init([pgc_type:t()], pgc_connection:parameters(), options()) -> {ok, t()} | {error, Error} when
    Error :: pgc_error:t().
-type options() :: #{}.
init(Types, Parameters, Options) ->
    Codecs = [{CodecModule, init_codec(CodecModule, Parameters, Options)} || CodecModule <- ?default_codecs],
    maybe
        {ok, Encoders} ?= init_encoders(Codecs, Types),
        {ok, Decoders} ?= init_decoders(Codecs, Types),
        {ok, #codec{
            types = maps:from_list([{Type#pgc_type.oid, Type} || Type <- Types]),
            encoders = Encoders,
            decoders = Decoders
        }}
    end.


-spec get_type(pgc_type:oid(), t()) -> pgc_type:t().
get_type(TypeID, #codec{types = Types}) ->
    maps:get(TypeID, Types).


-spec encode(pgc_type:oid(), null, t()) -> {binary, null};
            (pgc_type:oid(), term(), t()) -> {binary, iodata()}.
encode(_TypeID, null, _Codec) ->
    {binary, null};
encode(TypeID, Value, #codec{types = Types, encoders = Encoders} = Codec) ->
    #{TypeID := Type} = Types,
    #{TypeID := {CodecModule, CodecState}} = Encoders,
    case erlang:function_exported(CodecModule, encode, 4) of
        true ->
            EncodeFun = fun(SubTypeID, SubValue) -> encode(SubTypeID, SubValue, Codec) end,
            {binary, CodecModule:encode(Type, Value, CodecState, EncodeFun)};
        false ->
            {binary, CodecModule:encode(Type, Value, CodecState)}
    end.


-spec encode_many([pgc_type:oid()], [null | term()], t()) -> [{binary, null | iodata()}].
encode_many(TypeIDs, Values, Codec) ->
    [encode(TypeID, Value, Codec) || {TypeID, Value} <- lists:zip(TypeIDs, Values)].


-spec decode(pgc_type:oid(), null, t()) -> null;
            (pgc_type:oid(), binary(), t()) -> term().
decode(_TypeID, null, _Codec) ->
    null;
decode(TypeID, Value, #codec{types = Types, decoders = Decoders} = Codec) ->
    #{TypeID := Type} = Types,
    #{TypeID := {CodecModule, CodecState}} = Decoders,
    case erlang:function_exported(CodecModule, decode, 4) of
        true ->
            DecodeFun = fun(SubTypeID, SubValue) -> decode(SubTypeID, SubValue, Codec) end,
            CodecModule:decode(Type, Value, CodecState, DecodeFun);
        false ->
            CodecModule:decode(Type, Value, CodecState)
    end.


-spec decode_many([pgc_type:oid()], [null | binary()], t()) -> [null | term()].
decode_many(TypeIDs, Values, Codec) ->
    [decode(TypeID, Value, Codec) || {TypeID, Value} <- lists:zip(TypeIDs, Values)].


%% @private
init_codec(CodecModule, Parameters, Options) ->
    case erlang:function_exported(CodecModule, init, 2) of
        true ->
            CodecModule:init(Parameters, Options);
        false ->
            Options
    end.


%% @private
init_encoders(Codecs, Types) ->
    init_encoders(Codecs, Types, #{}).

%% @private
init_encoders(_Codecs, [], Acc) ->
    {ok, Acc};
init_encoders(Codecs, [Type | Types], Acc) ->
    case find_encoder(Type, Codecs) of
        {ok, Codec} ->
            init_encoders(Codecs, Types, Acc#{Type#pgc_type.oid => Codec});
        error ->
            {error, pgc_error:feature_not_supported({"Missing encoder for type '~s'", Type#pgc_type.name})}
    end.


%% @private
init_decoders(Codecs, Types) ->
    init_decoders(Codecs, Types, #{}).

%% @private
init_decoders(_Codecs, [], Acc) ->
    {ok, Acc};
init_decoders(Codecs, [Type | Types], Acc) ->
    case find_decoder(Type, Codecs) of
        {ok, Codec} ->
            init_decoders(Codecs, Types, Acc#{Type#pgc_type.oid => Codec});
        error ->
            {error, pgc_error:feature_not_supported({"Missing decoder for type '~s'", Type#pgc_type.name})}
    end.


%% @private
find_encoder(_, []) ->
    error;
find_encoder(#pgc_type{send = SendProc} = Type, [{CodecModule, CodecState} = Codec | Codecs]) ->
    #{encodes := SendProcs} = CodecModule:info(CodecState),
    case lists:member(SendProc, SendProcs) of
        true -> {ok, Codec};
        false -> find_encoder(Type, Codecs)
    end.


%% @private
find_decoder(_, []) ->
    error;
find_decoder(#pgc_type{recv = RecvProc} = Type, [{CodecModule, CodecState} = Codec | Codecs]) ->
    #{decodes := RecvProcs} = CodecModule:info(CodecState),
    case lists:member(RecvProc, RecvProcs) of
        true -> {ok, Codec};
        false -> find_decoder(Type, Codecs)
    end.