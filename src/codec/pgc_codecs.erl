%% @private
-module(pgc_codecs).
-export([
    new/1,
    new/2,
    encode/4,
    decode/4
]).
-export_type([
    t/0
]).

-include("../types/pgc_type.hrl").


-record(codecs, {
    encoders :: #{atom() => {module(), State :: term()}},
    decoders :: #{atom() => {module(), State :: term()}}
}).
-opaque t() :: #codecs{}.


-spec new(options()) -> t().
new(Options) ->
    new([], Options).


-spec new([module() | {module(), map()}], options()) -> t().
-type options() :: #{}.
new(ExtraCodecs, Options) ->
    CodecModules = ExtraCodecs ++ default_codecs(Options),
    Codecs = lists:foldl(fun ({CodecModule, CodecState, CodecInfo}, CodecsAcc) ->
        #{encodes := Encodes} = CodecInfo,
        lists:foldl(fun
            (SendProc, Acc) when not is_map_key(SendProc, Acc) ->
                Acc#{SendProc => {CodecModule, CodecState}};
            (_SendProc, Acc)->
                Acc
        end, CodecsAcc, Encodes)
    end, #{}, [init_codec(CodecModule, Options) || CodecModule <- CodecModules]),
    #codecs{
        encoders = Codecs,
        decoders = Codecs
    }.


encode(_Type, null, _GetTypeFun, _Codecs) ->
    {binary, null};
encode(#pgc_type{send = SendProc} = Type, Term, GetTypeFun, #codecs{} = Codecs) ->
    case Codecs#codecs.encoders of
        #{SendProc := {CodecModule, CodecState}} ->
            case erlang:function_exported(CodecModule, encode, 4) of
                true ->
                    EncodeFun = fun(SubTypeID, SubTerm) ->
                        {binary, Encoded} = encode(GetTypeFun(SubTypeID), SubTerm, GetTypeFun, Codecs),
                        Encoded
                    end,
                    {binary, CodecModule:encode(Term, CodecState, Type, EncodeFun)};
                false ->
                    {binary, CodecModule:encode(Term, CodecState)}
            end;
        #{} ->
            erlang:error({encoder_missing, Type})
    end.


decode(_Type, null, _GetTypeFun, _Codecs) ->
    null;
decode(#pgc_type{send = SendProc} = Type, Data, GetTypeFun, #codecs{} = Codecs) ->
    case Codecs#codecs.decoders of
        #{SendProc := {CodecModule, CodecState}} ->
            case erlang:function_exported(CodecModule, decode, 4) of
                true ->
                    DecodeFun = fun(SubTypeID, SubTerm) ->
                        decode(GetTypeFun(SubTypeID), SubTerm, GetTypeFun, Codecs)
                    end,
                    CodecModule:decode(Data, CodecState, Type, DecodeFun);
                false ->
                    CodecModule:decode(Data, CodecState)
            end;
        #{} ->
            erlang:error({decoder_missing, Type})
    end.


%% @private
init_codec({CodecModule, CodecOptions}, GlobalOptions) when is_atom(CodecModule) ->
    Options = maps:merge(GlobalOptions, CodecOptions),
    {CodecInfo, CodecState} = CodecModule:init(Options),
    {CodecModule, CodecState, CodecInfo};
init_codec(CodecModule, GlobalOptions) ->
    init_codec({CodecModule, #{}}, GlobalOptions).


%% @private
default_codecs(_Options) ->
    [
        pgc_codec_array,
        pgc_codec_bitstring,
        pgc_codec_bool,
        pgc_codec_bytea,
        pgc_codec_char,
        pgc_codec_date,
        pgc_codec_enum,
        pgc_codec_float4,
        pgc_codec_float8,
        pgc_codec_hstore,
        % pgc_codec_inet,
        pgc_codec_int2,
        pgc_codec_int4,
        pgc_codec_int8,
        % pgc_codec_interval,
        pgc_codec_json,
        pgc_codec_jsonb,
        % pgc_codec_mac_address,
        % pgc_codec_multirange,
        pgc_codec_name,
        % pgc_codec_numeric,
        pgc_codec_oid,
        % pgc_codec_range,
        pgc_codec_record,
        pgc_codec_text,
        pgc_codec_tid,
        pgc_codec_time,
        pgc_codec_timetz,
        pgc_codec_timestamp,
        % pgc_codec_tsvector,
        pgc_codec_uuid,
        pgc_codec_void,
        pgc_codec_xid8
    ].