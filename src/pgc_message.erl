%% @private
-module(pgc_message).
-export([
    encode/1,
    decode/2
]).
-export_type([
    t/0,
    message_f/0,
    message_b/0,
    message_fb/0,
    error_fields/0,
    notice_fields/0
]).

-include("./pgc_message.hrl").
-define(PROTOCOL_VERSION_MAJOR, 3).
-define(PROTOCOL_VERSION_MINOR, 0).

-type t() :: message().
-type message() :: message_f() | message_b() | message_fb().
-type message_f() ::
      #msg_bind{}
    | #msg_cancel_request{}
    | #msg_close{}
    | #msg_copy_fail{}
    | #msg_describe{}
    | #msg_execute{}
    | #msg_flush{}
    % | #msg_function_call{}
    | #msg_parse{}
    | #msg_password{}
    | #msg_sasl_initial_response{}
    | #msg_sasl_response{}
    | #msg_query{}
    | #msg_startup{}
    | #msg_sync{}
    | #msg_terminate{}.
-type message_b() ::
      #msg_auth{}
    | #msg_backend_key_data{}
    | #msg_bind_complete{}
    | #msg_close_complete{}
    | #msg_command_complete{}
    % | #msg_copy_in_response{}
    % | #msg_copy_out_response{}
    % | #msg_copy_both_response{}
    | #msg_data_row{}
    | #msg_empty_query_response{}
    | #msg_error_response{}
    % | #msg_function_call_response{}
    | #msg_no_data{}
    | #msg_notice_response{}
    | #msg_notification_response{}
    | #msg_parameter_description{}
    | #msg_parameter_status{}
    | #msg_parse_complete{}
    | #msg_portal_suspended{}
    | #msg_ready_for_query{}
    | #msg_row_description{}.
-type message_fb() ::
      #msg_copy_data{}
    | #msg_copy_done{}.

-type error_fields() :: #{
    severity := panic | fatal | error,
    code := binary(),
    message := unicode:unicode_binary(),
    detail => unicode:unicode_binary(),
    hint => unicode:unicode_binary(),
    position => pos_integer(),
    internal_position => pos_integer(),
    internal_query => unicode:unicode_binary(),
    where => unicode:unicode_binary(),
    schema => unicode:unicode_binary(),
    table => unicode:unicode_binary(),
    column => unicode:unicode_binary(),
    data_type => unicode:unicode_binary(),
    constraint => unicode:unicode_binary(),
    file => unicode:unicode_binary(),
    line => non_neg_integer(),
    routine => unicode:unicode_binary()
}.

-type notice_fields() :: #{
    severity := warning | notice | debug | info | log,
    code := binary(),
    message := unicode:unicode_binary(),
    detail => unicode:unicode_binary(),
    hint => unicode:unicode_binary(),
    schema => unicode:unicode_binary(),
    table => unicode:unicode_binary(),
    column => unicode:unicode_binary(),
    data_type => unicode:unicode_binary(),
    constraint => unicode:unicode_binary()
}.

-spec encode(message_f() | message_fb()) -> {Type :: byte() | <<>>, Payload :: iodata()}.
encode(#msg_bind{portal = Portal, statement = Statement, parameters = Parameters, results = Results}) ->
    ParametersCount = <<(length(Parameters)):16/integer>>,
    ParametersFormats = << <<(encode_format(Format)):16/integer>> || {Format, _} <- Parameters >>,
    ParametersValues = [case Value of
        null -> <<-1:32/signed-integer>>;
        _ -> [<<(iolist_size(Value)):32/signed-integer>>, Value]
    end || {_, Value} <- Parameters],
    ResultsCount = <<(length(Results)):16/integer>>,
    ResultsFormats = << <<(encode_format(Format)):16/integer>> || Format <- Results >>,
    {$B, [
        Portal, 0,
        Statement, 0,
        ParametersCount,
        ParametersFormats,
        ParametersCount,
        ParametersValues,
        ResultsCount,
        ResultsFormats
    ]};
encode(#msg_cancel_request{id = Id, secret = Secret}) ->
    {<<>>, <<1234:16/integer, 5678:16/integer, Id:32/integer, Secret:32/integer>>};
%encode(#msg_copy_data{}) ->
%encode(#msg_copy_done{}) ->
%encode(#msg_copy_fail{}) ->
encode(#msg_close{type = Type, name = Name}) ->
    {$C, [case Type of statement -> $S; portal -> $P end, Name, 0]};
encode(#msg_describe{type = Type, name = Query}) ->
    {$D, [case Type of statement -> $S; portal -> $P end, Query, 0]};
encode(#msg_execute{portal = Portal, limit = Limit}) ->
    {$E, [Portal, 0, <<Limit:32/integer>>]};
encode(#msg_flush{}) ->
    {$H, <<>>};
%encode(#msg_function_call{}) ->
encode(#msg_parse{name = Name, statement = Query, types = Types}) ->
    TypesCount = <<(length(Types)):16/integer>>,
    {$P, [
        Name, 0,
        Query, 0,
        TypesCount,
        << <<Type:32/integer>> || Type <- Types >>
    ]};
encode(#msg_password{password = Password}) ->
    {$p, [Password, 0]};
encode(#msg_query{query = Query}) ->
    {$Q, [Query, 0]};
encode(#msg_sasl_initial_response{mechanism = Mechanism, data = Data}) ->
    Payload = if
        Data == undefined ->
            <<-1:32/signed-integer>>;
        Data /= undefined ->
            DataSize = iolist_size(Data),
            [<<DataSize:32/signed-integer>>, Data]
    end,
    {$p, [Mechanism, 0, Payload]};
encode(#msg_sasl_response{data = Data}) ->
    {$p, Data};
encode(#msg_startup{parameters = Parameters}) ->
    {<<>>, [
        <<?PROTOCOL_VERSION_MAJOR:16/integer, ?PROTOCOL_VERSION_MINOR:16/integer>>,
        [[atom_to_binary(Key, utf8), 0, Value, 0] || {Key, Value} <- maps:to_list(Parameters)],
        0
    ]};
encode(#msg_sync{}) ->
    {$S, <<>>};
encode(#msg_terminate{}) ->
    {$X, <<>>}.

%% @private
encode_format(text) -> 0;
encode_format(binary) -> 1.

-spec decode(byte(), binary()) -> message_b() | message_fb().
decode($R, <<Type:32/integer, Payload/binary>>) ->
    case Type of
        0 -> #msg_auth{type = ok};
        2 -> #msg_auth{type = kerberos};
        3 -> #msg_auth{type = cleartext};
        5 -> #msg_auth{type = md5, data = Payload};
        7 -> #msg_auth{type = gss};
        8 -> #msg_auth{type = gss_continue, data = Payload};
        9 -> #msg_auth{type = sspi};
        10 -> #msg_auth{type = sasl, data = Payload};
        11 -> #msg_auth{type = sasl_continue, data = Payload};
        12 -> #msg_auth{type = sasl_final, data = Payload};
        _ -> #msg_auth{type = Type, data = Payload}
    end;
decode($K, <<Id:32/integer, Secret:32/integer>>) ->
    #msg_backend_key_data{id = Id, secret = Secret};
decode($2, <<>>) ->
    #msg_bind_complete{};
decode($3, <<>>) ->
    #msg_close_complete{};
decode($C, Payload) ->
    {Tag, <<>>} = decode_string(Payload),
    #msg_command_complete{tag = Tag};
decode($d, Payload) ->
    #msg_copy_data{data = Payload};
decode($c, <<>>) ->
    #msg_copy_done{};
%decode($G, Payload) -> #msg_copy_in_response{};
%decode($H, Payload) -> #msg_copy_out_response{};
%decode($W, Payload) -> #msg_copy_both_response{};
decode($D, <<Count:16/integer, Payload/binary>>) ->
    Values = decode_row_values(Payload),
    Count = length(Values),
    #msg_data_row{
        count = Count,
        values = Values
    };
decode($I, <<>>) ->
    #msg_empty_query_response{};
decode($E, Payload) ->
    #msg_error_response{fields = decode_notice_fields(Payload)};
%decode($V, Payload) -> #msg_function_call_response{};
decode($n, <<>>) ->
    #msg_no_data{};
decode($N, Payload) ->
    #msg_notice_response{fields = decode_notice_fields(Payload)};
decode($A, <<Id:32/integer, Payload/binary>>) ->
    {Channel, Rest} = decode_string(Payload),
    {Message, <<>>} = decode_string(Rest),
    #msg_notification_response{id = Id, channel = Channel, payload = Message};
decode($t, <<Count:16/integer, Payload/binary>>) ->
    Types = [ Oid || <<Oid:32/integer>> <= Payload ],
    Count = length(Types),
    #msg_parameter_description{
        count = Count,
        types = Types
    };
decode($S, Payload) ->
    {Name, Rest} = decode_string(Payload),
    {Value, <<>>} = decode_string(Rest),
    #msg_parameter_status{name = Name, value = Value};
decode($1, <<>>) ->
    #msg_parse_complete{};
decode($s, <<>>) ->
    #msg_portal_suspended{};
decode($Z, <<Status>>) ->
    #msg_ready_for_query{
        status = case Status of
            $I -> idle;
            $T -> transaction;
            $E -> error
        end
    };
decode($T, <<Count:16/integer, Payload/binary>>) ->
    Fields = decode_row_description_fields(Payload),
    Count = length(Fields),
    #msg_row_description{
        count = Count,
        fields = Fields
    }.

%% @private
decode_row_values(Data) ->
    decode_row_values(Data, []).

decode_row_values(<<>>, Acc) ->
    lists:reverse(Acc);
decode_row_values(<<-1:32/signed-integer, Rest/binary>>, Acc) ->
    decode_row_values(Rest, [null | Acc]);
decode_row_values(<<Size:32/signed-integer, Value:Size/binary, Rest/binary>>, Acc) ->
    decode_row_values(Rest, [Value | Acc]).

%% @private
decode_notice_fields(Data) ->
    decode_notice_fields(Data, #{}).

decode_notice_fields(<<0>>, Acc) ->
    Acc;
decode_notice_fields(<<TypeCode, Payload/binary>>, Acc) ->
    case decode_notice_field(TypeCode, Payload) of
        {Field, Value, Rest} when is_atom(Field) ->
            decode_notice_fields(Rest, Acc#{Field => Value});
        {_Field, _Value, Rest} ->
            decode_notice_fields(Rest, Acc)
    end.

%% @private
decode_notice_field(TypeCode, Payload) ->
    {String, Rest} = decode_string(Payload),
    Type = decode_notice_field_type(TypeCode),
    Value = decode_notice_field_value(Type, String),
    {Type, Value, Rest}.

%% @private
decode_notice_field_type($V) -> severity;
decode_notice_field_type($C) -> code;
decode_notice_field_type($M) -> message;
decode_notice_field_type($D) -> detail;
decode_notice_field_type($H) -> hint;
decode_notice_field_type($P) -> position;
decode_notice_field_type($p) -> internal_position;
decode_notice_field_type($q) -> internal_query;
decode_notice_field_type($W) -> where;
decode_notice_field_type($s) -> schema;
decode_notice_field_type($t) -> table;
decode_notice_field_type($c) -> column;
decode_notice_field_type($d) -> data_type;
decode_notice_field_type($n) -> constraint;
decode_notice_field_type($F) -> file;
decode_notice_field_type($L) -> line;
decode_notice_field_type($R) -> routine;
decode_notice_field_type(T) -> T.

%% @private
decode_notice_field_value(severity, String) ->
    case String of
        %% error messages
        <<"FATAL">> -> fatal;
        <<"PANIC">> -> panic;
        <<"ERROR">> -> error;
        %% notice messages
        <<"WARNING">> -> warning;
        <<"NOTICE">> -> notice;
        <<"DEBUG">> -> debug;
        <<"INFO">> -> info;
        <<"LOG">> -> log
    end;
decode_notice_field_value(position, String) ->
    erlang:binary_to_integer(String);
decode_notice_field_value(internal_position, String) ->
    erlang:binary_to_integer(String);
decode_notice_field_value(where, String) ->
    string:split(String, <<$\n>>, all);
decode_notice_field_value(line, String) ->
    erlang:binary_to_integer(String);
decode_notice_field_value(_Type, String) ->
    String.

%% @private
decode_row_description_fields(Payload) ->
    decode_row_description_fields(Payload, []).

decode_row_description_fields(<<>>, Acc) ->
    lists:reverse(Acc);
decode_row_description_fields(Payload, Acc) ->
    {Name, <<
        TableOid:32/integer,
        FieldNumber:16/integer,
        TypeOid:32/integer,
        TypeSize:16/signed-integer,
        TypeModifier:32/signed-integer,
        Format:16/integer,
        Rest/binary>>} = decode_string(Payload),
    decode_row_description_fields(Rest, [#pgc_row_field{
        name = Name,
        table_oid = TableOid,
        field_number = FieldNumber,
        type_oid = TypeOid,
        type_size = TypeSize,
        type_modifier = TypeModifier,
        format = decode_format(Format)
    } | Acc]).


%% @private
decode_string(Data) -> decode_string(Data, <<>>).

decode_string(<<0, Rest/binary>>, Acc) -> {binary:copy(Acc), Rest};
decode_string(<<C, Rest/binary>>, Acc) -> decode_string(Rest, <<Acc/binary, C>>).

%% @private
decode_format(0) -> text;
decode_format(1) -> binary.