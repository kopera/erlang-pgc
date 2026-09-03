-module(pgc_protocol_message).
-moduledoc false.

-export([
    encode/1,
    decode/2
]).
-export_record([
    auth,
    backend_key_data,
    bind,
    bind_complete,
    cancel_request,
    close,
    close_complete,
    command_complete,
    copy_data,
    copy_done,
    copy_fail,
    copy_in_response,
    copy_out_response,
    copy_both_response,
    data_row,
    describe,
    empty_query_response,
    error_response,
    execute,
    flush,
    negotiate_protocol_version,
    no_data,
    notice_response,
    notification_response,
    parameter_description,
    parameter_status,
    parse,
    parse_complete,
    password,
    portal_suspended,
    query,
    ready_for_query,
    row_description,
    sasl_initial_response,
    sasl_response,
    startup,
    sync,
    terminate,

    row_description_field
]).
-export_type([
    t/0,
    message_f/0,
    message_b/0,
    message_fb/0,

    row_description_field/0,
    error_response_fields/0,
    notice_response_fields/0
]).


% -----------------------------------------------------------------------------
% Protocol message fields
% -----------------------------------------------------------------------------

-record #row_description_field{
    name :: binary(),
    table_oid :: pgc_protocol:oid() | 0,
    field_number :: pos_integer() | 0,
    type_oid :: pgc_protocol:oid(),
    type_size :: integer(),
    type_modifier :: integer(),
    format :: text | binary
}.
-type row_description_field() :: #row_description_field{}.

-type error_response_fields() :: #{
    severity := panic | fatal | error,
    code := binary(),
    message := unicode:unicode_binary(),
    detail => unicode:unicode_binary(),
    hint => unicode:unicode_binary(),
    position => pos_integer(),
    internal_position => pos_integer(),
    internal_query => unicode:unicode_binary(),
    where => [unicode:unicode_binary()],
    schema => unicode:unicode_binary(),
    table => unicode:unicode_binary(),
    column => unicode:unicode_binary(),
    data_type => unicode:unicode_binary(),
    constraint => unicode:unicode_binary(),
    file => unicode:unicode_binary(),
    line => non_neg_integer(),
    routine => unicode:unicode_binary()
}.

-type notice_response_fields() :: #{
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

% -----------------------------------------------------------------------------
% Protocol messages
% -----------------------------------------------------------------------------
-record #auth{
    type ::  ok
        | kerberos
        | cleartext
        | md5
        | gss
        | gss_continue
        | sspi
        | sasl
        | sasl_continue
        | sasl_final
        | byte(),
    data :: binary()
}.
-record #backend_key_data{
    id :: non_neg_integer(),
    secret :: binary()
}.
-record #bind{
    portal :: iodata(),
    statement :: iodata(),
    parameters :: [{binary | text, iodata() | null}],
    results :: [binary | text]
}.
-record #bind_complete{}.
-record #cancel_request{
    id :: non_neg_integer(),
    secret :: binary()
}.
-record #close{
    type :: statement | portal,
    name :: iodata()
}.
-record #close_complete{}.
-record #command_complete{
    tag :: binary()
}.
-record #copy_data{
    data :: iodata()
}.
-record #copy_done{}.
-record #copy_fail{
    message :: binary()
}.
-record #copy_in_response{
    format :: text | binary,
    columns :: [text | binary]
}.
-record #copy_out_response{
    format :: text | binary,
    columns :: [text | binary]
}.
-record #copy_both_response{
    format :: text | binary,
    columns :: [text | binary]
}.
-record #data_row{
    count :: non_neg_integer(),
    values :: [null | binary()]
}.
-record #describe{
    type :: statement | portal,
    name :: iodata()
}.
-record #empty_query_response{}.
-record #error_response{
    fields :: error_response_fields()
}.
-record #execute{
    portal :: iodata(),
    limit :: non_neg_integer()
}.
-record #flush{}.
-record #negotiate_protocol_version{
    minor :: non_neg_integer(),
    options :: [binary()]
}.
-record #no_data{}.
-record #notice_response{
    fields :: notice_response_fields()
}.
-record #notification_response{
    id :: non_neg_integer(),
    channel :: binary(),
    payload :: binary()
}.
-record #parameter_description{
    count :: non_neg_integer(),
    types :: [pgc_protocol:oid()]
}.
-record #parameter_status{
    name :: binary(),
    value :: binary()
}.
-record #parse{
    name :: iodata(),
    statement :: iodata(),
    types :: [pgc_protocol:oid() | 0]
}.
-record #parse_complete{}.
-record #password{
    password :: iodata()
}.
-record #portal_suspended{}.
-record #query{
    text :: iodata()
}.
-record #ready_for_query{
    status :: idle | transaction | error
}.
-record #row_description{
    count :: non_neg_integer(),
    fields :: [#row_description_field{}]
}.
-record #sasl_initial_response{
    mechanism :: binary(),
    data :: iodata() | undefined
}.
-record #sasl_response{
    data :: iodata()
}.
-record #startup{
    version :: {non_neg_integer(), non_neg_integer()},
    parameters :: #{atom() => unicode:chardata()}
}.
-record #sync{}.
-record #terminate{}.

% -----------------------------------------------------------------------------
% Types
% -----------------------------------------------------------------------------

-type t() :: message_f() | message_b() | message_fb().
-type message_f() ::
      #bind{}
    | #cancel_request{}
    | #close{}
    | #copy_fail{}
    | #describe{}
    | #execute{}
    | #flush{}
    | #parse{}
    | #password{}
    | #sasl_initial_response{}
    | #sasl_response{}
    | #query{}
    | #startup{}
    | #sync{}
    | #terminate{}.
-type message_b() ::
      #auth{}
    | #backend_key_data{}
    | #bind_complete{}
    | #close_complete{}
    | #command_complete{}
    | #copy_in_response{}
    | #copy_out_response{}
    | #copy_both_response{}
    | #data_row{}
    | #empty_query_response{}
    | #error_response{}
    | #negotiate_protocol_version{}
    | #no_data{}
    | #notice_response{}
    | #notification_response{}
    | #parameter_description{}
    | #parameter_status{}
    | #parse_complete{}
    | #portal_suspended{}
    | #ready_for_query{}
    | #row_description{}.
-type message_fb() ::
      #copy_data{}
    | #copy_done{}.


% -----------------------------------------------------------------------------
% API
% -----------------------------------------------------------------------------

-doc """
Encodes a single frontend message into its wire representation.

Returns the message's type byte and payload; `StartupMessage` and
`CancelRequest` have no type byte, so `<<>>` is returned for `Type` in those
cases. Framing (the 4-byte length prefix) is added by `pgc_protocol`, not
here.
""".
-spec encode(message_f() | message_fb()) -> {Type :: byte() | <<>>, Payload :: iodata()}.
encode(#bind{portal = Portal, statement = Statement, parameters = Parameters, results = Results}) ->
    ParametersCount = <<(length(Parameters)):16>>,
    ParametersFormats = << <<(encode_format(Format)):16>> || {Format, _} <- Parameters >>,
    ParametersValues = [case Value of
        null -> <<-1:32/signed>>;
        _ -> [<<(iolist_size(Value)):32/signed>>, Value]
    end || {_, Value} <- Parameters],
    ResultsCount = <<(length(Results)):16>>,
    ResultsFormats = << <<(encode_format(Format)):16>> || Format <- Results >>,
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
encode(#cancel_request{id = Id, secret = Secret}) ->
    {<<>>, <<1234:16, 5678:16, Id:32, Secret/binary>>};
encode(#copy_data{data = Data}) ->
    {$d, Data};
encode(#copy_done{}) ->
    {$c, []};
encode(#copy_fail{message = Message}) ->
    {$f, [Message, 0]};
encode(#close{type = Type, name = Name}) ->
    {$C, [case Type of statement -> $S; portal -> $P end, Name, 0]};
encode(#describe{type = Type, name = Query}) ->
    {$D, [case Type of statement -> $S; portal -> $P end, Query, 0]};
encode(#execute{portal = Portal, limit = Limit}) ->
    {$E, [Portal, 0, <<Limit:32>>]};
encode(#flush{}) ->
    {$H, []};
encode(#parse{name = Name, statement = Query, types = Types}) ->
    TypesCount = <<(length(Types)):16>>,
    {$P, [
        Name, 0,
        Query, 0,
        TypesCount,
        << <<Type:32>> || Type <- Types >>
    ]};
encode(#password{password = Password}) ->
    {$p, [Password, 0]};
encode(#query{text = Text}) ->
    {$Q, [Text, 0]};
encode(#sasl_initial_response{mechanism = Mechanism, data = Data}) ->
    Payload = if
        Data == undefined ->
            <<-1:32/signed>>;
        Data /= undefined ->
            DataSize = iolist_size(Data),
            [<<DataSize:32/signed>>, Data]
    end,
    {$p, [Mechanism, 0, Payload]};
encode(#sasl_response{data = Data}) ->
    {$p, Data};
encode(#startup{version = {Major, Minor}, parameters = Parameters}) ->
    {<<>>, [
        <<Major:16, Minor:16>>,
        [
            atom_to_binary(Key, utf8), 0, Value, 0
            || {Key, Value} <- maps:to_list(Parameters)
        ],
        0
    ]};
encode(#sync{}) ->
    {$S, <<>>};
encode(#terminate{}) ->
    {$X, <<>>}.


-spec encode_format(text | binary) -> byte().
encode_format(text) -> 0;
encode_format(binary) -> 1.


-doc """
Decodes a single backend message from its type byte and payload (with the
4-byte length prefix already stripped by `pgc_protocol`).
""".
-spec decode(byte(), binary()) -> message_b() | message_fb().
decode($R, <<Type:32, Payload/binary>>) ->
    case Type of
        0 -> #auth{type = ok, data = Payload};
        2 -> #auth{type = kerberos, data = Payload};
        3 -> #auth{type = cleartext, data = Payload};
        5 -> #auth{type = md5, data = Payload};
        7 -> #auth{type = gss, data = Payload};
        8 -> #auth{type = gss_continue, data = Payload};
        9 -> #auth{type = sspi, data = Payload};
        10 -> #auth{type = sasl, data = Payload};
        11 -> #auth{type = sasl_continue, data = Payload};
        12 -> #auth{type = sasl_final, data = Payload};
        _ -> #auth{type = Type, data = Payload}
    end;
decode($K, <<Id:32, Secret/binary>>) ->
    #backend_key_data{id = Id, secret = Secret};
decode($2, <<>>) ->
    #bind_complete{};
decode($3, <<>>) ->
    #close_complete{};
decode($C, Payload) ->
    {Tag, _} = decode_string(Payload),
    #command_complete{tag = Tag};
decode($G, <<Format:8, Columns:16, ColumnFormats:Columns/binary-unit:16>>) ->
    #copy_in_response{
        format = decode_format(Format),
        columns = [decode_format(ColumnFormat) || <<ColumnFormat:16>> <= ColumnFormats]
    };
decode($H, <<Format:8, Columns:16, ColumnFormats:Columns/binary-unit:16>>) ->
    #copy_out_response{
        format = decode_format(Format),
        columns = [decode_format(ColumnFormat) || <<ColumnFormat:16>> <= ColumnFormats]
    };
decode($W, <<Format:8, Columns:16, ColumnFormats:Columns/binary-unit:16>>) ->
    #copy_both_response{
        format = decode_format(Format),
        columns = [decode_format(ColumnFormat) || <<ColumnFormat:16>> <= ColumnFormats]
    };
decode($d, Payload) ->
    #copy_data{data = Payload};
decode($c, <<>>) ->
    #copy_done{};
decode($D, <<Count:16, Payload/binary>>) ->
    #data_row{
        count = Count,
        values = decode_row_values(Count, Payload)
    };
decode($I, <<>>) ->
    #empty_query_response{};
decode($E, Payload) ->
    #error_response{fields = decode_notice_fields(Payload)};
decode($v, <<Minor:32, OptionsCount:32, Options/binary>>) ->
    #negotiate_protocol_version{
        minor = Minor,
        options = decode_strings(OptionsCount, Options)
    };
decode($n, <<>>) ->
    #no_data{};
decode($N, Payload) ->
    #notice_response{fields = decode_notice_fields(Payload)};
decode($A, <<Id:32, Payload/binary>>) ->
    {Channel, Rest} = decode_string(Payload),
    {Message, <<>>} = decode_string(Rest),
    #notification_response{id = Id, channel = Channel, payload = Message};
decode($t, <<Count:16, Payload:Count/binary-unit:32>>) ->
    #parameter_description{
        count = Count,
        types = [Oid || <<Oid:32>> <= Payload]
    };
decode($S, Payload) ->
    {Name, Rest} = decode_string(Payload),
    {Value, <<>>} = decode_string(Rest),
    #parameter_status{name = Name, value = Value};
decode($1, <<>>) ->
    #parse_complete{};
decode($s, <<>>) ->
    #portal_suspended{};
decode($Z, <<Status>>) ->
    #ready_for_query{
        status = case Status of
            $I -> idle;
            $T -> transaction;
            $E -> error
        end
    };
decode($T, <<Count:16, Payload/binary>>) ->
    #row_description{
        count = Count,
        fields = decode_row_description_fields(Count, Payload)
    }.

-spec decode_row_values(non_neg_integer(), binary()) -> [null | binary()].
decode_row_values(0, <<>>) ->
    [];
decode_row_values(ExpectedCount, <<-1:32/signed-integer, Rest/binary>>) ->
    [null | decode_row_values(ExpectedCount - 1, Rest)];
decode_row_values(ExpectedCount, <<Size:32/signed-integer, Value:Size/binary, Rest/binary>>) ->
    [Value | decode_row_values(ExpectedCount - 1, Rest)].


-spec decode_notice_fields(binary()) -> #{atom() => dynamic()}.
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

decode_notice_field(TypeCode, Payload) ->
    {String, Rest} = decode_string(Payload),
    Type = decode_notice_field_type(TypeCode),
    Value = decode_notice_field_value(Type, String),
    {Type, Value, Rest}.

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

decode_notice_field_value(severity, String) ->
    case String of
        % error messages
        <<"FATAL">> -> fatal;
        <<"PANIC">> -> panic;
        <<"ERROR">> -> error;
        % notice messages
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

-spec decode_row_description_fields(non_neg_integer(), binary()) -> [#row_description_field{}].
decode_row_description_fields(0, <<>>) ->
    [];
decode_row_description_fields(Count, Payload) ->
    {Name, <<
        TableOid:32,
        FieldNumber:16,
        TypeOid:32,
        TypeSize:16/signed,
        TypeModifier:32/signed,
        Format:16,
        Rest/binary
    >>} = decode_string(Payload),
    [#row_description_field{
        name = Name,
        table_oid = TableOid,
        field_number = FieldNumber,
        type_oid = TypeOid,
        type_size = TypeSize,
        type_modifier = TypeModifier,
        format = decode_format(Format)
    } | decode_row_description_fields(Count - 1, Rest)].


-spec decode_string(binary()) -> {String :: binary(), Rest :: binary()}.
decode_string(Data) -> decode_string(Data, <<>>).

decode_string(<<0, Rest/binary>>, Acc) -> {binary:copy(Acc), Rest};
decode_string(<<C, Rest/binary>>, Acc) -> decode_string(Rest, <<Acc/binary, C>>).


-spec decode_strings(non_neg_integer(), binary()) -> [String :: binary()].
decode_strings(0, <<>>) -> [];
decode_strings(Count, Data) when Count > 0 ->
    {String, Rest} = decode_string(Data),
    [String | decode_strings(Count - 1, Rest)].


-spec decode_format(0 | 1) -> text | binary.
decode_format(0) -> text;
decode_format(1) -> binary.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

do_encode(Message) ->
    {Type, Payload} = encode(Message),
    {Type, iolist_to_binary(Payload)}.

%% -----------------------------------------------------------------------
%% Frontend messages (encode/1)
%% -----------------------------------------------------------------------

bind_encode_test() ->
    Message = #bind{
        portal = <<"p">>,
        statement = <<"s">>,
        parameters = [{text, <<"a">>}, {binary, <<1, 2>>}, {text, null}],
        results = [text, binary]
    },
    Expected = iolist_to_binary([
        <<"p">>, 0,
        <<"s">>, 0,
        <<3:16>>,
        <<0:16, 1:16, 0:16>>,
        <<3:16>>,
        <<1:32/signed>>, <<"a">>,
        <<2:32/signed>>, 1, 2,
        <<-1:32/signed>>,
        <<2:16>>,
        <<0:16, 1:16>>
    ]),
    ?assertEqual({$B, Expected}, do_encode(Message)).

cancel_request_encode_test() ->
    Message = #cancel_request{id = 42, secret = <<99:32>>},
    ?assertEqual({<<>>, <<1234:16, 5678:16, 42:32, 99:32>>}, do_encode(Message)).

close_encode_test() ->
    ?assertEqual({$C, <<$S, "foo", 0>>}, do_encode(#close{type = statement, name = <<"foo">>})),
    ?assertEqual({$C, <<$P, "bar", 0>>}, do_encode(#close{type = portal, name = <<"bar">>})).

describe_encode_test() ->
    ?assertEqual({$D, <<$S, "foo", 0>>}, do_encode(#describe{type = statement, name = <<"foo">>})),
    ?assertEqual({$D, <<$P, "bar", 0>>}, do_encode(#describe{type = portal, name = <<"bar">>})).

execute_encode_test() ->
    Message = #execute{portal = <<"p">>, limit = 10},
    ?assertEqual({$E, <<"p", 0, 10:32>>}, do_encode(Message)).

flush_encode_test() ->
    ?assertEqual({$H, <<>>}, do_encode(#flush{})).

parse_encode_test() ->
    Message = #parse{name = <<"n">>, statement = <<"select 1">>, types = [23, 25]},
    Expected = iolist_to_binary([<<"n">>, 0, <<"select 1">>, 0, <<2:16>>, <<23:32, 25:32>>]),
    ?assertEqual({$P, Expected}, do_encode(Message)).

password_encode_test() ->
    ?assertEqual({$p, <<"secret", 0>>}, do_encode(#password{password = <<"secret">>})).

query_encode_test() ->
    ?assertEqual({$Q, <<"select 1", 0>>}, do_encode(#query{text = <<"select 1">>})).

sasl_initial_response_with_data_encode_test() ->
    Message = #sasl_initial_response{mechanism = <<"SCRAM-SHA-256">>, data = <<"abc">>},
    Expected = iolist_to_binary([<<"SCRAM-SHA-256">>, 0, <<3:32/signed>>, <<"abc">>]),
    ?assertEqual({$p, Expected}, do_encode(Message)).

sasl_initial_response_without_data_encode_test() ->
    Message = #sasl_initial_response{mechanism = <<"SCRAM-SHA-256">>, data = undefined},
    Expected = iolist_to_binary([<<"SCRAM-SHA-256">>, 0, <<-1:32/signed>>]),
    ?assertEqual({$p, Expected}, do_encode(Message)).

sasl_response_encode_test() ->
    ?assertEqual({$p, <<"xyz">>}, do_encode(#sasl_response{data = <<"xyz">>})).

startup_encode_test() ->
    Message = #startup{version = {3, 0}, parameters = #{user => <<"postgres">>}},
    Expected = iolist_to_binary([<<3:16, 0:16>>, <<"user">>, 0, <<"postgres">>, 0, 0]),
    ?assertEqual({<<>>, Expected}, do_encode(Message)).

sync_encode_test() ->
    ?assertEqual({$S, <<>>}, do_encode(#sync{})).

terminate_encode_test() ->
    ?assertEqual({$X, <<>>}, do_encode(#terminate{})).

%% -----------------------------------------------------------------------
%% Backend messages (decode/2)
%% -----------------------------------------------------------------------

auth_ok_decode_test() ->
    ?assertEqual(#auth{type = ok, data = <<>>}, decode($R, <<0:32>>)).

auth_md5_decode_test() ->
    ?assertEqual(
        #auth{type = md5, data = <<1, 2, 3, 4>>},
        decode($R, <<5:32, 1, 2, 3, 4>>)
    ).

auth_sasl_decode_test() ->
    ?assertEqual(
        #auth{type = sasl, data = <<"SCRAM-SHA-256", 0>>},
        decode($R, <<10:32, "SCRAM-SHA-256", 0>>)
    ).

auth_sasl_continue_decode_test() ->
    ?assertEqual(
        #auth{type = sasl_continue, data = <<"challenge-data">>},
        decode($R, <<11:32, "challenge-data">>)
    ).

backend_key_data_decode_test() ->
    ?assertEqual(#backend_key_data{id = 111, secret = <<222:32>>}, decode($K, <<111:32, 222:32>>)),
    ?assertEqual(#backend_key_data{id = 111, secret = <<"Version 3.2 long key">>}, decode($K, <<111:32, "Version 3.2 long key">>)).

bind_complete_decode_test() ->
    ?assertEqual(#bind_complete{}, decode($2, <<>>)).

close_complete_decode_test() ->
    ?assertEqual(#close_complete{}, decode($3, <<>>)).

command_complete_decode_test() ->
    ?assertEqual(
        #command_complete{tag = <<"SELECT 1">>},
        decode($C, <<"SELECT 1", 0>>)
    ).

copy_data_decode_test() ->
    ?assertEqual(#copy_data{data = <<1, 2, 3>>}, decode($d, <<1, 2, 3>>)).

copy_done_decode_test() ->
    ?assertEqual(#copy_done{}, decode($c, <<>>)).

data_row_decode_test() ->
    Payload = <<2:16, -1:32/signed, 3:32/signed, "abc">>,
    ?assertEqual(
        #data_row{count = 2, values = [null, <<"abc">>]},
        decode($D, Payload)
    ).

empty_query_response_decode_test() ->
    ?assertEqual(#empty_query_response{}, decode($I, <<>>)).

error_response_decode_test() ->
    Payload = <<
        $V, "ERROR", 0,
        $C, "42P01", 0,
        $M, "oops", 0,
        $P, "10", 0,
        $W, "line1\nline2", 0,
        0
    >>,
    Expected = #error_response{fields = #{
        severity => error,
        code => <<"42P01">>,
        message => <<"oops">>,
        position => 10,
        where => [<<"line1">>, <<"line2">>]
    }},
    ?assertEqual(Expected, decode($E, Payload)).

notice_response_decode_test() ->
    Payload = <<
        $V, "WARNING", 0,
        $C, "01000", 0,
        $M, "heads up", 0,
        0
    >>,
    Expected = #notice_response{fields = #{
        severity => warning,
        code => <<"01000">>,
        message => <<"heads up">>
    }},
    ?assertEqual(Expected, decode($N, Payload)).

no_data_decode_test() ->
    ?assertEqual(#no_data{}, decode($n, <<>>)).

notification_response_decode_test() ->
    Payload = <<555:32, "mychan", 0, "payload", 0>>,
    ?assertEqual(
        #notification_response{id = 555, channel = <<"mychan">>, payload = <<"payload">>},
        decode($A, Payload)
    ).

parameter_description_decode_test() ->
    ?assertEqual(
        #parameter_description{count = 2, types = [23, 25]},
        decode($t, <<2:16, 23:32, 25:32>>)
    ).

parameter_status_decode_test() ->
    ?assertEqual(
        #parameter_status{name = <<"server_version">>, value = <<"16.0">>},
        decode($S, <<"server_version", 0, "16.0", 0>>)
    ).

parse_complete_decode_test() ->
    ?assertEqual(#parse_complete{}, decode($1, <<>>)).

portal_suspended_decode_test() ->
    ?assertEqual(#portal_suspended{}, decode($s, <<>>)).

ready_for_query_decode_test() ->
    ?assertEqual(#ready_for_query{status = idle}, decode($Z, <<$I>>)),
    ?assertEqual(#ready_for_query{status = transaction}, decode($Z, <<$T>>)),
    ?assertEqual(#ready_for_query{status = error}, decode($Z, <<$E>>)).

row_description_decode_test() ->
    Payload = <<
        2:16,
        "id", 0, 16384:32, 1:16, 23:32, 4:16/signed, -1:32/signed, 0:16,
        "name", 0, 0:32, 0:16, 25:32, -1:16/signed, -1:32/signed, 1:16
    >>,
    Expected = #row_description{
        count = 2,
        fields = [
            #row_description_field{
                name = <<"id">>,
                table_oid = 16384,
                field_number = 1,
                type_oid = 23,
                type_size = 4,
                type_modifier = -1,
                format = text
            },
            #row_description_field{
                name = <<"name">>,
                table_oid = 0,
                field_number = 0,
                type_oid = 25,
                type_size = -1,
                type_modifier = -1,
                format = binary
            }
        ]
    },
    ?assertEqual(Expected, decode($T, Payload)).

-endif.
