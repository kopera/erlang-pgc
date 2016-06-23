-module(pgsql_protocol_messages).
-export([
    encode/1,
    decode/2
]).
-export_type([
    message/0,
    message_f/0,
    message_b/0,
    message_fb/0
]).

-include("pgsql_protocol_messages.hrl").
-define(PROTOCOL_VERSION_MAJOR, 3).
-define(PROTOCOL_VERSION_MINOR, 0).

-type message() :: message_f() | message_b() | message_fb().
-type message_f() :: #msg_bind{}
    | #msg_cancel_request{}
    | #msg_close{}
    | #msg_copy_fail{}
    | #msg_describe{}
    | #msg_execute{}
    | #msg_flush{}
    | #msg_function_call{}
    | #msg_parse{}
    | #msg_password{}
    | #msg_query{}
    | #msg_ssl_request{}
    | #msg_startup{}
    | #msg_sync{}
    | #msg_terminate{}.
-type message_b() :: #msg_auth{}
    | #msg_backend_key_data{}
    | #msg_bind_complete{}
    | #msg_close_complete{}
    | #msg_command_complete{}
    | #msg_copy_in_response{}
    | #msg_copy_out_response{}
    | #msg_copy_both_response{}
    | #msg_data_row{}
    | #msg_empty_query_response{}
    | #msg_error_response{}
    | #msg_function_call_response{}
    | #msg_no_data{}
    | #msg_notice_response{}
    | #msg_notification_response{}
    | #msg_parameter_description{}
    | #msg_parameter_status{}
    | #msg_parse_complete{}
    | #msg_portal_suspended{}
    | #msg_ready_for_query{}
    | #msg_row_description{}.
-type message_fb() :: #msg_copy_data{}
    | #msg_copy_done{}.

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
encode(#msg_ssl_request{}) ->
    {<<>>, <<1234:16/integer, 5679:16/integer>>};
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
        6 -> #msg_auth{type = scm};
        7 -> #msg_auth{type = gss};
        9 -> #msg_auth{type = sspi};
        8 -> #msg_auth{type = gss_continue, data = Payload};
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
    #{code := Code} = Fields = decode_notice_fields(Payload),
    #msg_error_response{fields = maps:put(name, error_code_to_name(Code), Fields)};
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
    decode_notice_fields(Data, []).

decode_notice_fields(<<0>>, Acc) ->
    maps:from_list(Acc);
decode_notice_fields(<<Type, Payload/binary>>, Acc) ->
    case decode_notice_field(Type, Payload) of
        {Field, Rest} -> decode_notice_fields(Rest, [Field | Acc]);
        Rest -> decode_notice_fields(Rest, Acc)
    end.

%% @private
decode_notice_field(Code, Payload) ->
    Type = decode_notice_field_type(Code),
    case decode_string(Payload) of
        {Value, Rest} when is_atom(Type) -> {{Type, Value}, Rest};
        {_, Rest} -> Rest
    end.

%% @private
decode_notice_field_type($S) -> severity;
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
    decode_row_description_fields(Rest, [#msg_row_description_field{
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

%% @private

error_code_to_name(<<"00000">>) -> successful_completion;
error_code_to_name(<<"01000">>) -> warning;
error_code_to_name(<<"0100C">>) -> dynamic_result_sets_returned;
error_code_to_name(<<"01008">>) -> implicit_zero_bit_padding;
error_code_to_name(<<"01003">>) -> null_value_eliminated_in_set_function;
error_code_to_name(<<"01007">>) -> privilege_not_granted;
error_code_to_name(<<"01006">>) -> privilege_not_revoked;
error_code_to_name(<<"01004">>) -> string_data_right_truncation;
error_code_to_name(<<"01P01">>) -> deprecated_feature;
error_code_to_name(<<"02000">>) -> no_data;
error_code_to_name(<<"02001">>) -> no_additional_dynamic_result_sets_returned;
error_code_to_name(<<"03000">>) -> sql_statement_not_yet_complete;
error_code_to_name(<<"08000">>) -> connection_exception;
error_code_to_name(<<"08003">>) -> connection_does_not_exist;
error_code_to_name(<<"08006">>) -> connection_failure;
error_code_to_name(<<"08001">>) -> sqlclient_unable_to_establish_sqlconnection;
error_code_to_name(<<"08004">>) -> sqlserver_rejected_establishment_of_sqlconnection;
error_code_to_name(<<"08007">>) -> transaction_resolution_unknown;
error_code_to_name(<<"08P01">>) -> protocol_violation;
error_code_to_name(<<"09000">>) -> triggered_action_exception;
error_code_to_name(<<"0A000">>) -> feature_not_supported;
error_code_to_name(<<"0B000">>) -> invalid_transaction_initiation;
error_code_to_name(<<"0F000">>) -> locator_exception;
error_code_to_name(<<"0F001">>) -> invalid_locator_specification;
error_code_to_name(<<"0L000">>) -> invalid_grantor;
error_code_to_name(<<"0LP01">>) -> invalid_grant_operation;
error_code_to_name(<<"0P000">>) -> invalid_role_specification;
error_code_to_name(<<"0Z000">>) -> diagnostics_exception;
error_code_to_name(<<"0Z002">>) -> stacked_diagnostics_accessed_without_active_handler;
error_code_to_name(<<"20000">>) -> case_not_found;
error_code_to_name(<<"21000">>) -> cardinality_violation;
error_code_to_name(<<"22000">>) -> data_exception;
error_code_to_name(<<"2202E">>) -> array_subscript_error;
error_code_to_name(<<"22021">>) -> character_not_in_repertoire;
error_code_to_name(<<"22008">>) -> datetime_field_overflow;
error_code_to_name(<<"22012">>) -> division_by_zero;
error_code_to_name(<<"22005">>) -> error_in_assignment;
error_code_to_name(<<"2200B">>) -> escape_character_conflict;
error_code_to_name(<<"22022">>) -> indicator_overflow;
error_code_to_name(<<"22015">>) -> interval_field_overflow;
error_code_to_name(<<"2201E">>) -> invalid_argument_for_logarithm;
error_code_to_name(<<"22014">>) -> invalid_argument_for_ntile_function;
error_code_to_name(<<"22016">>) -> invalid_argument_for_nth_value_function;
error_code_to_name(<<"2201F">>) -> invalid_argument_for_power_function;
error_code_to_name(<<"2201G">>) -> invalid_argument_for_width_bucket_function;
error_code_to_name(<<"22018">>) -> invalid_character_value_for_cast;
error_code_to_name(<<"22007">>) -> invalid_datetime_format;
error_code_to_name(<<"22019">>) -> invalid_escape_character;
error_code_to_name(<<"2200D">>) -> invalid_escape_octet;
error_code_to_name(<<"22025">>) -> invalid_escape_sequence;
error_code_to_name(<<"22P06">>) -> nonstandard_use_of_escape_character;
error_code_to_name(<<"22010">>) -> invalid_indicator_parameter_value;
error_code_to_name(<<"22023">>) -> invalid_parameter_value;
error_code_to_name(<<"2201B">>) -> invalid_regular_expression;
error_code_to_name(<<"2201W">>) -> invalid_row_count_in_limit_clause;
error_code_to_name(<<"2201X">>) -> invalid_row_count_in_result_offset_clause;
error_code_to_name(<<"2202H">>) -> invalid_tablesample_argument;
error_code_to_name(<<"2202G">>) -> invalid_tablesample_repeat;
error_code_to_name(<<"22009">>) -> invalid_time_zone_displacement_value;
error_code_to_name(<<"2200C">>) -> invalid_use_of_escape_character;
error_code_to_name(<<"2200G">>) -> most_specific_type_mismatch;
error_code_to_name(<<"22004">>) -> null_value_not_allowed;
error_code_to_name(<<"22002">>) -> null_value_no_indicator_parameter;
error_code_to_name(<<"22003">>) -> numeric_value_out_of_range;
error_code_to_name(<<"22026">>) -> string_data_length_mismatch;
error_code_to_name(<<"22001">>) -> string_data_right_truncation;
error_code_to_name(<<"22011">>) -> substring_error;
error_code_to_name(<<"22027">>) -> trim_error;
error_code_to_name(<<"22024">>) -> unterminated_c_string;
error_code_to_name(<<"2200F">>) -> zero_length_character_string;
error_code_to_name(<<"22P01">>) -> floating_point_exception;
error_code_to_name(<<"22P02">>) -> invalid_text_representation;
error_code_to_name(<<"22P03">>) -> invalid_binary_representation;
error_code_to_name(<<"22P04">>) -> bad_copy_file_format;
error_code_to_name(<<"22P05">>) -> untranslatable_character;
error_code_to_name(<<"2200L">>) -> not_an_xml_document;
error_code_to_name(<<"2200M">>) -> invalid_xml_document;
error_code_to_name(<<"2200N">>) -> invalid_xml_content;
error_code_to_name(<<"2200S">>) -> invalid_xml_comment;
error_code_to_name(<<"2200T">>) -> invalid_xml_processing_instruction;
error_code_to_name(<<"23000">>) -> integrity_constraint_violation;
error_code_to_name(<<"23001">>) -> restrict_violation;
error_code_to_name(<<"23502">>) -> not_null_violation;
error_code_to_name(<<"23503">>) -> foreign_key_violation;
error_code_to_name(<<"23505">>) -> unique_violation;
error_code_to_name(<<"23514">>) -> check_violation;
error_code_to_name(<<"23P01">>) -> exclusion_violation;
error_code_to_name(<<"24000">>) -> invalid_cursor_state;
error_code_to_name(<<"25000">>) -> invalid_transaction_state;
error_code_to_name(<<"25001">>) -> active_sql_transaction;
error_code_to_name(<<"25002">>) -> branch_transaction_already_active;
error_code_to_name(<<"25008">>) -> held_cursor_requires_same_isolation_level;
error_code_to_name(<<"25003">>) -> inappropriate_access_mode_for_branch_transaction;
error_code_to_name(<<"25004">>) -> inappropriate_isolation_level_for_branch_transaction;
error_code_to_name(<<"25005">>) -> no_active_sql_transaction_for_branch_transaction;
error_code_to_name(<<"25006">>) -> read_only_sql_transaction;
error_code_to_name(<<"25007">>) -> schema_and_data_statement_mixing_not_supported;
error_code_to_name(<<"25P01">>) -> no_active_sql_transaction;
error_code_to_name(<<"25P02">>) -> in_failed_sql_transaction;
error_code_to_name(<<"26000">>) -> invalid_sql_statement_name;
error_code_to_name(<<"27000">>) -> triggered_data_change_violation;
error_code_to_name(<<"28000">>) -> invalid_authorization_specification;
error_code_to_name(<<"28P01">>) -> invalid_password;
error_code_to_name(<<"2B000">>) -> dependent_privilege_descriptors_still_exist;
error_code_to_name(<<"2BP01">>) -> dependent_objects_still_exist;
error_code_to_name(<<"2D000">>) -> invalid_transaction_termination;
error_code_to_name(<<"2F000">>) -> sql_routine_exception;
error_code_to_name(<<"2F005">>) -> function_executed_no_return_statement;
error_code_to_name(<<"2F002">>) -> modifying_sql_data_not_permitted;
error_code_to_name(<<"2F003">>) -> prohibited_sql_statement_attempted;
error_code_to_name(<<"2F004">>) -> reading_sql_data_not_permitted;
error_code_to_name(<<"34000">>) -> invalid_cursor_name;
error_code_to_name(<<"38000">>) -> external_routine_exception;
error_code_to_name(<<"38001">>) -> containing_sql_not_permitted;
error_code_to_name(<<"38002">>) -> modifying_sql_data_not_permitted;
error_code_to_name(<<"38003">>) -> prohibited_sql_statement_attempted;
error_code_to_name(<<"38004">>) -> reading_sql_data_not_permitted;
error_code_to_name(<<"39000">>) -> external_routine_invocation_exception;
error_code_to_name(<<"39001">>) -> invalid_sqlstate_returned;
error_code_to_name(<<"39004">>) -> null_value_not_allowed;
error_code_to_name(<<"39P01">>) -> trigger_protocol_violated;
error_code_to_name(<<"39P02">>) -> srf_protocol_violated;
error_code_to_name(<<"39P03">>) -> event_trigger_protocol_violated;
error_code_to_name(<<"3B000">>) -> savepoint_exception;
error_code_to_name(<<"3B001">>) -> invalid_savepoint_specification;
error_code_to_name(<<"3D000">>) -> invalid_catalog_name;
error_code_to_name(<<"3F000">>) -> invalid_schema_name;
error_code_to_name(<<"40000">>) -> transaction_rollback;
error_code_to_name(<<"40002">>) -> transaction_integrity_constraint_violation;
error_code_to_name(<<"40001">>) -> serialization_failure;
error_code_to_name(<<"40003">>) -> statement_completion_unknown;
error_code_to_name(<<"40P01">>) -> deadlock_detected;
error_code_to_name(<<"42000">>) -> syntax_error_or_access_rule_violation;
error_code_to_name(<<"42601">>) -> syntax_error;
error_code_to_name(<<"42501">>) -> insufficient_privilege;
error_code_to_name(<<"42846">>) -> cannot_coerce;
error_code_to_name(<<"42803">>) -> grouping_error;
error_code_to_name(<<"42P20">>) -> windowing_error;
error_code_to_name(<<"42P19">>) -> invalid_recursion;
error_code_to_name(<<"42830">>) -> invalid_foreign_key;
error_code_to_name(<<"42602">>) -> invalid_name;
error_code_to_name(<<"42622">>) -> name_too_long;
error_code_to_name(<<"42939">>) -> reserved_name;
error_code_to_name(<<"42804">>) -> datatype_mismatch;
error_code_to_name(<<"42P18">>) -> indeterminate_datatype;
error_code_to_name(<<"42P21">>) -> collation_mismatch;
error_code_to_name(<<"42P22">>) -> indeterminate_collation;
error_code_to_name(<<"42809">>) -> wrong_object_type;
error_code_to_name(<<"42703">>) -> undefined_column;
error_code_to_name(<<"42883">>) -> undefined_function;
error_code_to_name(<<"42P01">>) -> undefined_table;
error_code_to_name(<<"42P02">>) -> undefined_parameter;
error_code_to_name(<<"42704">>) -> undefined_object;
error_code_to_name(<<"42701">>) -> duplicate_column;
error_code_to_name(<<"42P03">>) -> duplicate_cursor;
error_code_to_name(<<"42P04">>) -> duplicate_database;
error_code_to_name(<<"42723">>) -> duplicate_function;
error_code_to_name(<<"42P05">>) -> duplicate_prepared_statement;
error_code_to_name(<<"42P06">>) -> duplicate_schema;
error_code_to_name(<<"42P07">>) -> duplicate_table;
error_code_to_name(<<"42712">>) -> duplicate_alias;
error_code_to_name(<<"42710">>) -> duplicate_object;
error_code_to_name(<<"42702">>) -> ambiguous_column;
error_code_to_name(<<"42725">>) -> ambiguous_function;
error_code_to_name(<<"42P08">>) -> ambiguous_parameter;
error_code_to_name(<<"42P09">>) -> ambiguous_alias;
error_code_to_name(<<"42P10">>) -> invalid_column_reference;
error_code_to_name(<<"42611">>) -> invalid_column_definition;
error_code_to_name(<<"42P11">>) -> invalid_cursor_definition;
error_code_to_name(<<"42P12">>) -> invalid_database_definition;
error_code_to_name(<<"42P13">>) -> invalid_function_definition;
error_code_to_name(<<"42P14">>) -> invalid_prepared_statement_definition;
error_code_to_name(<<"42P15">>) -> invalid_schema_definition;
error_code_to_name(<<"42P16">>) -> invalid_table_definition;
error_code_to_name(<<"42P17">>) -> invalid_object_definition;
error_code_to_name(<<"44000">>) -> with_check_option_violation;
error_code_to_name(<<"53000">>) -> insufficient_resources;
error_code_to_name(<<"53100">>) -> disk_full;
error_code_to_name(<<"53200">>) -> out_of_memory;
error_code_to_name(<<"53300">>) -> too_many_connections;
error_code_to_name(<<"53400">>) -> configuration_limit_exceeded;
error_code_to_name(<<"54000">>) -> program_limit_exceeded;
error_code_to_name(<<"54001">>) -> statement_too_complex;
error_code_to_name(<<"54011">>) -> too_many_columns;
error_code_to_name(<<"54023">>) -> too_many_arguments;
error_code_to_name(<<"55000">>) -> object_not_in_prerequisite_state;
error_code_to_name(<<"55006">>) -> object_in_use;
error_code_to_name(<<"55P02">>) -> cant_change_runtime_param;
error_code_to_name(<<"55P03">>) -> lock_not_available;
error_code_to_name(<<"57000">>) -> operator_intervention;
error_code_to_name(<<"57014">>) -> query_canceled;
error_code_to_name(<<"57P01">>) -> admin_shutdown;
error_code_to_name(<<"57P02">>) -> crash_shutdown;
error_code_to_name(<<"57P03">>) -> cannot_connect_now;
error_code_to_name(<<"57P04">>) -> database_dropped;
error_code_to_name(<<"58000">>) -> system_error;
error_code_to_name(<<"58030">>) -> io_error;
error_code_to_name(<<"58P01">>) -> undefined_file;
error_code_to_name(<<"58P02">>) -> duplicate_file;
error_code_to_name(<<"F0000">>) -> config_file_error;
error_code_to_name(<<"F0001">>) -> lock_file_exists;
error_code_to_name(<<"HV000">>) -> fdw_error;
error_code_to_name(<<"HV005">>) -> fdw_column_name_not_found;
error_code_to_name(<<"HV002">>) -> fdw_dynamic_parameter_value_needed;
error_code_to_name(<<"HV010">>) -> fdw_function_sequence_error;
error_code_to_name(<<"HV021">>) -> fdw_inconsistent_descriptor_information;
error_code_to_name(<<"HV024">>) -> fdw_invalid_attribute_value;
error_code_to_name(<<"HV007">>) -> fdw_invalid_column_name;
error_code_to_name(<<"HV008">>) -> fdw_invalid_column_number;
error_code_to_name(<<"HV004">>) -> fdw_invalid_data_type;
error_code_to_name(<<"HV006">>) -> fdw_invalid_data_type_descriptors;
error_code_to_name(<<"HV091">>) -> fdw_invalid_descriptor_field_identifier;
error_code_to_name(<<"HV00B">>) -> fdw_invalid_handle;
error_code_to_name(<<"HV00C">>) -> fdw_invalid_option_index;
error_code_to_name(<<"HV00D">>) -> fdw_invalid_option_name;
error_code_to_name(<<"HV090">>) -> fdw_invalid_string_length_or_buffer_length;
error_code_to_name(<<"HV00A">>) -> fdw_invalid_string_format;
error_code_to_name(<<"HV009">>) -> fdw_invalid_use_of_null_pointer;
error_code_to_name(<<"HV014">>) -> fdw_too_many_handles;
error_code_to_name(<<"HV001">>) -> fdw_out_of_memory;
error_code_to_name(<<"HV00P">>) -> fdw_no_schemas;
error_code_to_name(<<"HV00J">>) -> fdw_option_name_not_found;
error_code_to_name(<<"HV00K">>) -> fdw_reply_handle;
error_code_to_name(<<"HV00Q">>) -> fdw_schema_not_found;
error_code_to_name(<<"HV00R">>) -> fdw_table_not_found;
error_code_to_name(<<"HV00L">>) -> fdw_unable_to_create_execution;
error_code_to_name(<<"HV00M">>) -> fdw_unable_to_create_reply;
error_code_to_name(<<"HV00N">>) -> fdw_unable_to_establish_connection;
error_code_to_name(<<"P0000">>) -> plpgsql_error;
error_code_to_name(<<"P0001">>) -> raise_exception;
error_code_to_name(<<"P0002">>) -> no_data_found;
error_code_to_name(<<"P0003">>) -> too_many_rows;
error_code_to_name(<<"P0004">>) -> assert_failure;
error_code_to_name(<<"XX000">>) -> internal_error;
error_code_to_name(<<"XX001">>) -> data_corrupted;
error_code_to_name(<<"XX002">>) -> index_corrupted;
error_code_to_name(_Other) -> unknown.
