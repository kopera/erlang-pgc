-module(pgc_error).
-export([
    protocol_violation/1,
    feature_not_supported/1,
    invalid_parameter_value/3,
    authentication_failure/1,
    invalid_sql_statement_name/1,
    disconnected/0,
    disconnected/1
]).
-export([
    from_message/1
]).
-export_type([
    t/0,
    class/0,
    name/0,
    info/0
]).

-include("./protocol/pgc_message.hrl").
-include("./types/pgc_type.hrl").

-type t() :: info().
-type info() :: #{
    severity := panic | fatal | error,
    code := binary(),
    name := name(),
    class := class(),
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

%% @private
-spec protocol_violation(unicode:chardata() | {io:format(), [term()]}) -> t().
protocol_violation(Message) ->
    new(<<"08P01">>, Message).

%% @private
-spec feature_not_supported(unicode:chardata() | {io:format(), [term()]}) -> t().
feature_not_supported(Message) ->
    new(<<"0A000">>, Message).

%% @private
-spec invalid_parameter_value(pos_integer(), term(), pgc_type:t()) -> t().
invalid_parameter_value(Index, Value, #pgc_type{name = TypeName}) ->
    new(<<"22023">>, {"Invalid parameter of type '~s' value '~p' at index ~b", [TypeName, Value, Index]}).

%% @private
-spec invalid_sql_statement_name(unicode:chardata()) -> t().
invalid_sql_statement_name(Name) ->
    new(<<"26000">>, {"Reserved SQL statememt name: ~s", [Name]}).

%% @private
-spec authentication_failure(unicode:chardata() | {io:format(), [term()]}) -> t().
authentication_failure(Message) ->
    new(<<"28P01">>, Message).

%% @private
-spec disconnected() -> t().
disconnected() ->
    new(<<"28P01">>, <<"Server connection closed">>).

%% @private
-spec disconnected(unicode:chardata() | {io:format(), [term()]}) -> t().
disconnected(Message) ->
    new(<<"28P01">>, Message).


%% @private
-spec new(binary(), unicode:chardata() | {io:format(), [term()]}) -> t().
new(Code, {Format, Params} = _Message) ->
    new(Code, io_lib:format(Format, Params));

new(Code, Message) ->
    #{
        severity => fatal,
        class => class_from_code(Code),
        code => Code,
        name => name_from_code(Code),
        message => characters_to_binary(Message)
    }.


%% @private
characters_to_binary(Input) ->
    case unicode:characters_to_binary(Input) of
        {error, _, _} ->
            erlang:error(badarg, [Input]);
        {incomplete, _, _} ->
            erlang:error(badarg, [Input]);
        UnicodeBinary ->
            UnicodeBinary
    end.


-spec from_message(#msg_error_response{}) -> t().
from_message(#msg_error_response{fields = Fields}) ->
    #{code := Code} = Fields,
    Fields#{
        name => name_from_code(Code),
        class => class_from_code(Code)
    }.


%% @private
-spec class_from_code(Code :: binary()) -> class().
-type class() ::
      sql_statement_not_yet_complete
    | connection_exception
    | triggered_action_exception
    | feature_not_supported
    | invalid_transaction_initiation
    | locator_exception
    | invalid_grantor
    | invalid_role_specification
    | diagnostics_exception
    | data_exception
    | integrity_constraint_violation
    | invalid_cursor_state
    | invalid_transaction_state
    | invalid_sql_statement_name
    | triggered_data_change_violation
    | invalid_authorization_specification
    | dependent_privilege_descriptors_still_exist
    | invalid_transaction_termination
    | sql_routine_exception
    | invalid_cursor_name
    | external_routine_exception
    | external_routine_invocation_exception
    | savepoint_exception
    | invalid_catalog_name
    | invalid_schema_name
    | transaction_rollback
    | syntax_error_or_access_rule_violation
    | with_check_option_violation
    | insufficient_resources
    | program_limit_exceeded
    | object_not_in_prerequisite_state
    | operator_intervention
    | system_error
    | config_file_error
    | fdw_error
    | plpgsql_error
    | internal_error
    | other_error.
class_from_code(<<"03", _:3/binary>>) -> sql_statement_not_yet_complete;
class_from_code(<<"08", _:3/binary>>) -> connection_exception;
class_from_code(<<"09", _:3/binary>>) -> triggered_action_exception;
class_from_code(<<"0A", _:3/binary>>) -> feature_not_supported;
class_from_code(<<"0B", _:3/binary>>) -> invalid_transaction_initiation;
class_from_code(<<"0F", _:3/binary>>) -> locator_exception;
class_from_code(<<"0L", _:3/binary>>) -> invalid_grantor;
class_from_code(<<"0P", _:3/binary>>) -> invalid_role_specification;
class_from_code(<<"0Z", _:3/binary>>) -> diagnostics_exception;
class_from_code(<<"22", _:3/binary>>) -> data_exception;
class_from_code(<<"23", _:3/binary>>) -> integrity_constraint_violation;
class_from_code(<<"24", _:3/binary>>) -> invalid_cursor_state;
class_from_code(<<"25", _:3/binary>>) -> invalid_transaction_state;
class_from_code(<<"26", _:3/binary>>) -> invalid_sql_statement_name;
class_from_code(<<"27", _:3/binary>>) -> triggered_data_change_violation;
class_from_code(<<"28", _:3/binary>>) -> invalid_authorization_specification;
class_from_code(<<"2B", _:3/binary>>) -> dependent_privilege_descriptors_still_exist;
class_from_code(<<"2D", _:3/binary>>) -> invalid_transaction_termination;
class_from_code(<<"2F", _:3/binary>>) -> sql_routine_exception;
class_from_code(<<"34", _:3/binary>>) -> invalid_cursor_name;
class_from_code(<<"38", _:3/binary>>) -> external_routine_exception;
class_from_code(<<"39", _:3/binary>>) -> external_routine_invocation_exception;
class_from_code(<<"3B", _:3/binary>>) -> savepoint_exception;
class_from_code(<<"3D", _:3/binary>>) -> invalid_catalog_name;
class_from_code(<<"3F", _:3/binary>>) -> invalid_schema_name;
class_from_code(<<"40", _:3/binary>>) -> transaction_rollback;
class_from_code(<<"42", _:3/binary>>) -> syntax_error_or_access_rule_violation;
class_from_code(<<"44", _:3/binary>>) -> with_check_option_violation;
class_from_code(<<"53", _:3/binary>>) -> insufficient_resources;
class_from_code(<<"54", _:3/binary>>) -> program_limit_exceeded;
class_from_code(<<"55", _:3/binary>>) -> object_not_in_prerequisite_state;
class_from_code(<<"57", _:3/binary>>) -> operator_intervention;
class_from_code(<<"58", _:3/binary>>) -> system_error;
class_from_code(<<"F0", _:3/binary>>) -> config_file_error;
class_from_code(<<"HV", _:3/binary>>) -> fdw_error;
class_from_code(<<"P0", _:3/binary>>) -> plpgsql_error;
class_from_code(<<"XX", _:3/binary>>) -> internal_error;
class_from_code(<<_:5/binary>>) -> other_error.


%% @private
-spec name_from_code(Code :: binary()) -> name().
-type name() :: atom().
name_from_code(<<"03000">>) -> sql_statement_not_yet_complete;

name_from_code(<<"08000">>) -> connection_exception;
name_from_code(<<"08003">>) -> connection_does_not_exist;
name_from_code(<<"08006">>) -> connection_failure;
name_from_code(<<"08001">>) -> sqlclient_unable_to_establish_sqlconnection;
name_from_code(<<"08004">>) -> sqlserver_rejected_establishment_of_sqlconnection;
name_from_code(<<"08007">>) -> transaction_resolution_unknown;
name_from_code(<<"08P01">>) -> protocol_violation;

name_from_code(<<"09000">>) -> triggered_action_exception;

name_from_code(<<"0A000">>) -> feature_not_supported;

name_from_code(<<"0B000">>) -> invalid_transaction_initiation;

name_from_code(<<"0F000">>) -> locator_exception;
name_from_code(<<"0F001">>) -> invalid_locator_specification;

name_from_code(<<"0L000">>) -> invalid_grantor;
name_from_code(<<"0LP01">>) -> invalid_grant_operation;

name_from_code(<<"0P000">>) -> invalid_role_specification;

name_from_code(<<"0Z000">>) -> diagnostics_exception;
name_from_code(<<"0Z002">>) -> stacked_diagnostics_accessed_without_active_handler;

name_from_code(<<"22000">>) -> data_exception;
name_from_code(<<"2202E">>) -> array_subscript_error;
name_from_code(<<"22021">>) -> character_not_in_repertoire;
name_from_code(<<"22008">>) -> datetime_field_overflow;
name_from_code(<<"22012">>) -> division_by_zero;
name_from_code(<<"22005">>) -> error_in_assignment;
name_from_code(<<"2200B">>) -> escape_character_conflict;
name_from_code(<<"22022">>) -> indicator_overflow;
name_from_code(<<"22015">>) -> interval_field_overflow;
name_from_code(<<"2201E">>) -> invalid_argument_for_logarithm;
name_from_code(<<"22014">>) -> invalid_argument_for_ntile_function;
name_from_code(<<"22016">>) -> invalid_argument_for_nth_value_function;
name_from_code(<<"2201F">>) -> invalid_argument_for_power_function;
name_from_code(<<"2201G">>) -> invalid_argument_for_width_bucket_function;
name_from_code(<<"22018">>) -> invalid_character_value_for_cast;
name_from_code(<<"22007">>) -> invalid_datetime_format;
name_from_code(<<"22019">>) -> invalid_escape_character;
name_from_code(<<"2200D">>) -> invalid_escape_octet;
name_from_code(<<"22025">>) -> invalid_escape_sequence;
name_from_code(<<"22P06">>) -> nonstandard_use_of_escape_character;
name_from_code(<<"22010">>) -> invalid_indicator_parameter_value;
name_from_code(<<"22023">>) -> invalid_parameter_value;
name_from_code(<<"22013">>) -> invalid_preceding_or_following_size;
name_from_code(<<"2201B">>) -> invalid_regular_expression;
name_from_code(<<"2201W">>) -> invalid_row_count_in_limit_clause;
name_from_code(<<"2201X">>) -> invalid_row_count_in_result_offset_clause;
name_from_code(<<"2202H">>) -> invalid_tablesample_argument;
name_from_code(<<"2202G">>) -> invalid_tablesample_repeat;
name_from_code(<<"22009">>) -> invalid_time_zone_displacement_value;
name_from_code(<<"2200C">>) -> invalid_use_of_escape_character;
name_from_code(<<"2200G">>) -> most_specific_type_mismatch;
name_from_code(<<"22004">>) -> null_value_not_allowed;
name_from_code(<<"22002">>) -> null_value_no_indicator_parameter;
name_from_code(<<"22003">>) -> numeric_value_out_of_range;
name_from_code(<<"2200H">>) -> sequence_generator_limit_exceeded;
name_from_code(<<"22026">>) -> string_data_length_mismatch;
name_from_code(<<"22001">>) -> string_data_right_truncation;
name_from_code(<<"22011">>) -> substring_error;
name_from_code(<<"22027">>) -> trim_error;
name_from_code(<<"22024">>) -> unterminated_c_string;
name_from_code(<<"2200F">>) -> zero_length_character_string;
name_from_code(<<"22P01">>) -> floating_point_exception;
name_from_code(<<"22P02">>) -> invalid_text_representation;
name_from_code(<<"22P03">>) -> invalid_binary_representation;
name_from_code(<<"22P04">>) -> bad_copy_file_format;
name_from_code(<<"22P05">>) -> untranslatable_character;
name_from_code(<<"2200L">>) -> not_an_xml_document;
name_from_code(<<"2200M">>) -> invalid_xml_document;
name_from_code(<<"2200N">>) -> invalid_xml_content;
name_from_code(<<"2200S">>) -> invalid_xml_comment;
name_from_code(<<"2200T">>) -> invalid_xml_processing_instruction;
name_from_code(<<"22030">>) -> duplicate_json_object_key_value;
name_from_code(<<"22031">>) -> invalid_argument_for_sql_json_datetime_function;
name_from_code(<<"22032">>) -> invalid_json_text;
name_from_code(<<"22033">>) -> invalid_sql_json_subscript;
name_from_code(<<"22034">>) -> more_than_one_sql_json_item;
name_from_code(<<"22035">>) -> no_sql_json_item;
name_from_code(<<"22036">>) -> non_numeric_sql_json_item;
name_from_code(<<"22037">>) -> non_unique_keys_in_a_json_object;
name_from_code(<<"22038">>) -> singleton_sql_json_item_required;
name_from_code(<<"22039">>) -> sql_json_array_not_found;
name_from_code(<<"2203A">>) -> sql_json_member_not_found;
name_from_code(<<"2203B">>) -> sql_json_number_not_found;
name_from_code(<<"2203C">>) -> sql_json_object_not_found;
name_from_code(<<"2203D">>) -> too_many_json_array_elements;
name_from_code(<<"2203E">>) -> too_many_json_object_members;
name_from_code(<<"2203F">>) -> sql_json_scalar_required;
name_from_code(<<"2203G">>) -> sql_json_item_cannot_be_cast_to_target_type;

name_from_code(<<"23000">>) -> integrity_constraint_violation;
name_from_code(<<"23001">>) -> restrict_violation;
name_from_code(<<"23502">>) -> not_null_violation;
name_from_code(<<"23503">>) -> foreign_key_violation;
name_from_code(<<"23505">>) -> unique_violation;
name_from_code(<<"23514">>) -> check_violation;
name_from_code(<<"23P01">>) -> exclusion_violation;

name_from_code(<<"24000">>) -> invalid_cursor_state;

name_from_code(<<"25000">>) -> invalid_transaction_state;
name_from_code(<<"25001">>) -> active_sql_transaction;
name_from_code(<<"25002">>) -> branch_transaction_already_active;
name_from_code(<<"25008">>) -> held_cursor_requires_same_isolation_level;
name_from_code(<<"25003">>) -> inappropriate_access_mode_for_branch_transaction;
name_from_code(<<"25004">>) -> inappropriate_isolation_level_for_branch_transaction;
name_from_code(<<"25005">>) -> no_active_sql_transaction_for_branch_transaction;
name_from_code(<<"25006">>) -> read_only_sql_transaction;
name_from_code(<<"25007">>) -> schema_and_data_statement_mixing_not_supported;
name_from_code(<<"25P01">>) -> no_active_sql_transaction;
name_from_code(<<"25P02">>) -> in_failed_sql_transaction;
name_from_code(<<"25P03">>) -> idle_in_transaction_session_timeout;

name_from_code(<<"26000">>) -> invalid_sql_statement_name;

name_from_code(<<"27000">>) -> triggered_data_change_violation;

name_from_code(<<"28000">>) -> invalid_authorization_specification;
name_from_code(<<"28P01">>) -> invalid_password;

name_from_code(<<"2B000">>) -> dependent_privilege_descriptors_still_exist;
name_from_code(<<"2BP01">>) -> dependent_objects_still_exist;

name_from_code(<<"2D000">>) -> invalid_transaction_termination;

name_from_code(<<"2F000">>) -> sql_routine_exception;
name_from_code(<<"2F005">>) -> function_executed_no_return_statement;
name_from_code(<<"2F002">>) -> modifying_sql_data_not_permitted;
name_from_code(<<"2F003">>) -> prohibited_sql_statement_attempted;
name_from_code(<<"2F004">>) -> reading_sql_data_not_permitted;

name_from_code(<<"34000">>) -> invalid_cursor_name;

name_from_code(<<"38000">>) -> external_routine_exception;
name_from_code(<<"38001">>) -> containing_sql_not_permitted;
name_from_code(<<"38002">>) -> modifying_sql_data_not_permitted;
name_from_code(<<"38003">>) -> prohibited_sql_statement_attempted;
name_from_code(<<"38004">>) -> reading_sql_data_not_permitted;

name_from_code(<<"39000">>) -> external_routine_invocation_exception;
name_from_code(<<"39001">>) -> invalid_sqlstate_returned;
name_from_code(<<"39004">>) -> null_value_not_allowed;
name_from_code(<<"39P01">>) -> trigger_protocol_violated;
name_from_code(<<"39P02">>) -> srf_protocol_violated;
name_from_code(<<"39P03">>) -> event_trigger_protocol_violated;

name_from_code(<<"3B000">>) -> savepoint_exception;
name_from_code(<<"3B001">>) -> invalid_savepoint_specification;

name_from_code(<<"3D000">>) -> invalid_catalog_name;

name_from_code(<<"3F000">>) -> invalid_schema_name;

name_from_code(<<"40000">>) -> transaction_rollback;
name_from_code(<<"40002">>) -> transaction_integrity_constraint_violation;
name_from_code(<<"40001">>) -> serialization_failure;
name_from_code(<<"40003">>) -> statement_completion_unknown;
name_from_code(<<"40P01">>) -> deadlock_detected;

name_from_code(<<"42000">>) -> syntax_error_or_access_rule_violation;
name_from_code(<<"42601">>) -> syntax_error;
name_from_code(<<"42501">>) -> insufficient_privilege;
name_from_code(<<"42846">>) -> cannot_coerce;
name_from_code(<<"42803">>) -> grouping_error;
name_from_code(<<"42P20">>) -> windowing_error;
name_from_code(<<"42P19">>) -> invalid_recursion;
name_from_code(<<"42830">>) -> invalid_foreign_key;
name_from_code(<<"42602">>) -> invalid_name;
name_from_code(<<"42622">>) -> name_too_long;
name_from_code(<<"42939">>) -> reserved_name;
name_from_code(<<"42804">>) -> datatype_mismatch;
name_from_code(<<"42P18">>) -> indeterminate_datatype;
name_from_code(<<"42P21">>) -> collation_mismatch;
name_from_code(<<"42P22">>) -> indeterminate_collation;
name_from_code(<<"42809">>) -> wrong_object_type;
name_from_code(<<"42703">>) -> undefined_column;
name_from_code(<<"42883">>) -> undefined_function;
name_from_code(<<"42P01">>) -> undefined_table;
name_from_code(<<"42P02">>) -> undefined_parameter;
name_from_code(<<"42704">>) -> undefined_object;
name_from_code(<<"42701">>) -> duplicate_column;
name_from_code(<<"42P03">>) -> duplicate_cursor;
name_from_code(<<"42P04">>) -> duplicate_database;
name_from_code(<<"42723">>) -> duplicate_function;
name_from_code(<<"42P05">>) -> duplicate_prepared_statement;
name_from_code(<<"42P06">>) -> duplicate_schema;
name_from_code(<<"42P07">>) -> duplicate_table;
name_from_code(<<"42712">>) -> duplicate_alias;
name_from_code(<<"42710">>) -> duplicate_object;
name_from_code(<<"42702">>) -> ambiguous_column;
name_from_code(<<"42725">>) -> ambiguous_function;
name_from_code(<<"42P08">>) -> ambiguous_parameter;
name_from_code(<<"42P09">>) -> ambiguous_alias;
name_from_code(<<"42P10">>) -> invalid_column_reference;
name_from_code(<<"42611">>) -> invalid_column_definition;
name_from_code(<<"42P11">>) -> invalid_cursor_definition;
name_from_code(<<"42P12">>) -> invalid_database_definition;
name_from_code(<<"42P13">>) -> invalid_function_definition;
name_from_code(<<"42P14">>) -> invalid_prepared_statement_definition;
name_from_code(<<"42P15">>) -> invalid_schema_definition;
name_from_code(<<"42P16">>) -> invalid_table_definition;
name_from_code(<<"42P17">>) -> invalid_object_definition;

name_from_code(<<"44000">>) -> with_check_option_violation;

name_from_code(<<"53000">>) -> insufficient_resources;
name_from_code(<<"53100">>) -> disk_full;
name_from_code(<<"53200">>) -> out_of_memory;
name_from_code(<<"53300">>) -> too_many_connections;
name_from_code(<<"53400">>) -> configuration_limit_exceeded;

name_from_code(<<"54000">>) -> program_limit_exceeded;
name_from_code(<<"54001">>) -> statement_too_complex;
name_from_code(<<"54011">>) -> too_many_columns;
name_from_code(<<"54023">>) -> too_many_arguments;

name_from_code(<<"55000">>) -> object_not_in_prerequisite_state;
name_from_code(<<"55006">>) -> object_in_use;
name_from_code(<<"55P02">>) -> cant_change_runtime_param;
name_from_code(<<"55P03">>) -> lock_not_available;

name_from_code(<<"57000">>) -> operator_intervention;
name_from_code(<<"57014">>) -> query_canceled;
name_from_code(<<"57P01">>) -> admin_shutdown;
name_from_code(<<"57P02">>) -> crash_shutdown;
name_from_code(<<"57P03">>) -> cannot_connect_now;
name_from_code(<<"57P04">>) -> database_dropped;

name_from_code(<<"58000">>) -> system_error;
name_from_code(<<"58030">>) -> io_error;
name_from_code(<<"58P01">>) -> undefined_file;
name_from_code(<<"58P02">>) -> duplicate_file;

name_from_code(<<"F0000">>) -> config_file_error;
name_from_code(<<"F0001">>) -> lock_file_exists;

name_from_code(<<"HV000">>) -> fdw_error;
name_from_code(<<"HV005">>) -> fdw_column_name_not_found;
name_from_code(<<"HV002">>) -> fdw_dynamic_parameter_value_needed;
name_from_code(<<"HV010">>) -> fdw_function_sequence_error;
name_from_code(<<"HV021">>) -> fdw_inconsistent_descriptor_information;
name_from_code(<<"HV024">>) -> fdw_invalid_attribute_value;
name_from_code(<<"HV007">>) -> fdw_invalid_column_name;
name_from_code(<<"HV008">>) -> fdw_invalid_column_number;
name_from_code(<<"HV004">>) -> fdw_invalid_data_type;
name_from_code(<<"HV006">>) -> fdw_invalid_data_type_descriptors;
name_from_code(<<"HV091">>) -> fdw_invalid_descriptor_field_identifier;
name_from_code(<<"HV00B">>) -> fdw_invalid_handle;
name_from_code(<<"HV00C">>) -> fdw_invalid_option_index;
name_from_code(<<"HV00D">>) -> fdw_invalid_option_name;
name_from_code(<<"HV090">>) -> fdw_invalid_string_length_or_buffer_length;
name_from_code(<<"HV00A">>) -> fdw_invalid_string_format;
name_from_code(<<"HV009">>) -> fdw_invalid_use_of_null_pointer;
name_from_code(<<"HV014">>) -> fdw_too_many_handles;
name_from_code(<<"HV001">>) -> fdw_out_of_memory;
name_from_code(<<"HV00P">>) -> fdw_no_schemas;
name_from_code(<<"HV00J">>) -> fdw_option_name_not_found;
name_from_code(<<"HV00K">>) -> fdw_reply_handle;
name_from_code(<<"HV00Q">>) -> fdw_schema_not_found;
name_from_code(<<"HV00R">>) -> fdw_table_not_found;
name_from_code(<<"HV00L">>) -> fdw_unable_to_create_execution;
name_from_code(<<"HV00M">>) -> fdw_unable_to_create_reply;
name_from_code(<<"HV00N">>) -> fdw_unable_to_establish_connection;

name_from_code(<<"P0000">>) -> plpgsql_error;
name_from_code(<<"P0001">>) -> raise_exception;
name_from_code(<<"P0002">>) -> no_data_found;
name_from_code(<<"P0003">>) -> too_many_rows;
name_from_code(<<"P0004">>) -> assert_failure;

name_from_code(<<"XX000">>) -> internal_error;
name_from_code(<<"XX001">>) -> data_corrupted;
name_from_code(<<"XX002">>) -> index_corrupted;
name_from_code(<<_:5/binary>>) -> other_error.

