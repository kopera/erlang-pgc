-module(pgc_connection_statem_extended_query).
-moduledoc false.

-export([
    prepare/4,
    unprepare/3,
    execute/5
]).
-export_type([
    execute_parameters/0,
    execute_options/0
]).

-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3,
    format_status/1
]).

-import_record(pgc_connection_statem, [data, send, callback]).
-import_record(pgc_protocol_message, [
    bind_complete,
    bind,
    close_complete,
    close,
    command_complete,
    data_row,
    describe,
    empty_query_response,
    error_response,
    execute,
    no_data,
    parameter_description,
    parse_complete,
    parse,
    ready_for_query,
    row_description,
    sync
]).

% -----------------------------------------------------------------------------
% Types
% -----------------------------------------------------------------------------

-type statement_name() :: unicode:chardata().
-type statement_text() :: unicode:chardata().
-type execute_parameters() :: [{binary | text, iodata() | null}].
-type execute_options() :: #{
    result_format => text | binary | nonempty_list(text | binary)
}.

% States ----------------------------------------------------------------------

-record #s_preparing {
    ref :: term(),
    name :: statement_name(),
    parameters_description :: [pgc_protocol:oid()],
    row_description :: [pgc_protocol_message:row_description_field()]
}.

-record #s_unpreparing {
    ref :: term(),
    name :: statement_name()
}.

-record #s_executing {
    ref :: term(),
    name :: statement_name(),
    row_description :: [pgc_protocol_message:row_description_field()]
}.

% ------------------------------------------------------------------------------
% API
% ------------------------------------------------------------------------------

-spec prepare(Ref, StatementName, StatementText, ConnectionData) -> gen_statem:event_handler_result(#s_preparing{}, ConnectionData) when
    Ref :: term(),
    StatementName :: statement_name(),
    StatementText :: statement_text(),
    ConnectionData :: #data{}.
prepare(Ref, StatementName, StatementText, ConnectionData) ->
    {ok, NextState, NextData, Actions} = init({
        prepare,
        Ref,
        StatementName,
        StatementText,
        ConnectionData
    }),
    {next_state, NextState, NextData, [
        {change_callback_module, ?MODULE} | Actions
    ]}.

-spec unprepare(Ref, StatementName, ConnectionData) -> gen_statem:event_handler_result(#s_unpreparing{}, ConnectionData) when
    Ref :: term(),
    StatementName :: statement_name(),
    ConnectionData :: #data{}.
unprepare(Ref, StatementName, ConnectionData) ->
    {ok, NextState, NextData, Actions} = init({
        unprepare,
        Ref,
        StatementName,
        ConnectionData
    }),
    {next_state, NextState, NextData, [
        {change_callback_module, ?MODULE} | Actions
    ]}.

-spec execute(Ref, StatementName, Parameters, Options, ConnectionData) -> gen_statem:event_handler_result(#s_executing{}, ConnectionData) when
    Ref :: term(),
    StatementName :: statement_name(),
    Parameters :: execute_parameters(),
    Options :: execute_options(),
    ConnectionData :: #data{}.
execute(Ref, StatementName, Parameters, Options, ConnectionData) ->
    {ok, NextState, NextData, Actions} = init({
        execute,
        Ref,
        StatementName,
        Parameters,
        Options,
        ConnectionData
    }),
    {next_state, NextState, NextData, [
        {change_callback_module, ?MODULE} | Actions
    ]}.


% ------------------------------------------------------------------------------
% gen_statem callbacks
% ------------------------------------------------------------------------------

-spec init
    ({prepare, term(), statement_name(), statement_text(), #data{}}) -> {ok, #s_preparing{}, #data{}, [gen_statem:action()]};
    ({unprepare, term(), statement_name(), #data{}}) -> {ok, #s_unpreparing{}, #data{}, [gen_statem:action()]};
    ({execute, term(), statement_name(), execute_parameters(), execute_options(), #data{}}) -> {ok, #s_executing{}, #data{}, [gen_statem:action()]}.
init({prepare, Ref, StatementName, StatementText, ConnectionData}) ->
    {ok, #s_preparing{
        ref = Ref,
        name = StatementName,
        parameters_description = [],
        row_description = []
    }, ConnectionData, [
        {next_event, internal, #send{
            messages = [
                #parse{name = StatementName, statement = StatementText, types = []},
                #describe{type = statement, name = StatementName},
                #sync{}
            ]
        }}
    ]};
init({unprepare, Ref, StatementName, ConnectionData}) ->
    {ok, #s_unpreparing{
        ref = Ref,
        name = StatementName
    }, ConnectionData, [
        {next_event, internal, #send{
            messages = [
                #close{type = statement, name = StatementName},
                #sync{}
            ]
        }}
    ]};
init({execute, Ref, StatementName, ExecuteParameters, ExecuteOptions, ConnectionData}) ->
    ResultFormat = case ExecuteOptions of
        #{result_format := binary} -> [binary];
        #{result_format := text} -> [text];
        #{result_format := Format} -> Format;
        #{} -> []
    end,

    {ok, #s_executing{
        ref = Ref,
        name = StatementName,
        row_description = []
    }, ConnectionData, [
        {next_event, internal, #send{
            messages = [
                #bind{
                    statement = StatementName,
                    portal = ~"",
                    parameters = ExecuteParameters,
                    results = ResultFormat
                },
                #describe{type = portal, name = ~""},
                #execute{portal = ~"", limit = 0},
                #sync{}
            ]
        }}
    ]}.

-doc false.
callback_mode() ->
    [handle_event_function].

-doc false.
terminate(Reason, State, Data) ->
    pgc_connection_statem_common:terminate(Reason, State, Data).

-doc false.
format_status(Status) ->
    pgc_connection_statem_common:format_status(Status).

% -------------------------------------------------------------------------------
% State: preparing
% -------------------------------------------------------------------------------

handle_event(internal, #parse_complete{}, #s_preparing{} = _State, _ConnectionData) ->
    keep_state_and_data;

handle_event(internal, #parameter_description{types = Types}, #s_preparing{} = State, ConnectionData) ->
    {next_state, State#s_preparing{parameters_description = Types}, ConnectionData};

handle_event(internal, #row_description{fields = Fields}, #s_preparing{} = State, ConnectionData) ->
    #s_preparing{
        ref = Ref,
        name = Name,
        parameters_description = Parameters
    } = State,
    StatementDescription = #{
        parameters_description => Parameters,
        row_description => Fields
    },
    {next_state, State#s_preparing{row_description = Fields}, ConnectionData, [
        {next_event, internal, #callback{
            name = handle_prepare_result, args = [Ref, {ok, Name, StatementDescription}]
        }}
    ]};

handle_event(internal, #no_data{}, #s_preparing{} = State, _ConnectionData) ->
    #s_preparing{
        ref = Ref,
        name = Name,
        parameters_description = Parameters
    } = State,
    StatementDescription = #{
        parameters_description => Parameters,
        row_description => []
    },
    {keep_state_and_data, [
        {next_event, internal, #callback{
            name = handle_prepare_result, args = [Ref, {ok, Name, StatementDescription}]
        }}
    ]};

handle_event(internal, #error_response{fields = Fields}, #s_preparing{ref = Ref}, _ConnectionData) ->
    {keep_state_and_data, [
        {next_event, internal, #callback{
            name = handle_prepare_result, args = [Ref, {error, Fields}]
        }}
    ]};

handle_event(internal, #ready_for_query{status = Status}, #s_preparing{} = _State, ConnectionData) ->
    pgc_connection_statem:ready(Status, ConnectionData);

% -------------------------------------------------------------------------------
% State: unpreparing
% -------------------------------------------------------------------------------

handle_event(internal, #close_complete{}, #s_unpreparing{} = State, _ConnectionData) ->
    #s_unpreparing{
        ref = Ref,
        name = Name
    } = State,
    {keep_state_and_data, [
        {next_event, internal, #callback{
            name = handle_unprepare_result, args = [Ref, {ok, Name}]
        }}
    ]};

handle_event(internal, #ready_for_query{status = Status}, #s_unpreparing{} = _State, ConnectionData) ->
    pgc_connection_statem:ready(Status, ConnectionData);

% -------------------------------------------------------------------------------
% State: executing
% -------------------------------------------------------------------------------

handle_event(internal, #bind_complete{}, #s_executing{} = _State, _ConnectionData) ->
    keep_state_and_data;

handle_event(internal, #row_description{fields = Fields}, #s_executing{} = State, ConnectionData) ->
    {next_state, State#s_executing{row_description = Fields}, ConnectionData};

handle_event(internal, #no_data{}, #s_executing{} = _State, _ConnectionData) ->
    keep_state_and_data;

handle_event(internal, #data_row{values = Values}, #s_executing{} = State, _ConnectionData) ->
    #s_executing{
        ref = Ref,
        row_description = RowDescription
    } = State,
    {keep_state_and_data, [
        {next_event, internal, #callback{name = handle_row_data, args = [Ref, RowDescription, Values]}}
    ]};

handle_event(internal, #command_complete{tag = Tag}, #s_executing{ref = Ref}, _ConnectionData) ->
    {keep_state_and_data, [
        {next_event, internal, #callback{name = handle_execute_result, args = [Ref, {ok, Tag}]}}
    ]};

handle_event(internal, #empty_query_response{}, #s_executing{ref = Ref}, _ConnectionData) ->
    {keep_state_and_data, [
        {next_event, internal, #callback{name = handle_execute_result, args = [Ref, empty]}}
    ]};

handle_event(internal, #error_response{fields = Fields}, #s_executing{ref = Ref}, _ConnectionData) ->
    {keep_state_and_data, [
        {next_event, internal, #callback{name = handle_execute_result, args = [Ref, {error, Fields}]}}
    ]};

handle_event(internal, #ready_for_query{status = Status}, #s_executing{} = _State, ConnectionData) ->
    pgc_connection_statem:ready(Status, ConnectionData);

% -------------------------------------------------------------------------------
% state: *
% -------------------------------------------------------------------------------

handle_event(Type, Content, State, ConnectionData) ->
    pgc_connection_statem_common:handle_event(Type, Content, State, ConnectionData).
