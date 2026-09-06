-module(pgc_connection_statem_simple_query).
-moduledoc false.

-export([
    enter/3
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
-import_record(pgc_protocol_message, [query, row_description, data_row, empty_query_response, command_complete, error_response, ready_for_query]).

% -----------------------------------------------------------------------------
% Types
% -----------------------------------------------------------------------------

% States ----------------------------------------------------------------------

-record #s_simple_query {
    ref :: term(),
    row_description :: [pgc_protocol_message:row_description_field()]
}.

% ------------------------------------------------------------------------------
% API
% ------------------------------------------------------------------------------

-spec enter(Ref, QueryText, ConnectionData) -> gen_statem:event_handler_result(#s_simple_query{}, ConnectionData) when
    Ref :: term(),
    QueryText :: unicode:chardata(),
    ConnectionData :: #data{}.
enter(Ref, QueryText, ConnectionData) ->
    {ok, NextState, NextData, Actions} = init({
        Ref,
        QueryText,
        ConnectionData
    }),
    {next_state, NextState, NextData, [
        {change_callback_module, ?MODULE} | Actions
    ]}.

% ------------------------------------------------------------------------------
% gen_statem callbacks
% ------------------------------------------------------------------------------

-spec init({Ref, QueryText, ConnectionData}) -> {ok, #s_simple_query{}, ConnectionData, [gen_statem:action()]} when
    Ref :: term(),
    QueryText :: unicode:chardata(),
    ConnectionData :: #data{}.
init({Ref, QueryText, ConnectionData}) ->
    {ok, #s_simple_query{
        ref = Ref,
        row_description = []
    }, ConnectionData, [
        {next_event, internal, #send{
            messages = [
                #query{text = QueryText}
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
% State: simple_query
% -------------------------------------------------------------------------------

handle_event(internal, #row_description{fields = Fields}, #s_simple_query{} = State, ConnectionData) ->
    {next_state, State#s_simple_query{row_description = Fields}, ConnectionData, [
    ]};

handle_event(internal, #data_row{values = Values}, #s_simple_query{} = State, _ConnectionData) ->
    #s_simple_query{
        ref = Ref,
        row_description = RowDescription
    } = State,
    {keep_state_and_data, [
        {next_event, internal, #callback{name = handle_row_data, args = [Ref, RowDescription, Values]}}
    ]};

handle_event(internal, #command_complete{tag = Tag}, #s_simple_query{ref = Ref}, _ConnectionData) ->
    {keep_state_and_data, [
        {next_event, internal, #callback{name = handle_query_result, args = [Ref, {ok, Tag}]}}
    ]};

handle_event(internal, #empty_query_response{}, #s_simple_query{ref = Ref}, _ConnectionData) ->
    {keep_state_and_data, [
        {next_event, internal, #callback{name = handle_query_result, args = [Ref, empty]}}
    ]};

handle_event(internal, #error_response{fields = Fields}, #s_simple_query{ref = Ref}, _ConnectionData) ->
    {keep_state_and_data, [
        {next_event, internal, #callback{name = handle_query_result, args = [Ref, {error, Fields}]}}
    ]};

handle_event(internal, #ready_for_query{status = Status}, #s_simple_query{}, ConnectionData) ->
    pgc_connection_statem:ready(Status, ConnectionData);

% -------------------------------------------------------------------------------
% state: *
% -------------------------------------------------------------------------------

handle_event(Type, Content, State, ConnectionData) ->
    pgc_connection_statem_common:handle_event(Type, Content, State, ConnectionData).
