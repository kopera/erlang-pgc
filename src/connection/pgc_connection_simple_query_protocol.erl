-module(pgc_connection_simple_query_protocol).
-moduledoc """
The PostgreSQL simple query sub-protocol: send `Query`, deliver whatever comes back
through the handler (`RowDescription`+`DataRow` via `c:pgc_connection:handle_row_data/3`,
`CommandComplete`/`EmptyQueryResponse` via `c:pgc_connection:handle_result/2`,
`ErrorResponse` via `c:pgc_connection:handle_error/2`), and wait for `ReadyForQuery`.

`pgc_connection` `push_callback_module`s into this module from its `#ready{}` state --
via `pgc_connection:start_query/2` -- constructing this module's initial `t:#simple_query{}`
state itself. This module `pop_callback_module`s back to `pgc_connection`'s
`#pgc_connection:ready{}` state on `ReadyForQuery`.

Unlike the startup handshake, an `ErrorResponse` here is **not** fatal: real PostgreSQL
keeps the connection alive and still sends `ReadyForQuery` afterward, so this module
reports the error to the handler and keeps waiting, rather than
`pgc_connection:stop_with_error/2`.

`CopyInResponse`/`CopyOutResponse`/`CopyBothResponse` are deliberately unhandled -- they
are the hand-off point to a future `pgc_connection_copy_protocol`/
`pgc_connection_replication_protocol`, neither of which exist yet.
""".

-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3,
    format_status/1
]).

-export_record([
    simple_query
]).

-on_load(ensure_deps_loaded/0).


% ------------------------------------------------------------------------------
% State
% ------------------------------------------------------------------------------

-record #simple_query{
    % The most recent `RowDescription`'s fields -- `DataRow` carries only values, not
    % column info, so it has to be stashed here for `handle_row_data/3`. `undefined`
    % between statements in a multi-statement query, until the next `RowDescription`.
    row_description :: [pgc_protocol_message:row_description_field()] | undefined
}.

% ------------------------------------------------------------------------------
% gen_statem callbacks
% ------------------------------------------------------------------------------

-doc false.
init({Sql, Data}) ->
    {ok, #pgc_connection_simple_query_protocol:simple_query{
        row_description = undefined
    }, Data, [
        {next_event, internal, #pgc_connection:send{messages = [
            #pgc_protocol_message:query{text = Sql}
        ]}}
    ]}.

-doc false.
callback_mode() ->
    [handle_event_function, state_enter].

-doc false.
terminate(Reason, State, Data) ->
    pgc_connection:terminate(Reason, State, Data).

-doc false.
format_status(Status) ->
    pgc_connection:format_status(Status).


% -------------------------------------------------------------------------------
% State: simple_query
% -------------------------------------------------------------------------------

handle_event(enter, _, #simple_query{}, _Data) ->
    keep_state_and_data;

handle_event(internal, #pgc_protocol_message:row_description{fields = Fields}, #simple_query{} = State, Data) ->
    {next_state, State#simple_query{row_description = Fields}, Data};

handle_event(internal, #pgc_protocol_message:data_row{values = Values}, #simple_query{row_description = Fields}, Data) ->
    #pgc_connection:connection{
        handler_module = Module,
        handler_state = HandlerState0
    } = Data,
    {ok, HandlerState1} = Module:handle_row_data(Fields, Values, HandlerState0),
    {keep_state, Data#pgc_connection:connection{handler_state = HandlerState1}};

handle_event(internal, #pgc_protocol_message:command_complete{tag = Tag}, #simple_query{} = State, Data) ->
    #pgc_connection:connection{
        handler_module = Module,
        handler_state = HandlerState0
    } = Data,
    {ok, HandlerState1} = Module:handle_result(Tag, HandlerState0),
    {next_state, State#simple_query{row_description = undefined}, Data#pgc_connection:connection{
        handler_state = HandlerState1
    }};

handle_event(internal, #pgc_protocol_message:empty_query_response{}, #simple_query{} = State, Data) ->
    #pgc_connection:connection{
        handler_module = Module,
        handler_state = HandlerState0
    } = Data,
    {ok, HandlerState1} = Module:handle_result(empty, HandlerState0),
    {next_state, State#simple_query{row_description = undefined}, Data#pgc_connection:connection{
        handler_state = HandlerState1
    }};

handle_event(internal, #pgc_protocol_message:error_response{fields = Fields}, #simple_query{}, Data) ->
    #pgc_connection:connection{
        handler_module = Module,
        handler_state = HandlerState0
    } = Data,
    {ok, HandlerState1} = Module:handle_error(pgc_protocol:from_error_response_fields(Fields), HandlerState0),
    {keep_state, Data#pgc_connection:connection{handler_state = HandlerState1}};

handle_event(internal, #pgc_protocol_message:ready_for_query{status = Status}, #simple_query{}, Data) ->
    {next_state, #pgc_connection:ready{status = Status}, Data, [
        pop_callback_module
    ]};


% -------------------------------------------------------------------------------
% state: * -- anything not specific to the simple query sub-protocol.
% -------------------------------------------------------------------------------

handle_event(Type, Content, State, Data) ->
    pgc_connection:handle_common_event(Type, Content, State, Data).


% ------------------------------------------------------------------------------
% Internals
% ------------------------------------------------------------------------------

ensure_deps_loaded() ->
    {module, _} = code:ensure_loaded(pgc_connection),
    {module, _} = code:ensure_loaded(pgc_protocol_message),
    ok.
