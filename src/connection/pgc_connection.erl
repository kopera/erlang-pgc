%% @private
-module(pgc_connection).
-export([
    execute/4
    % transaction/3,
    % reset/1
]).
-export_type([
    options/0,
    parameters/0
]).

-export([
    start_link/3,
    stop/1
]).

-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3,
    format_status/1
]).

-include_lib("kernel/include/logger.hrl").
-include("../protocol/pgc_message.hrl").
-include("../types/pgc_type.hrl").
-include("./pgc_connection.hrl").


%% @private
-spec execute(Connection, Statement, Parameters, Options) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: pid(),
    Statement :: unicode:chardata(),
    Parameters :: [term()],
    Options :: pgc:execute_options(),
    Metadata :: pgc:execute_metadata(),
    Rows :: [term()],
    Error :: pgc_error:t().

execute(Connection, StatementText, Parameters, Options) ->
    StatementName = case Options of
        #{cache := {true, Key}} -> atom_to_binary(Key, utf8);
        % #{cache := true} -> binary:encode_hex(crypto:hash(sha, StatementText));
        #{} -> <<>>
    end,
    case prepare_(Connection, StatementName, StatementText) of
        {ok, PreparedStatement} ->
            execute_(Connection, PreparedStatement, Parameters, Options);
        {error, _} = Error ->
            Error
    end.

%% @private
prepare_(Connection, StatementName, StatementText) ->
    StatementBinary = unicode:characters_to_binary(StatementText),
    StatementHash = crypto:hash(sha256, StatementBinary),
    try gen_statem:call(Connection, {prepare, StatementName, StatementHash, StatementBinary}) of
        Result -> Result
    catch
        exit:{noproc, _} -> exit(noproc)
    end.

%% @private
execute_(Connection, Statement, Parameters, Options) ->
    ExecutionRef = erlang:monitor(process, Connection, [{alias, demonitor}]),
    try gen_statem:call(Connection, {execute, self(), ExecutionRef, Statement, Parameters}) of
        {ok, RowColumns} ->
            RowFormat = maps:get(row, Options, map),
            Collector = fun
                (row, RowValues, {Notices, Rows}) ->
                    Row = case RowFormat of
                        map -> maps:from_list(lists:zip(RowColumns, RowValues));
                        tuple -> list_to_tuple(RowValues);
                        list -> RowValues;
                        proplist -> lists:zip(RowColumns, RowValues)
                    end,
                    {Notices, [Row | Rows]};
                (notice, Notice, {Notices, Rows}) ->
                    {[Notice | Notices], Rows}
            end,
            case execute_collect(Collector, ExecutionRef, {[], []}) of
                {ok, Metadata, {Notices, Rows}} ->
                    {ok, Metadata#{columns => RowColumns, notices => lists:reverse(Notices)}, lists:reverse(Rows)};
                {error, Error, {_Notices, _Rows}} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    catch
        exit:{noproc, _} ->
            exit(noproc)
    after
        erlang:demonitor(ExecutionRef, [flush])
    end.

%% @private
execute_collect(Collector, ExecutionRef, Acc0) ->
    receive
        {data, ExecutionRef, {row, Row}} ->
            execute_collect(Collector, ExecutionRef, Collector(row, Row, Acc0));
        {notice, ExecutionRef, Notice} ->
            execute_collect(Collector, ExecutionRef, Collector(notice, Notice, Acc0));
        {done, ExecutionRef, {ok, Tag}} ->
            Metadata = decode_tag(Tag),
            {ok, Metadata, Acc0};
        {done, ExecutionRef, {error, Error}} ->
            {error, Error, Acc0};
        {'DOWN', ExecutionRef, _, _, Reason} ->
            exit(Reason)
    end.


%% @private
-spec decode_tag(undefined) -> #{};
                (unicode:unicode_binary()) -> #{command := atom(), rows => non_neg_integer()}.
decode_tag(undefined) ->
    #{};

decode_tag(Tag) ->
    case binary:split(Tag, <<" ">>, [global]) of
        [<<"SELECT">>, Count] ->
            #{command => select, rows => binary_to_integer(Count)};
        [<<"INSERT">>, _Oid, Count] ->
            #{command => insert, rows => binary_to_integer(Count)};
        [<<"UPDATE">>, Count] ->
            #{command => update, rows => binary_to_integer(Count)};
        [<<"DELETE">>, Count] ->
            #{command => delete, rows => binary_to_integer(Count)};
        [<<"MERGE">>, Count] ->
            #{command => merge, rows => binary_to_integer(Count)};
        [<<"MOVE">>, Count] ->
            #{command => move, rows => binary_to_integer(Count)};
        [<<"FETCH">>, Count] ->
            #{command => fetch, rows => binary_to_integer(Count)};
        [<<"COPY">>, Count] ->
            #{command => copy, rows => binary_to_integer(Count)};
        [Command | _Rest] ->
            #{command => binary_to_atom(string:lowercase(Command))}
    end.


% ------------------------------------------------------------------------------
% Private/Internal API
% ------------------------------------------------------------------------------

%% @private
-spec start_link(TransportOptions, ConnectionOptions, OwnerPid) -> {ok, pid()} when
    TransportOptions :: pgc_transport:options(),
    ConnectionOptions :: options(),
    OwnerPid :: pid().
-type options() :: #{
    user := unicode:chardata(),
    password => unicode:chardata() | fun(() -> unicode:chardata()),
    database := unicode:chardata(),
    parameters => parameters(),

    hibernate_after => timeout()
}.
-type parameters() :: #{
    atom() => unicode:chardata()
}.
start_link(TransportOptions, ConnectionOptions, OwnerPid) ->
    gen_statem:start_link(?MODULE, {OwnerPid, TransportOptions, ConnectionOptions}, [
        {hibernate_after, maps:get(hibernate_after, ConnectionOptions, 5000)}
    ]).


%% @private
-spec stop(pid()) -> ok.
stop(Connection) ->
    gen_statem:stop(Connection).


% ------------------------------------------------------------------------------
% Data records
% ------------------------------------------------------------------------------

-record(connection, {
    transport :: pgc_transport:t(),
    transport_tags :: pgc_transport:tags(),
    transport_buffer :: binary(),

    key :: {pos_integer(), pos_integer()} | undefined,
    parameters :: pgc_connection:parameters(),

    types :: pgc_types:t(),
    statements :: #{unicode:unicode_binary() => statement()},

    codecs :: pgc_codecs:t()
}).

% ------------------------------------------------------------------------------
% State records
% ------------------------------------------------------------------------------

-record(disconnected, {
}).

-record(authenticating, {
    username :: unicode:unicode_binary(),
    database :: unicode:unicode_binary(),
    parameters :: pgc_connection:parameters(),
    auth_state :: pgc_auth:state()
}).


-record(configuring, {
}).


-record(initializing, {
}).

-record(ready, {
}).


-record(preparing, {
    from :: gen_statem:from(),

    statement_name :: unicode:unicode_binary(),
    statement_hash :: binary(),
    statement_text :: unicode:unicode_binary(),
    statement_parameters = [] :: [pgc_type:oid()]
}).

-record(unpreparing, {
    statement_name :: unicode:unicode_binary()
}).

-record(executing, {
    client_pid :: pid(),
    client_monitor :: reference(),

    execution_ref :: reference(),

    statement_name :: unicode:unicode_binary(),
    statement_parameters :: [iodata()],
    statement_result_format :: nonempty_list(text | binary),
    statement_result_row_decoder :: fun(([binary()]) -> {ok, term()} | {error, pgc_error:t()})
}).

-record(refreshing_types, {
    missing :: [pgc_type:oid()]
}).

-record(syncing, {}).

% ------------------------------------------------------------------------------
% Internal records
% ------------------------------------------------------------------------------

-record(pgc_statement, {
    name :: binary(),
    % text :: unicode:unicode_binary(),
    hash :: binary(),
    parameters :: [pgc_type:oid()],
    result :: [#pgc_row_field{}]
}).
-type statement() :: #pgc_statement{}.


% ------------------------------------------------------------------------------
% gen_statem callbacks
% ------------------------------------------------------------------------------

%% @private
init({OwnerPid, TransportOptions, ConnectionOptions}) ->
    OwnerMonitor = erlang:monitor(process, OwnerPid, [
        {tag, {'DOWN', owner}}
    ]),
    erlang:put(owner_pid, OwnerPid),
    erlang:put(owner_monitor, OwnerMonitor),
    {ok, #disconnected{}, undefined, [
        {next_event, internal, {connect, TransportOptions, ConnectionOptions}}
    ]}.


%% @private
callback_mode() ->
    [handle_event_function, state_enter].


%% @private

% State: disconnected ----------------------------------------------------------

handle_event(enter, _, #disconnected{}, undefined) ->
    keep_state_and_data;

handle_event(internal, {connect, TransportOptions, ConnectionOptions}, #disconnected{}, undefined) ->
    case pgc_transport:connect(TransportOptions) of
        {ok, Transport} ->
            #{user := Username, database := Database} = ConnectionOptions,
            Parameters = maps:get(parameters, ConnectionOptions, #{}),
            PasswordFun = case maps:get(password, ConnectionOptions, <<>>) of
                Fun when is_function(Fun, 0) ->
                    Fun;
                Password when is_binary(Password); is_list(Password) ->
                    fun () -> Password  end
            end,
            {next_state, #authenticating{
                username = Username,
                database = Database,
                parameters = Parameters,
                auth_state = pgc_auth:init(Username, PasswordFun)
            }, #connection{
                transport = Transport,
                transport_tags = pgc_transport:get_tags(Transport),
                transport_buffer = <<>>,

                key = undefined,
                parameters = #{},

                types = pgc_types:new(),
                statements = #{},

                codecs = pgc_codecs:new(#{})
            }};
        {error, TransportError} ->
            Error = pgc_error:client_connection_failure(case TransportError of
                econnrefused -> "Connection refused";
                timeout -> "Connection timed out";
                {tls, unavailable} -> "TLS unavailable";
                {tls, TLSError} -> {"TLS failed with error: ~p", TLSError}
            end),
            notify_owner({error, Error}),
            {stop, normal}
    end;

% State: authenticating --------------------------------------------------------

handle_event(enter, #disconnected{}, #authenticating{} = Authenticating, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    #authenticating{
        username = Username,
        database = Database,
        parameters = Parameters
    } = Authenticating,
    ok = send(Transport, #msg_startup{
        parameters = maps:merge(Parameters, #{
            user => Username,
            database => Database
        })
    }),
    ok = pgc_transport:set_active(Transport, once),
    keep_state_and_data;

handle_event(enter, #authenticating{}, #authenticating{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, #msg_auth{type = ok}, #authenticating{}, #connection{} = ConnectionData) ->
    {next_state, #configuring{}, ConnectionData};

handle_event(internal, #msg_error_response{} = Message, #authenticating{}, #connection{} = _ConnectionData) ->
    notify_owner({error, pgc_error:from_message(Message)}),
    {stop, normal};

handle_event(internal, #msg_auth{type = AuthType, data = AuthData}, #authenticating{} = Authenticating, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    #authenticating{auth_state = AuthState} = Authenticating,
    case pgc_auth:handle(AuthType, AuthData, AuthState) of
        ok ->
            keep_state_and_data;
        {ok, Response, AuthState1} ->
            ok = send(Transport, Response),
            {next_state, Authenticating#authenticating{
                auth_state = AuthState1
            }, ConnectionData};
        {error, Error} ->
            notify_owner({error, Error}),
            {stop, normal}
    end;

% State: configuring -----------------------------------------------------------

handle_event(enter, #authenticating{}, #configuring{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, #msg_ready_for_query{}, #configuring{}, #connection{} = ConnectionData) ->
    {next_state, #initializing{}, ConnectionData};

handle_event(internal, #msg_error_response{} = Message, #configuring{}, #connection{} = _ConnectionData) ->
    notify_owner({error, pgc_error:from_message(Message)}),
    {stop, normal};

handle_event(internal, #msg_backend_key_data{id = Id, secret = Secret}, #configuring{}, #connection{} = ConnectionData) ->
    {keep_state, ConnectionData#connection{
        key = {Id, Secret}
    }};

% State: initializing -----------------------------------------------------------

handle_event(enter, #configuring{}, #initializing{}, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    ok = send(Transport, [
        #msg_parse{
            name = ?refresh_types_statement_name,
            statement = ?refresh_types_statement_text
        },
        #msg_sync{}
    ]),
    keep_state_and_data;

handle_event(internal, #msg_parse_complete{}, #initializing{}, #connection{} = _ConnectionData) ->
    keep_state_and_data;

handle_event(internal, #msg_error_response{} = Message, #initializing{}, #connection{}) ->
    notify_owner({error, pgc_error:from_message(Message)}),
    {stop, normal};

handle_event(internal, #msg_ready_for_query{}, #initializing{}, #connection{} = ConnectionData) ->
    notify_owner(connected),
    {next_state, #ready{}, ConnectionData};

% State: ready -----------------------------------------------------------------

handle_event(enter, _, #ready{}, #connection{}) ->
    keep_state_and_data;

handle_event({call, From}, {prepare, <<?internal_statement_name_prefix, _/binary>> = Name, _, _}, #ready{}, #connection{}) ->
    {keep_state_and_data, [
        {reply, From, {error, pgc_error:invalid_sql_statement_name(Name)}}
    ]};

handle_event({call, From}, {prepare, StatementName, StatementHash, StatementText}, #ready{}, #connection{} = ConnectionData) ->
    #connection{statements = Statements} = ConnectionData,
    case Statements of
        #{StatementName := #pgc_statement{hash = StatementHash} = Statement} ->
            {keep_state_and_data, [
                {reply, From, {ok, Statement}}
            ]};
        #{StatementName := _} when StatementName =/= <<>> ->
            {next_state, #unpreparing{
                statement_name = StatementName
            }, ConnectionData, [postpone]};
        #{} ->
            {next_state, #preparing{
                from = From,
                statement_name = StatementName,
                statement_hash = StatementHash,
                statement_text = StatementText
            }, ConnectionData}
    end;

handle_event({call, From}, {execute, ClientPid, ExecutionRef, #pgc_statement{} = Statement, Parameters}, #ready{}, #connection{} = ConnectionData) ->
    #connection{types = ConnectionTypes, codecs = Codecs} = ConnectionData,
    #pgc_statement{name = StatementName, parameters = ParametersDesc, result = ResultDesc} = Statement,
    ParametersTypeIDs = ParametersDesc,
    RowColumns = [binary_to_atom(Field#pgc_row_field.name) || Field <- ResultDesc],
    RowTypeIDs = [Field#pgc_row_field.type_oid || Field <- ResultDesc],
    case pgc_types:find_all(ParametersTypeIDs ++ RowTypeIDs, ConnectionTypes) of
        {ok, Types} ->
            ParametersTypes = [maps:get(TypeID, Types) || TypeID <- ParametersTypeIDs],
            RowTypes = [maps:get(TypeID, Types) || TypeID <- RowTypeIDs],
            GetTypeFun = fun (TypeID) ->
                case pgc_types:find(TypeID, ConnectionTypes) of
                    {ok, Type} -> Type;
                    error -> erlang:error({type_missing, TypeID})
                end
            end,
            case encode_parameters(ParametersTypes, Parameters, GetTypeFun, Codecs) of
                {ok, StatementParameters} ->
                    {next_state, #executing{
                        client_pid = ClientPid,
                        client_monitor = erlang:monitor(process, ClientPid, [
                            {tag, {'DOWN', client}}
                        ]),
                        execution_ref = ExecutionRef,
                        statement_name = StatementName,
                        statement_parameters = StatementParameters,
                        statement_result_format = [binary],
                        statement_result_row_decoder = fun (RowData) ->
                            decode_row(RowTypes, RowData, GetTypeFun, Codecs)
                        end
                    }, ConnectionData, [
                        {reply, From, {ok, RowColumns}}
                    ]};
                {error, _} = Error ->
                    {keep_state_and_data, [
                        {reply, From, Error}
                    ]}
            end;
        {error, _, MissingTypeIDs} ->
            {next_state, #refreshing_types{missing = MissingTypeIDs}, ConnectionData, [postpone]}
    end;

handle_event({call, From}, Request, #ready{}, #connection{} = _ConnectionData) ->
    {keep_state_and_data, [
        {reply, From, {bad_request, Request}}
    ]};

handle_event(cast, _, #ready{}, #connection{} = _ConnectionData) ->
    keep_state_and_data;


% State: preparing -------------------------------------------------------------

handle_event(enter, #ready{}, #preparing{} = Preparing, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    #preparing{statement_name = StatementName, statement_text = StatementText} = Preparing,
    ok = send(Transport, [
        #msg_parse{name = StatementName, statement = StatementText},
        #msg_describe{type = statement, name = StatementName},
        #msg_sync{}
    ]),
    keep_state_and_data;

handle_event(enter, #preparing{}, #preparing{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, #msg_parse_complete{}, #preparing{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, #msg_parameter_description{types = ParametersTypes}, #preparing{} = Preparing, #connection{} = ConnectionData) ->
    {next_state, Preparing#preparing{statement_parameters = ParametersTypes}, ConnectionData};

handle_event(internal, Message, #preparing{} = Preparing, #connection{} = ConnectionData)
        when is_record(Message, msg_row_description)
            ;is_record(Message, msg_no_data) ->
    #connection{statements = Statements} = ConnectionData,
    #preparing{
        from = From,
        statement_name = StatementName,
        statement_hash = StatementHash,
        statement_parameters = StatementParameters
    } = Preparing,
    Statement = #pgc_statement{
        name = StatementName,
        hash = StatementHash,
        % text = StatementText,
        parameters = StatementParameters,
        result = case Message of
            #msg_row_description{fields = F} -> F;
            #msg_no_data{} -> []
        end
    },
    {next_state, #syncing{}, ConnectionData#connection{
        statements = maps:put(StatementName, Statement, Statements)
    }, [
        {reply, From, {ok, Statement}}
    ]};

handle_event(internal, #msg_error_response{} = Message, #preparing{} = Preparing, #connection{} = ConnectionData) ->
    #preparing{from = From} = Preparing,
    {next_state, #syncing{}, ConnectionData, [
        {reply, From, {error, pgc_error:from_message(Message)}}
    ]};

handle_event(internal, {pgc_transport, closed}, #preparing{} = Preparing, #connection{} = _ConnectionData) ->
    #preparing{from = From} = Preparing,
    notify_owner({error, pgc_error:disconnected()}),
    {stop_and_reply, normal, [
        {reply, From, {error, pgc_error:disconnected()}}
    ]};

% State: unpreparing -----------------------------------------------------------

handle_event(enter, #ready{}, #unpreparing{} = Unpreparing, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    #unpreparing{statement_name = StatementName} = Unpreparing,
    ok = send(Transport, [
        #msg_close{type = statement, name = StatementName},
        #msg_sync{}
    ]),
    keep_state_and_data;

handle_event(internal, #msg_close_complete{}, #unpreparing{} = Unpreparing, #connection{} = ConnectionData) ->
    #connection{statements = Statements} = ConnectionData,
    #unpreparing{statement_name = StatementName} = Unpreparing,
    {next_state, #syncing{}, ConnectionData#connection{
        statements = maps:remove(StatementName, Statements)
    }};

handle_event(internal, #msg_error_response{}, #unpreparing{} = Unpreparing, #connection{} = ConnectionData) ->
    #connection{statements = Statements} = ConnectionData,
    #unpreparing{statement_name = StatementName} = Unpreparing,
    {next_state, #syncing{}, ConnectionData#connection{
        statements = maps:remove(StatementName, Statements)
    }};

% State: executing -------------------------------------------------------------

handle_event(enter, #ready{}, #executing{} = Executing, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    #executing{
        statement_name = StatementName,
        statement_parameters = StatementParameters,
        statement_result_format = StatementResultFormat
    } = Executing,
    ok = send(Transport, [
        #msg_bind{
            statement = StatementName,
            portal = <<>>,
            parameters = StatementParameters,
            results = StatementResultFormat
        },
        #msg_execute{portal = <<>>},
        #msg_sync{}
    ]),
    keep_state_and_data;

handle_event(internal, #msg_bind_complete{}, #executing{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, #msg_notice_response{fields = Notice}, #executing{} = Executing, #connection{} = _ConnectionData) ->
    #executing{execution_ref = ExecutionRef} = Executing,
    ExecutionRef ! {notice, ExecutionRef, Notice},
    keep_state_and_data;

handle_event(internal, #msg_data_row{values = RowData}, #executing{} = Executing, #connection{} = _ConnectionData) ->
    #executing{execution_ref = ExecutionRef, statement_result_row_decoder = RowDecoder} = Executing,
    case RowDecoder(RowData) of
        {ok, Row} ->
            ExecutionRef ! {data, ExecutionRef, {row, Row}},
            keep_state_and_data;
        {error, Error} ->
            {stop, {error, Error}}
    end;

handle_event(internal, #msg_command_complete{tag = Tag}, #executing{} = Executing, #connection{} = ConnectionData) ->
    #executing{client_monitor = ClientMonitor, execution_ref = ExecutionRef} = Executing,
    ExecutionRef ! {done, ExecutionRef, {ok, Tag}},
    _ = erlang:demonitor(ClientMonitor),
    {next_state, #syncing{}, ConnectionData};

handle_event(internal, #msg_empty_query_response{}, #executing{} = Executing, #connection{} = ConnectionData) ->
    #executing{client_monitor = ClientMonitor, execution_ref = ExecutionRef} = Executing,
    ExecutionRef ! {done, ExecutionRef, {ok, undefined}},
    _ = erlang:demonitor(ClientMonitor),
    {next_state, #syncing{}, ConnectionData};

handle_event(internal, #msg_error_response{} = Message, #executing{} = Executing, #connection{} = ConnectionData) ->
    #executing{client_monitor = ClientMonitor, execution_ref = ExecutionRef} = Executing,
    ExecutionRef ! {done, ExecutionRef, {error, pgc_error:from_message(Message)}},
    _ = erlang:demonitor(ClientMonitor),
    {next_state, #syncing{}, ConnectionData};

handle_event(cast, {cancel, ClientMonitor}, #executing{} = Executing, #connection{} = ConnectionData) ->
    #connection{transport = Transport, key = BackendKey} = ConnectionData,
    #executing{client_monitor = ClientMonitor} = Executing,
    _ = erlang:demonitor(ClientMonitor),
    ok = send_cancel_request(Transport, BackendKey),
    {next_state, #syncing{}, ConnectionData};

handle_event(info, {{'DOWN', client}, ClientMonitor, process, _ClientPid, _Reason}, #executing{client_monitor = ClientMonitor}, #connection{} = ConnectionData) ->
    #connection{transport = Transport, key = BackendKey} = ConnectionData,
    ok = send_cancel_request(Transport, BackendKey),
    {next_state, #syncing{}, ConnectionData};

handle_event(internal, {pgc_transport, closed}, #executing{} = Executing, #connection{} = _ConnectionData) ->
    #executing{client_monitor = ClientMonitor, execution_ref = ExecutionRef} = Executing,
    ExecutionRef ! {done, ExecutionRef, {error, pgc_error:disconnected()}},
    _ = erlang:demonitor(ClientMonitor),
    notify_owner({error, pgc_error:disconnected()}),
    {stop, normal};

% State: refreshing types ------------------------------------------------------

handle_event(enter, _, #refreshing_types{}, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    ok = send(Transport, [
        #msg_bind{
            statement = ?refresh_types_statement_name,
            portal = <<>>,
            parameters = [],
            results = [binary, binary, binary, binary, text, text, binary, binary, binary, binary]
        },
        #msg_execute{portal = <<>>},
        #msg_sync{}
    ]),
    {keep_state, ConnectionData#connection{
        types = pgc_types:new()
    }};

handle_event(internal, #msg_parse_complete{}, #refreshing_types{}, #connection{} = _ConnectionData) ->
    keep_state_and_data;

handle_event(internal, #msg_bind_complete{}, #refreshing_types{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, #msg_data_row{values = Row}, #refreshing_types{}, #connection{} = ConnectionData) ->
    #connection{types = Types} = ConnectionData,
    {keep_state, ConnectionData#connection{
        types = pgc_types:add(decode_type(Row), Types)
    }};

handle_event(internal, #msg_command_complete{}, #refreshing_types{} = RefreshingTypes, #connection{} = ConnectionData) ->
    #connection{types = Types} = ConnectionData,
    #refreshing_types{missing = MissingTypeIDs} = RefreshingTypes,
    {ok, _} = pgc_types:find_all(MissingTypeIDs, Types),
    {next_state, #syncing{}, ConnectionData};

handle_event(internal, #msg_error_response{} = Message, #refreshing_types{}, #connection{}) ->
    {stop, {error, pgc_error:from_message(Message)}};

% State: syncing ---------------------------------------------------------------

handle_event(enter, _, #syncing{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, Message, #syncing{}, #connection{} = _ConnectionData)
        when is_record(Message, msg_bind_complete)
            ;is_record(Message, msg_data_row)
            ;is_record(Message, msg_command_complete)
            ;is_record(Message, msg_empty_query_response)
            ;is_record(Message, msg_close_complete)
            ;is_record(Message, msg_error_response) ->
    keep_state_and_data;

handle_event(internal, #msg_ready_for_query{}, #syncing{}, #connection{} = ConnectionData) ->
    {next_state, #ready{}, ConnectionData};

% State: * ---------------------------------------------------------------------

handle_event(internal, #msg_parameter_status{name = Name, value = Value}, _, #connection{} = ConnectionData) ->
    #connection{parameters = Parameters} = ConnectionData,
    {keep_state, ConnectionData#connection{
        parameters = maps:put(binary_to_atom(Name), Value, Parameters)
    }};

handle_event(internal, #msg_notice_response{}, _, #connection{} = _ConnectionData) ->
    %% TODO: log notices?
    keep_state_and_data;

handle_event(internal, #msg_notification_response{}, _, #connection{} = _ConnectionData) ->
    %% TODO: notify owning process
    keep_state_and_data;

handle_event(internal, {pgc_transport, closed}, _S, #connection{} = _ConnectionData) ->
    notify_owner({error, pgc_error:disconnected()}),
    {stop, normal};

handle_event(internal, {pgc_transport, set_active}, _, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    ok = pgc_transport:set_active(Transport, once),
    keep_state_and_data;

handle_event({call, _}, _Request, State, #connection{} = _ConnectionData) when not is_record(State, ready) ->
    {keep_state_and_data, [postpone]};

handle_event(cast, _Notification, State, #connection{} = _ConnectionData) when not is_record(State, ready) ->
    {keep_state_and_data, [postpone]};

handle_event(info, {DataTag, Source, Data}, _, #connection{transport_tags = {Source, DataTag, _, _, _}} = ConnectionData) ->
    #connection{transport_buffer = TransportBuffer} = ConnectionData,
    {Messages, Rest} = pgc_messages:decode(<<TransportBuffer/binary, Data/binary>>),
    Events = Messages ++ [{pgc_transport, set_active}],
    {keep_state, ConnectionData#connection{transport_buffer = Rest}, [
        {next_event, internal, Event} || Event <- Events
    ]};

handle_event(info, {ClosedTag, Source}, _, #connection{transport_tags = {Source, _, ClosedTag, _, _}}) ->
    {keep_state_and_data, [
        {next_event, internal, {pgc_transport, closed}}
    ]};

handle_event(info = EventType, {{'DOWN', owner}, OwnerMonitor, process, OwnerPid, _Reason} = EventContent, State, _ConnectionData) ->
    case {erlang:get(owner_pid), erlang:get(owner_monitor)} of
        {OwnerPid, OwnerMonitor} ->
            {stop, normal};
        _ ->
            ?LOG_WARNING(#{
                label => {?MODULE, unhandled_event},
                state => State,
                event_type => EventType,
                event_content => EventContent
            }),
            keep_state_and_data
    end;

handle_event(EventType, EventContent, State, _Data) ->
    ?LOG_WARNING(#{
        label => {?MODULE, unhandled_event},
        state => State,
        event_type => EventType,
        event_content => EventContent
    }),
    keep_state_and_data.


%% @private
terminate(_Reason, _State, undefined) ->
    ok;

terminate(_Reason, _State, #connection{transport = Transport}) ->
    pgc_transport:close(Transport).


%% @private
format_status(Status) ->
    maps:map(fun
        (data, #connection{} = Connection) ->
            Connection#connection{types = omitted};
        (_, Value) ->
                Value
    end, Status).


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

-spec notify_owner(term()) -> ok.
notify_owner(Message) ->
    case erlang:get(owner_pid) of
        OwnerPid when is_pid(OwnerPid) ->
            OwnerPid ! {?MODULE, self(), Message},
            ok
    end.


-spec send(pgc_transport:t(), [Message] | Message) -> ok | {error, Error} when
    Message :: pgc_message:message_f() | pgc_message:message_fb(),
    Error :: pgc_transport:send_error().
send(Transport, Messages) when is_list(Messages) ->
    ?LOG_DEBUG(#{
        label => {?MODULE, send},
        messages => Messages
    }),
    pgc_transport:send(Transport, pgc_messages:encode(Messages));
send(Transport, Message) ->
    send(Transport, [Message]).


-spec send_cancel_request(Transport, BackendKey) -> ok | {error, Error} when
    Transport :: pgc_transport:t(),
    BackendKey :: {non_neg_integer(), non_neg_integer()},
    Error :: pgc_transport:send_error().
send_cancel_request(Transport, {Id, Secret}) ->
    case pgc_transport:dup(Transport) of
        {ok, T} ->
            _ = send(T, #msg_cancel_request{id = Id, secret = Secret}),
            _ = pgc_transport:recv(T, 1),
            _ = pgc_transport:close(T),
            ok;
        {error, _} = Error ->
            Error
    end.


%% @private
decode_type([Oid, Namespace, Name, Type, Send, Recv, ElementType, ParentType, FieldsNames, FieldsTypes]) ->
    #pgc_type{
        oid = pgc_codec_oid:decode(Oid, []),
        namespace = binary_to_atom(pgc_codec_text:decode(Namespace, []), utf8),
        name = binary_to_atom(pgc_codec_text:decode(Name, []), utf8),
        type = case pgc_codec_char:decode(Type, []) of
            $b -> base;
            $c -> composite;
            $d -> domain;
            $e -> enum;
            $p -> pseudo;
            $r -> range;
            $m -> multirange;
            _ -> other
        end,
        send = binary_to_atom(pgc_codec_text:decode(Send, []), utf8),
        recv = binary_to_atom(pgc_codec_text:decode(Recv, []), utf8),
        element = case pgc_codec_oid:decode(ElementType, []) of
            0 -> undefined;
            ElementOid -> ElementOid
        end,
        parent = case pgc_codec_oid:decode(ParentType, []) of
            0 -> undefined;
            ParentOid -> ParentOid
        end,
        fields = decode_type_fields(FieldsNames, FieldsTypes)
    }.


%% @private
decode_type_fields(Names, Types) ->
    Fields = lists:zip(
        pgc_codec_array:decode(fun (_Oid, V) -> binary_to_atom(pgc_codec_text:decode(V, []), utf8) end, Names),
        pgc_codec_array:decode(fun (_Oid, V) -> pgc_codec_oid:decode(V, []) end, Types)
    ),
    if
        Fields =:= [] -> undefined;
        Fields =/= [] -> Fields
    end.


%% @private
encode_parameters(Types, Values, GetTypeFun, Codecs) when length(Types) =:= length(Values) ->
    encode_parameters(Types, Values, GetTypeFun, Codecs, []);
encode_parameters(Types, Values, _GetTypeFun, _Codecs) ->
    {error, pgc_error:protocol_violation({"Statement requires ~b parameters, ~b supplied", [length(Types), length(Values)]})}.

%% @private
encode_parameters([], [], _GetTypeFun, _Codecs, Acc) ->
    {ok, lists:reverse(Acc)};
encode_parameters([Type | Types], [Value | Values], GetTypeFun, Codecs, Acc) ->
    try pgc_codecs:encode(Type, Value, GetTypeFun, Codecs) of
        Encoded ->
            encode_parameters(Types, Values, GetTypeFun, Codecs, [Encoded | Acc])
    catch
        error:badarg ->
            {error, pgc_error:invalid_parameter_value(length(Acc) + 1, Value, Type)}
    end.


%% @private
decode_row(RowTypes, RowData, GetTypeFun, Codecs) ->
    decode_row(RowTypes, RowData, GetTypeFun, Codecs, []).

%% @private
decode_row([], [], _GetTypeFun, _Codecs, Acc) ->
    {ok, lists:reverse(Acc)};
decode_row([Type | Types], [Value | Values], GetTypeFun, Codecs, Acc) ->
    try pgc_codecs:decode(Type, Value, GetTypeFun, Codecs) of
        Encoded ->
            decode_row(Types, Values, GetTypeFun, Codecs, [Encoded | Acc])
    catch
        error:badarg ->
            TypeName = Type#pgc_type.name,
            Index = length(Acc) + 1,
            {error, pgc_error:protocol_violation(
                {"Failed to decode row field of type '~s' value '~p' at index ~b", [TypeName, Value, Index]}
            )}
    end.