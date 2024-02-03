%% @private
-module(pgc_connection).
-export([
    start_link/3,
    reset/1,
    stop/1
]).
-export_type([
    statement/0
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

-opaque statement() :: #statement{}.


-spec start_link(TransportOptions, ConnectionOptions, OwnerPid) -> {ok, pid()} when
    TransportOptions :: pgc:transport_options(),
    ConnectionOptions :: pgc:client_options(),
    OwnerPid :: pid().
start_link(TransportOptions, ConnectionOptions, OwnerPid) ->
    HibernateAfter = case ConnectionOptions of
        #{ping_interval := infinity} -> ?default_hibernate_after;
        #{ping_interval := PingInterval} -> PingInterval div 2;
        #{} -> ?default_hibernate_after
    end,
    {ok, _} = gen_statem:start_link(?MODULE, {OwnerPid, TransportOptions, ConnectionOptions}, [
        {hibernate_after, HibernateAfter}
    ]).


-spec reset(pid()) -> ok.
reset(Connection) ->
    gen_statem:cast(Connection, reset).


-spec stop(pid()) -> ok.
stop(Connection) ->
    gen_statem:stop(Connection).


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

handle_event(internal, {connect, TransportOptions, ConnectionOptions}, #disconnected{}, undefined = ConnectionData) ->
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
            PingInterval = maps:get(ping_interval, ConnectionOptions, ?default_ping_interval),
            Codecs = maps:get(codecs, ConnectionOptions, #{}),
            CodecsExtras = maps:get(extras, Codecs, []),
            CodecsOptions = maps:get(options, Codecs, #{}),
            {next_state, #authenticating{
                username = Username,
                database = Database,
                parameters = Parameters,
                auth_state = pgc_auth:init(Username, PasswordFun)
            }, #connection{
                transport = Transport,
                transport_tags = pgc_transport:get_tags(Transport),
                transport_buffer = <<>>,

                ping_interval = PingInterval,
                ping_timeout = if
                    is_integer(PingInterval) andalso PingInterval > 0 ->
                        2 * PingInterval;
                    PingInterval =:= infinity ->
                        infinity
                end,

                key = undefined,
                parameters = #{},

                types = pgc_types:new(),
                statements = #{},

                codecs = pgc_codecs:new(CodecsExtras, CodecsOptions)
            }};
        {error, TransportError} ->
            Error = pgc_error:client_connection_failure(case TransportError of
                econnrefused -> "connection refused";
                timeout -> "connection timed out";
                {tls, unavailable} -> "tls unavailable";
                {tls, TLSError} -> {"tls failed with error: ~w", [TLSError]}
            end),
            {next_state, #stopping{
                reason = Error
            }, ConnectionData}
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

handle_event(internal, #msg_error_response{} = Message, #authenticating{}, #connection{} = ConnectionData) ->
    {next_state, #stopping{
        reason = pgc_error:from_message(Message)
    }, ConnectionData};

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
            {next_state, #stopping{
                reason = Error
            }, ConnectionData}
    end;

% State: configuring -----------------------------------------------------------

handle_event(enter, #authenticating{}, #configuring{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, #msg_ready_for_query{}, #configuring{}, #connection{} = ConnectionData) ->
    {next_state, #initializing{}, ConnectionData};

handle_event(internal, #msg_error_response{} = Message, #configuring{}, #connection{} = ConnectionData) ->
    {next_state, #stopping{
        reason = pgc_error:from_message(Message)
    }, ConnectionData};

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

handle_event(internal, #msg_error_response{} = Message, #initializing{}, #connection{} = ConnectionData) ->
    {next_state, #stopping{
        reason = pgc_error:from_message(Message)
    }, ConnectionData};

handle_event(internal, #msg_ready_for_query{status = Status}, #initializing{}, #connection{} = ConnectionData) ->
    notify_owner(connected),
    {next_state, #ready{status = Status}, ConnectionData};

% State: ready -----------------------------------------------------------------

handle_event(enter, _From, #ready{}, #connection{ping_interval = PingInterval}) ->
    {keep_state_and_data, [
        {state_timeout, PingInterval, ping}
    ]};

handle_event({call, From}, {prepare, <<?internal_statement_name_prefix, _/binary>> = Name, _, _}, #ready{}, #connection{}) ->
    {keep_state_and_data, [
        {reply, From, {error, pgc_error:invalid_sql_statement_name(Name)}}
    ]};

handle_event({call, From}, {prepare, StatementName0, StatementText0, _Options}, #ready{}, #connection{} = ConnectionData) ->
    #connection{statements = Statements} = ConnectionData,
    StatementName = pgc_string:to_binary(StatementName0),
    StatementText = pgc_string:to_binary(StatementText0),
    StatementTextHash = crypto:hash(sha256, StatementText),
    StatementParametersTypes = [],
    case Statements of
        #{StatementName := #statement{hash = StatementTextHash} = Statement} ->
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
                statement_text = StatementText,
                statement_text_hash = StatementTextHash,
                statement_parameters_types = StatementParametersTypes
            }, ConnectionData}
    end;

handle_event({call, From}, {execute, ClientPid, ExecutionRef, #statement{} = Statement, Parameters}, #ready{}, #connection{} = ConnectionData) ->
    #connection{types = ConnectionTypes, codecs = Codecs} = ConnectionData,
    #statement{name = StatementName, parameters = ParametersDesc, result = ResultDesc} = Statement,
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
                        client_ref = ExecutionRef,
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
            {next_state, #refreshing_types{
                missing = MissingTypeIDs
            }, ConnectionData, [
                postpone
            ]}
    end;

handle_event({call, From}, {execute, StatementText}, #ready{}, #connection{} = ConnectionData)
        when is_binary(StatementText); is_list(StatementText) ->
    {next_state, #executing_simple{
        from = From,
        statement_text = StatementText
    }, ConnectionData};

handle_event({call, From}, Request, #ready{}, #connection{} = _ConnectionData) ->
    {keep_state_and_data, [
        {reply, From, {bad_request, Request}}
    ]};

handle_event(cast, {unprepare, StatementName0}, #ready{}, #connection{} = ConnectionData) ->
    case pgc_string:to_binary(StatementName0) of
        <<>> ->
            keep_state_and_data;
        StatementName ->
            {next_state, #unpreparing{
                statement_name = StatementName
            }, ConnectionData}
    end;

handle_event(cast, reset, #ready{status = idle}, #connection{} = ConnectionData) ->
    {next_state, #executing_simple{
        from = self,
        statement_text = <<"reset all">>
    }, ConnectionData};

handle_event(cast, reset, #ready{status = _}, #connection{} = ConnectionData) ->
    {next_state, #executing_simple{
        from = self,
        statement_text = <<"rollback">>
    }, ConnectionData, [
        postpone
    ]};

handle_event(cast, _, #ready{}, #connection{} = _ConnectionData) ->
    keep_state_and_data;

handle_event(state_timeout, ping, #ready{}, #connection{ping_timeout = PingTimeout} = ConnectionData) ->
    {next_state, #syncing{timeout = PingTimeout}, ConnectionData};


% State: preparing -------------------------------------------------------------

handle_event(enter, #ready{}, #preparing{} = Preparing, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    #preparing{statement_name = StatementName, statement_text = StatementText} = Preparing,
    ok = send(Transport, [
        #msg_parse{name = StatementName, statement = StatementText},
        #msg_describe{type = statement, name = StatementName},
        #msg_flush{}
    ]),
    keep_state_and_data;

handle_event(enter, #preparing{}, #preparing{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, #msg_parse_complete{}, #preparing{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, #msg_parameter_description{types = ParametersTypes}, #preparing{} = Preparing, #connection{} = ConnectionData) ->
    {next_state, Preparing#preparing{statement_parameters_types = ParametersTypes}, ConnectionData};

handle_event(internal, Message, #preparing{} = Preparing, #connection{} = ConnectionData)
        when is_record(Message, msg_row_description)
            ;is_record(Message, msg_no_data) ->
    #connection{statements = Statements} = ConnectionData,
    #preparing{
        from = From,
        statement_name = StatementName,
        statement_text_hash = StatementTextHash,
        statement_parameters_types = StatementParametersTypes
    } = Preparing,
    Statement = #statement{
        name = StatementName,
        % text = StatementText,
        hash = StatementTextHash,
        parameters = StatementParametersTypes,
        result = case Message of
            #msg_row_description{fields = RowFields} -> RowFields;
            #msg_no_data{} -> []
        end
    },
    {next_state, #syncing{timeout = infinity}, ConnectionData#connection{
        statements = maps:put(StatementName, Statement, Statements)
    }, [
        {reply, From, {ok, Statement}}
    ]};

handle_event(internal, #msg_error_response{} = Message, #preparing{} = Preparing, #connection{} = ConnectionData) ->
    #preparing{from = From} = Preparing,
    {next_state, #syncing{timeout = infinity}, ConnectionData, [
        {reply, From, {error, pgc_error:from_message(Message)}}
    ]};

handle_event(internal, {pgc_transport, closed}, #preparing{} = Preparing, #connection{} = _ConnectionData) ->
    #preparing{from = From} = Preparing,
    Error = pgc_error:disconnected(),
    {next_state, #stopping{
        reason = Error
    }, undefined, [
        {reply, From, {error, Error}}
    ]};

% State: unpreparing -----------------------------------------------------------

handle_event(enter, #ready{}, #unpreparing{} = Unpreparing, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    #unpreparing{statement_name = StatementName} = Unpreparing,
    ok = send(Transport, [
        #msg_close{type = statement, name = StatementName},
        #msg_flush{}
    ]),
    keep_state_and_data;

handle_event(internal, Message, #unpreparing{} = Unpreparing, #connection{} = ConnectionData)
        when is_record(Message, msg_close_complete)
            ;is_record(Message, msg_error_response) ->
    #connection{statements = Statements} = ConnectionData,
    #unpreparing{statement_name = StatementName} = Unpreparing,
    {next_state, #syncing{timeout = infinity}, ConnectionData#connection{
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
        #msg_flush{}
    ]),
    keep_state_and_data;

handle_event(internal, #msg_bind_complete{}, #executing{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, #msg_notice_response{fields = Notice}, #executing{} = Executing, #connection{} = _ConnectionData) ->
    #executing{
        client_ref = ClientRef,
        execution_ref = ExecutionRef
    } = Executing,
    ClientRef ! {notice, ExecutionRef, Notice},
    keep_state_and_data;

handle_event(internal, #msg_data_row{values = RowData}, #executing{} = Executing, #connection{} = _ConnectionData) ->
    #executing{
        client_ref = ClientRef,
        execution_ref = ExecutionRef,
        statement_result_row_decoder = RowDecoder
    } = Executing,
    case RowDecoder(RowData) of
        {ok, Row} ->
            ClientRef ! {data, ExecutionRef, {row, Row}},
            keep_state_and_data;
        {error, Error} ->
            {stop, {error, Error}}
    end;

handle_event(internal, Message, #executing{} = Executing, #connection{} = ConnectionData)
        when is_record(Message, msg_command_complete)
            ;is_record(Message, msg_empty_query_response)
            ;is_record(Message, msg_error_response) ->
    #executing{
        client_ref = ClientRef,
        client_monitor = ClientMonitor,
        execution_ref = ExecutionRef
    } = Executing,
    Result = case Message of
        #msg_command_complete{tag = Tag} -> {ok, Tag};
        #msg_empty_query_response{} -> {ok, undefined};
        #msg_error_response{} -> {error, pgc_error:from_message(Message)}
    end,
    ClientRef ! {done, ExecutionRef, Result},
    _ = erlang:demonitor(ClientMonitor),
    {next_state, #syncing{timeout = infinity}, ConnectionData};

handle_event(cast, {cancel, ExecutionRef}, #executing{execution_ref = ExecutionRef} = Executing, #connection{} = ConnectionData) ->
    #executing{
        client_monitor = ClientMonitor
    } = Executing,
    _ = erlang:demonitor(ClientMonitor),
    {next_state, #canceling{}, ConnectionData};

handle_event(cast, reset, #executing{} = Executing, #connection{} = ConnectionData) ->
    #executing{
        client_ref = ClientRef,
        client_monitor = ClientMonitor,
        execution_ref = ExecutionRef
    } = Executing,
    ClientRef ! {done, ExecutionRef, {error, pgc_error:statement_timeout()}},
    _ = erlang:demonitor(ClientMonitor),
    {next_state, #canceling{}, ConnectionData};

handle_event(info, {{'DOWN', client}, ClientMonitor, process, _ClientPid, _Reason}, #executing{client_monitor = ClientMonitor}, #connection{} = ConnectionData) ->
    {next_state, #canceling{}, ConnectionData};

handle_event(internal, {pgc_transport, closed}, #executing{} = Executing, #connection{} = _ConnectionData) ->
    #executing{
        client_ref = ClientRef,
        client_monitor = ClientMonitor,
        execution_ref = ExecutionRef
    } = Executing,
    Error = pgc_error:disconnected(),
    ClientRef ! {done, ExecutionRef, {error, Error}},
    _ = erlang:demonitor(ClientMonitor),
    {next_state, #stopping{
        reason = Error
    }, undefined};

% State: executing simple ------------------------------------------------------

handle_event(enter, #ready{}, #executing_simple{} = Executing, #connection{} = ConnectionData) ->
    #connection{transport = Transport, statements = PreparedStatements} = ConnectionData,
    #executing_simple{
        statement_text = StatementText
    } = Executing,
    ok = send(Transport, [
        #msg_parse{name = "", statement = StatementText},
        #msg_bind{portal = "", statement = ""},
        #msg_execute{portal = ""},
        #msg_flush{}
    ]),
    {keep_state, ConnectionData#connection{
        statements = maps:remove(<<>>, PreparedStatements)
    }};

handle_event(internal, #msg_parse_complete{}, #executing_simple{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, #msg_bind_complete{}, #executing_simple{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, Message, #executing_simple{from = self}, #connection{} = ConnectionData)
        when is_record(Message, msg_command_complete)
            ;is_record(Message, msg_empty_query_response) ->
    {next_state, #syncing{timeout = infinity}, ConnectionData};

handle_event(internal, #msg_error_response{} = Message, #executing_simple{from = self}, #connection{}) ->
    {stop, {error, pgc_error:from_message(Message)}};

handle_event(internal, Message, #executing_simple{from = From}, #connection{} = ConnectionData)
        when is_record(Message, msg_command_complete)
            ;is_record(Message, msg_empty_query_response)
            ;is_record(Message, msg_error_response) ->
    Result = case Message of
        #msg_command_complete{tag = Tag} -> {ok, Tag};
        #msg_empty_query_response{} -> {ok, undefined};
        #msg_error_response{} -> {error, pgc_error:from_message(Message)}
    end,
    {next_state, #syncing{timeout = infinity}, ConnectionData, [
        {reply, From, Result}
    ]};

handle_event(cast, reset, #executing_simple{from = From}, #connection{} = ConnectionData) when From =/= self ->
    {next_state, #canceling{}, ConnectionData, [
        {reply, From, {error, pgc_error:statement_timeout()}}
    ]};

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
        #msg_flush{}
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
    {next_state, #syncing{timeout = infinity}, ConnectionData};

handle_event(internal, #msg_error_response{} = Message, #refreshing_types{}, #connection{}) ->
    {stop, {error, pgc_error:from_message(Message)}};

% State: canceling ---------------------------------------------------------------

handle_event(enter, _, #canceling{}, #connection{key = {_Id, _Secret} = BackendKey} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    ok = send_cancel_request(Transport, BackendKey),
    ok = send(Transport, [
        #msg_sync{}
    ]),
    keep_state_and_data;

handle_event(internal, #msg_error_response{}, #canceling{}, #connection{} = _ConnectionData) ->
    keep_state_and_data;

handle_event(internal, #msg_ready_for_query{status = Status}, #canceling{}, #connection{} = ConnectionData) ->
    {next_state, #ready{status = Status}, ConnectionData};

% State: syncing ---------------------------------------------------------------

handle_event(enter, _, #syncing{} = Syncing, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    #syncing{timeout = Timeout} = Syncing,
    ok = send(Transport, [
        #msg_sync{}
    ]),
    {keep_state_and_data, [
        {state_timeout, Timeout, pang}
    ]};

handle_event(internal, #msg_ready_for_query{status = Status}, #syncing{}, #connection{} = ConnectionData) ->
    {next_state, #ready{status = Status}, ConnectionData};

handle_event(state_timeout, pang, #syncing{}, #connection{} = ConnectionData) ->
    {next_state, #stopping{
        reason = pgc_error:disconnected("server connection timed out")
    }, ConnectionData};

% State: stopping --------------------------------------------------------------

handle_event(enter, _, #stopping{reason = Reason}, undefined) ->
    notify_stop(Reason),
    {stop, normal};

handle_event(enter, _, #stopping{reason = Reason}, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    notify_stop(Reason),
    ok = send(Transport, [
        #msg_terminate{}
    ]),
    {keep_state_and_data, [
        {state_timeout, 100, stop}
    ]};

handle_event(internal, {transport, closed}, #stopping{}, #connection{} = _ConnectionData) ->
    {stop, normal, undefined};

handle_event(state_timeout, stop, #stopping{}, #connection{} = _ConnectionData) ->
    {stop, normal, undefined};

% State: * ---------------------------------------------------------------------

handle_event({call, _}, _Request, State, #connection{} = _ConnectionData) when not is_record(State, ready) ->
    {keep_state_and_data, [postpone]};

handle_event(cast, _Notification, State, #connection{} = _ConnectionData) when not is_record(State, ready) ->
    {keep_state_and_data, [postpone]};

handle_event(internal, {pgc_transport, closed}, _S, #connection{} = _ConnectionData) ->
    {next_state, #stopping{
        reason = pgc_error:disconnected()
    }, undefined};

handle_event(internal, {pgc_transport, set_active}, _, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    ok = pgc_transport:set_active(Transport, once),
    keep_state_and_data;

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

handle_event(info, {{'DOWN', owner}, _OwnerMonitor, process, _OwnerPid, _Reason}, _State, ConnectionData) ->
    {next_state, #stopping{
        reason = normal
    }, ConnectionData};

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
            %% eqwalizer:ignore
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


-spec notify_stop(normal | pgc_error:t()) -> ok.
notify_stop(normal) ->
    ok;
notify_stop(Reason) ->
    notify_owner({error, Reason}).


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
    {error, pgc_error:protocol_violation({"statement requires ~b parameters, ~b supplied", [length(Types), length(Values)]})}.

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
                {"failed to decode row field of type '~s' value '~w' at index ~b", [TypeName, Value, Index]}
            )}
    end.
