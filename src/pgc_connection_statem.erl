%% @private
-module(pgc_connection_statem).
-feature(maybe_expr, enable).

-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3
]).

-include_lib("kernel/include/logger.hrl").
-include("./pgc_message.hrl").
-include("./pgc_statement.hrl").
-include("./pgc_type.hrl").

-define(internal_statement_name_prefix, "_pgc_connection_:").
-define(refresh_types_statement_name, <<?internal_statement_name_prefix, "refresh_types">>).
-define(refresh_types_statement_text, <<
"select
    pg_type.oid as oid,
    pg_type.typname as name,
    pg_type.typtype as type,
    pg_type.typsend as send,
    pg_type.typreceive as recv,
    pg_type.typelem as element_type,
    coalesce(pg_range.rngsubtype, 0) as parent_type,
    array (
        select pg_attribute.attname
        from pg_attribute
        where pg_attribute.attrelid = pg_type.typrelid
          and pg_attribute.attnum > 0
          and not pg_attribute.attisdropped
        order by pg_attribute.attnum
    ) as fields_names,
    array (
        select pg_attribute.atttypid
        from pg_attribute
        where pg_attribute.attrelid = pg_type.typrelid
          and pg_attribute.attnum > 0
          and not pg_attribute.attisdropped
        order by pg_attribute.attnum
    ) as fields_types
from pg_catalog.pg_type
  left join pg_catalog.pg_range on pg_range.rngtypid = pg_type.oid"
>>).

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
    statements :: #{unicode:unicode_binary() => #pgc_statement{}}
}).

% ------------------------------------------------------------------------------
% State records
% ------------------------------------------------------------------------------

-record(disconnected, {
}).

-record(authenticating, {
    from :: gen_statem:from(),

    username :: unicode:unicode_binary(),
    database :: unicode:unicode_binary(),
    parameters :: pgc_connection:parameters(),
    auth_state :: pgc_auth:state()
}).


-record(configuring, {
    from :: gen_statem:from()
}).


-record(initializing, {
    from :: gen_statem:from()
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
    statement_result_format :: [text | binary]
}).

-record(refreshing_types, {
    missing :: [pgc_type:oid()]
}).

-record(syncing, {}).

% ------------------------------------------------------------------------------
% gen_statem callbacks
% ------------------------------------------------------------------------------

%% @hidden
init(OwnerPid) ->
    OwnerMonitor = erlang:monitor(process, OwnerPid),
    erlang:put(owner_pid, OwnerPid),
    erlang:put(owner_monitor, OwnerMonitor),
    {ok, #disconnected{}, undefined}.


%% @hidden
callback_mode() ->
    [handle_event_function, state_enter].


%% @hidden

% State: disconnected ----------------------------------------------------------

handle_event(enter, _, #disconnected{}, undefined) ->
    keep_state_and_data;

handle_event({call, From}, {open, Transport, Options}, #disconnected{}, undefined) ->
    #{user := Username, database := Database} = Options,
    {next_state, #authenticating{
        from = From,
        username = Username,
        database = Database,
        parameters = maps:get(parameters, Options, #{}),
        auth_state = pgc_auth:init(Username, case maps:get(password, Options, <<>>) of
            Fun when is_function(Fun, 0) ->
                Fun;
            Password when is_binary(Password); is_list(Password) ->
                fun () -> Password  end
        end)
    }, #connection{
        transport = Transport,
        transport_tags = pgc_transport:get_tags(Transport),
        transport_buffer = <<>>,

        key = undefined,
        parameters = #{},

        types = pgc_types:new(),
        statements = #{}
    }};

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

handle_event(internal, #msg_auth{type = ok}, #authenticating{} = Authenticating, #connection{} = ConnectionData) ->
    #authenticating{from = From} = Authenticating,
    {next_state, #configuring{from = From}, ConnectionData};

handle_event(internal, #msg_error_response{} = Message, #authenticating{} = Authenticating, #connection{} = _ConnectionData) ->
    #authenticating{
        from = From
    } = Authenticating,
    {stop_and_reply, normal, [
        {reply, From, {error, pgc_error:from_message(Message)}}
    ]};

handle_event(internal, #msg_auth{type = AuthType, data = AuthData}, #authenticating{} = Authenticating, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    #authenticating{from = From, auth_state = AuthState} = Authenticating,
    case pgc_auth:handle(AuthType, AuthData, AuthState) of
        ok ->
            keep_state_and_data;
        {ok, Response, AuthState1} ->
            ok = send(Transport, Response),
            {next_state, Authenticating#authenticating{
                auth_state = AuthState1
            }, ConnectionData};
        {error, Error} ->
            {stop_and_reply, normal, [
                {reply, From, {error, Error}}
            ]}
    end;

handle_event(internal, {pgc_transport, closed}, #authenticating{} = Authenticating, #connection{} = _ConnectionData) ->
    #authenticating{from = From} = Authenticating,
    {stop_and_reply, normal, [
        {reply, From, {error, pgc_error:disconnected()}}
    ]};

% State: configuring -----------------------------------------------------------

handle_event(enter, #authenticating{}, #configuring{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, #msg_ready_for_query{}, #configuring{} = Configuring, #connection{} = ConnectionData) ->
    #configuring{from = From} = Configuring,
    {next_state, #initializing{from = From}, ConnectionData};

handle_event(internal, #msg_error_response{} = Message, #configuring{} = Configuring, #connection{} = _ConnectionData) ->
    #configuring{from = From} = Configuring,
    {stop_and_reply, normal, [
        {reply, From, {error, pgc_error:from_message(Message)}}
    ]};

handle_event(internal, #msg_backend_key_data{id = Id, secret = Secret}, #configuring{}, #connection{} = ConnectionData) ->
    {keep_state, ConnectionData#connection{
        key = {Id, Secret}
    }};

handle_event(internal, {pgc_transport, closed}, #configuring{} = Configuring, #connection{} = _ConnectionData) ->
    #configuring{from = From} = Configuring,
    {stop_and_reply, normal, [
        {reply, From, {error, pgc_error:disconnected()}}
    ]};

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

handle_event(internal, #msg_error_response{} = Message, #initializing{} = Initializing, #connection{}) ->
    #initializing{from = From} = Initializing,
    {stop_and_reply, normal, [
        {reply, From, {error, pgc_error:from_message(Message)}}
    ]};

handle_event(internal, #msg_ready_for_query{}, #initializing{} = Initializing, #connection{} = ConnectionData) ->
    #initializing{from = From} = Initializing,
    {next_state, #ready{}, ConnectionData, [
        {reply, From, ok}
    ]};

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
    #connection{parameters = ConnectionParameters, types = ConnectionTypes} = ConnectionData,
    #pgc_statement{name = StatementName, parameters = ParametersDesc, result = ResultDesc} = Statement,
    ParametersTypeIDs = ParametersDesc,
    ResultTypeIDs = [Field#pgc_row_field.type_oid || Field <- ResultDesc],
    case pgc_types:lookup(ParametersTypeIDs ++ ResultTypeIDs, ConnectionTypes) of
        {Types, []} ->
            maybe
                {ok, Codec} ?= pgc_codec:init(Types, ConnectionParameters, #{}),
                {ok, StatementParameters} ?= encode_parameters(Codec, ParametersTypeIDs, Parameters),
                {next_state, #executing{
                    client_pid = ClientPid,
                    client_monitor = erlang:monitor(process, ClientPid),
                    execution_ref = ExecutionRef,
                    statement_name = StatementName,
                    statement_parameters = StatementParameters,
                    statement_result_format = [binary]
                }, ConnectionData, [
                    {reply, From, {ok, Codec}}
                ]}
            else
                {error, _} = Error ->
                    {keep_state_and_data, [
                        {reply, From, Error}
                    ]}
            end;
        {_Types, MissingTypeIDs} ->
            {next_state, #refreshing_types{missing = MissingTypeIDs}, ConnectionData, [postpone]}
    end;

handle_event({call, From}, Request, #ready{}, #connection{} = _ConnectionData) ->
    {keep_state_and_data, [
        {reply, From, {bad_request, Request}}
    ]};

handle_event(cast, _, #ready{}, #connection{} = _ConnectionData) ->
    keep_state_and_data;

handle_event(internal, {pgc_transport, closed}, #ready{}, #connection{} = _ConnectionData) ->
    notify_owner(closed),
    {stop, normal};

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
    notify_owner(closed),
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

handle_event(internal, #msg_data_row{values = Row}, #executing{} = Executing, #connection{} = _ConnectionData) ->
    #executing{execution_ref = ExecutionRef} = Executing,
    ExecutionRef ! {data, ExecutionRef, {row, Row}},
    keep_state_and_data;

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

handle_event(info, {'DOWN', ClientMonitor, process, _ClientPid, _Reason}, #executing{client_monitor = ClientMonitor}, #connection{} = ConnectionData) ->
    #connection{transport = Transport, key = BackendKey} = ConnectionData,
    ok = send_cancel_request(Transport, BackendKey),
    {next_state, #syncing{}, ConnectionData};

handle_event(internal, {pgc_transport, closed}, #executing{} = Executing, #connection{} = _ConnectionData) ->
    #executing{client_monitor = ClientMonitor, execution_ref = ExecutionRef} = Executing,
    ExecutionRef ! {done, ExecutionRef, {error, pgc_error:disconnected()}},
    _ = erlang:demonitor(ClientMonitor),
    notify_owner(closed),
    {stop, normal};

% State: refreshing types ------------------------------------------------------

handle_event(enter, _, #refreshing_types{}, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    ok = send(Transport, [
        #msg_bind{
            statement = ?refresh_types_statement_name,
            portal = <<>>,
            parameters = [],
            results = [binary, binary, binary, text, text, binary, binary, binary, binary]
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
    {_, []} = pgc_types:lookup(MissingTypeIDs, Types),
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
    notify_owner(closed),
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

handle_event(info = EventType, {'DOWN', OwnerMonitor, process, OwnerPid, _Reason} = EventContent, State, _ConnectionData) ->
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


%% @hidden
terminate(_Reason, _State, undefined) ->
    ok;

terminate(_Reason, _State, #connection{transport = Transport}) ->
    pgc_transport:close(Transport).

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


-spec send(pgc_transport:t(), [Message] | Message) -> ok | {error, send_error()} when
    Message :: pgc_message:message_f() | pgc_message:message_fb().
-type send_error() :: pgc_transport:send_error().
send(Transport, Messages) when is_list(Messages) ->
    ?LOG_DEBUG(#{
        label => {?MODULE, send},
        messages => Messages
    }),
    pgc_transport:send(Transport, pgc_messages:encode(Messages));
send(Transport, Message) ->
    send(Transport, [Message]).


-spec send_cancel_request(Transport, BackendKey) -> ok | {error, send_error()} when
    Transport :: pgc_transport:t(),
    BackendKey :: {non_neg_integer(), non_neg_integer()}.
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
decode_type([Oid, Name, Type, Send, Recv, ElementType, ParentType, FieldsNames, FieldsTypes]) ->
    #pgc_type{
        oid = pgc_codec_oid:decode(Oid),
        name = binary_to_atom(pgc_codec_text:parse(Name), utf8),
        type = case pgc_codec_text:parse(Type) of
            <<"b">> -> base;
            <<"c">> -> composite;
            <<"d">> -> domain;
            <<"e">> -> enum;
            <<"p">> -> pseudo;
            <<"r">> -> range;
            <<"m">> -> multirange;
            _ -> other
        end,
        send = binary_to_atom(pgc_codec_text:parse(Send), utf8),
        recv = binary_to_atom(pgc_codec_text:parse(Recv), utf8),
        element = case pgc_codec_oid:decode(ElementType) of
            0 -> undefined;
            ElementOid -> ElementOid
        end,
        parent = case pgc_codec_oid:decode(ParentType) of
            0 -> undefined;
            ParentOid -> ParentOid
        end,
        fields = decode_type_fields(FieldsNames, FieldsTypes)
    }.


%% @private
decode_type_fields(Names, Types) ->
    lists:zip(
        pgc_codec_array:decode(fun (_Oid, V) -> binary_to_atom(pgc_codec_text:parse(V), utf8) end, Names),
        pgc_codec_array:decode(fun (_Oid, V) -> pgc_codec_oid:decode(V) end, Types)
    ).


%% @private
encode_parameters(Codec, TypeIDs, Values) when length(TypeIDs) =:= length(Values) ->
    encode_parameters(Codec, TypeIDs, Values, []);
encode_parameters(_Codec, TypeIDs, Values) ->
    {error, pgc_error:protocol_violation({"Statement requires ~b parameters, ~b supplied", [length(TypeIDs), length(Values)]})}.

%% @private
encode_parameters(_Codec, [], [], Acc) ->
    {ok, lists:reverse(Acc)};
encode_parameters(Codec, [TypeID | TypeIDs], [Value | Values], Acc) ->
    try pgc_codec:encode(TypeID, Value, Codec) of
        Encoded ->
            encode_parameters(Codec, TypeIDs, Values, [Encoded | Acc])
    catch
        error:badarg ->
            Type = pgc_codec:get_type(TypeID, Codec),
            {error, pgc_error:invalid_parameter_value(length(Acc) + 1, Value, Type)}
    end.