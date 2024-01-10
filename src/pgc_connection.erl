-module(pgc_connection).
-export([
    execute/3,
    execute/4
]).
-export_type([
    options/0,
    parameters/0
]).

-export([
    start_link/1,
    startup/3
]).

-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3
]).

-include_lib("kernel/include/logger.hrl").
-include("./pgc_message.hrl").
-include("./pgc_type.hrl").
-define(refresh_types_statement_name, <<"_pgc_connection:refresh_types">>).
-define(refresh_types_statement, <<
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

    key = {0, 0} :: {non_neg_integer(), non_neg_integer()},
    parameters = #{} :: parameters(),
    types = pgc_types:new() :: pgc_types:t(),
    prepared_statements = #{} :: #{unicode:unicode_binary() => prepared_statement()},

    buffer :: binary()
}).

% ------------------------------------------------------------------------------
% State records
% ------------------------------------------------------------------------------

-record(disconnected, {
}).


-record(authenticating, {
    from :: gen_statem:from(),
    database :: unicode:chardata(),
    auth_state :: pgc_auth:state()
}).


-record(configuring, {
    from :: gen_statem:from()
}).


-record(ready, {
}).


-record(preparing, {
    from :: gen_statem:from(),

    name :: unicode:unicode_binary(),
    statement :: unicode:unicode_binary(),
    parameters = [] :: [pgc_type:oid()]
}).

-record(unpreparing, {
    name :: unicode:unicode_binary()
}).

-record(executing, {
    client_pid :: pid(),
    client_monitor :: reference(),
    execution_ref :: reference(),

    name :: unicode:unicode_binary(),
    parameters :: [iodata()],
    result :: fun(([binary()]) -> term())
}).

-record(refreshing, {
    types = pgc_types:new() :: pgc_types:t()
}).


-record(syncing, {}).

% ------------------------------------------------------------------------------
% Internal records
% ------------------------------------------------------------------------------

-record(prepared_statement, {
    name :: binary(),
    statement :: unicode:unicode_binary(),
    parameters :: [pgc_type:oid()],
    result :: [#msg_row_description_field{}]
}).
-type prepared_statement() :: #prepared_statement{}.


% ------------------------------------------------------------------------------
% Public API
% ------------------------------------------------------------------------------

-spec execute(Connection, Statement, Params) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: pid(),
    Statement :: unicode:chardata(),
    Params :: [term()],
    Metadata :: execute_metadata(),
    Rows :: execute_rows(),
    Error :: execute_error().
execute(Connection, Statement, Params) ->
    execute(Connection, Statement, Params, #{}).


-spec execute(Connection, Statement, Params, execute_options()) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: pid(),
    Statement :: unicode:chardata(),
    Params :: [term()],
    Metadata :: execute_metadata(),
    Rows :: execute_rows(),
    Error :: execute_error().
-type execute_options() :: #{
    timeout => timeout(),
    cache => false | {true, atom()}
}.
-type execute_metadata() :: #{
    command => atom(),
    columns => [atom()],
    rows => non_neg_integer(),
    notices => [map()]
}.
-type execute_rows() :: [term()].
-type execute_error() :: map().
execute(Connection, Statement, Params, Options) ->
    StatementName = case Options of
        #{cache := {true, Key}} -> atom_to_binary(Key, utf8);
        % #{cache := true} -> binary:encode_hex(crypto:hash(sha, Statement));
        #{} -> <<>>
    end,
    case prepare_(Connection, StatementName, Statement) of
        {ok, #prepared_statement{} = PreparedStatement} ->
            execute_(Connection, PreparedStatement, Params, Options);
        {error, _} = Error ->
            Error
    end.


%% @private
prepare_(Connection, StatementName, Statement) ->
    try gen_statem:call(Connection, {prepare, StatementName, unicode:characters_to_binary(Statement)}) of
        Result -> Result
    catch
        exit:{noproc, _} -> exit(noproc)
    end.


%% @private
execute_(Connection, #prepared_statement{result = ResultDesc} = PreparedStatement, Params, Options) ->
    ExecutionRef = erlang:monitor(process, Connection, [
        {alias, demonitor}
    ]),
    try gen_statem:call(Connection, {execute, self(), ExecutionRef, PreparedStatement, Params}) of
        ok ->
            Columns = [binary_to_atom(Field#msg_row_description_field.name) || Field <- ResultDesc],
            RowFormat = maps:get(row, Options, map),
            Collector = fun
                (row, RowData, {Notices, Rows}) ->
                    Row = case RowFormat of
                        map -> maps:from_list(lists:zip(Columns, RowData));
                        tuple -> list_to_tuple(RowData);
                        list -> RowData;
                        proplist -> lists:zip(Columns, RowData)
                    end,
                    {Notices, [Row | Rows]};
                (notice, Notice, {Notices, Rows}) ->
                    {[Notice | Notices], Rows}
            end,
            try execute_collect(Collector, ExecutionRef, {[], []}) of
                {ok, Metadata, {Notices, Rows}} ->
                    {ok, Metadata#{notices => lists:reverse(Notices)}, lists:reverse(Rows)};
                {error, Error, {_Notices, _Rows}} ->
                    {error, Error}
            after
                erlang:demonitor(ExecutionRef, [flush])
            end;
        {error, Error} ->
            {error, Error}
    catch
        exit:{noproc, _} ->
            exit(noproc)
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


% ------------------------------------------------------------------------------
% Private/Internal API
% ------------------------------------------------------------------------------

%% @private
-spec start_link(pid()) -> {ok, pid()}.
start_link(OwnerPid) ->
    {ok, _Pid} = gen_statem:start_link(?MODULE, [OwnerPid], [
        {hibernate_after, 5000}
    ]).


%% @private
-spec startup(Connection, Transport, ConnectionOptions) -> ok | {error, pgc_error:t()} when
    Connection :: pid(),
    Transport :: pgc_transport:t(),
    ConnectionOptions :: options().
-type options() :: #{
    user := unicode:chardata(),
    password => unicode:chardata() | fun(() -> unicode:chardata()),
    database := unicode:chardata(),
    parameters => parameters()
}.
-type parameters() :: #{atom() => unicode:chardata()}.
startup(Connection, Transport, ConnectionOptions) ->
    gen_statem:call(Connection, {startup, Transport, ConnectionOptions}).

% ------------------------------------------------------------------------------
% gen_statem callbacks
% ------------------------------------------------------------------------------

%% @hidden
init([OwnerPid]) ->
    OwnerMonitor = erlang:monitor(process, OwnerPid),
    erlang:put(owner_pid, OwnerPid),
    erlang:put(owner_monitor, OwnerMonitor),
    {ok, #disconnected{}, undefined}.

%% @hidden
callback_mode() ->
    [handle_event_function, state_enter].


%% @hidden

% ------------------------------------------------------------------------------
% State: disconnected
% ------------------------------------------------------------------------------

handle_event(enter, _, #disconnected{}, undefined) ->
    keep_state_and_data;

handle_event({call, From}, {startup, Transport, ConnectionOptions}, #disconnected{}, undefined) ->
    #{
        user := User,
        database := Database
    } = ConnectionOptions,
    PasswordFun = case maps:get(password, ConnectionOptions, <<>>) of
        Fun when is_function(Fun, 0) ->
            Fun;
        Password when is_binary(Password); is_list(Password) ->
            fun () -> Password  end
    end,
    ok = send(Transport, #msg_startup{
        parameters = case ConnectionOptions of
            #{parameters := Parameters} ->
                maps:merge(Parameters, #{user => User, database => Database});
            #{} ->
                #{user => User, database => Database}
        end
    }),
    ok = pgc_transport:set_active(Transport, once),
    {next_state, #authenticating{
        from = From,
        database = Database,
        auth_state = pgc_auth:init(User, PasswordFun)
    }, #connection{
        transport = Transport,
        transport_tags = pgc_transport:get_tags(Transport),
        key = {0, 0},
        parameters = #{},
        buffer = <<>>
    }};


% ------------------------------------------------------------------------------
% State: authenticating
% ------------------------------------------------------------------------------

handle_event(enter, _, #authenticating{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, #msg_auth{type = ok}, #authenticating{} = Authenticating, #connection{} = ConnectionData) ->
    #authenticating{
        from = From
    } = Authenticating,
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
            {next_state, Authenticating#authenticating{auth_state = AuthState1}, ConnectionData};
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

% ------------------------------------------------------------------------------
% State: configuring
% ------------------------------------------------------------------------------

handle_event(enter, _, #configuring{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, #msg_ready_for_query{}, #configuring{from = From}, #connection{} = ConnectionData) ->
    {next_state, #ready{}, ConnectionData, [
        {reply, From, ok}
    ]};

handle_event(internal, #msg_backend_key_data{id = Id, secret = Secret}, #configuring{}, #connection{} = ConnectionData) ->
    {keep_state, ConnectionData#connection{
        key = {Id, Secret}
    }};

handle_event(internal, #msg_error_response{} = Message, #configuring{from = From}, #connection{} = _ConnectionData) ->
    {stop_and_reply, normal, [
        {reply, From, {error, pgc_error:from_message(Message)}}
    ]};

handle_event(internal, {pgc_transport, closed}, #configuring{from = From}, #connection{} = _ConnectionData) ->
    {stop_and_reply, normal, [
        {reply, From, {error, pgc_error:disconnected()}}
    ]};

% ------------------------------------------------------------------------------
% State: ready
% ------------------------------------------------------------------------------

handle_event(enter, _, #ready{}, #connection{}) ->
    keep_state_and_data;

handle_event({call, From}, {prepare, StatementName, Statement}, #ready{}, #connection{} = ConnectionData) ->
    #connection{prepared_statements = PreparedStatements} = ConnectionData,
    case PreparedStatements of
        #{StatementName := #prepared_statement{statement = Statement} = PreparedStatement} ->
            {keep_state_and_data, [
                {reply, From, {ok, PreparedStatement}}
            ]};
        #{StatementName := _} when StatementName =/= <<>> ->
            {next_state, #unpreparing{
                name = StatementName
            }, ConnectionData, [postpone]};
        #{} ->
            {next_state, #preparing{
                from = From,
                name = StatementName,
                statement = Statement
            }, ConnectionData}
    end;

handle_event({call, From}, {execute, ClientPid, ExecutionRef, #prepared_statement{} = PreparedStatement, ParamValues}, #ready{}, #connection{} = ConnectionData) ->
    #connection{parameters = ConnectionParameters, types = TypesDB} = ConnectionData,
    #prepared_statement{name = StatementName, parameters = ParamTypeIDs, result = ResultDesc} = PreparedStatement,
    ResultTypeIDs = [Field#msg_row_description_field.type_oid || Field <- ResultDesc],
    case pgc_types:lookup(ParamTypeIDs ++ ResultTypeIDs, TypesDB) of
        {Types, []} ->
            Codec = pgc_codec:new(Types, ConnectionParameters, #{}),
            StatementParameters = pgc_codec:encode_many(ParamTypeIDs, ParamValues, Codec),
            ClientMonitor = erlang:monitor(process, ClientPid),
            {next_state, #executing{
                client_pid = ClientPid,
                client_monitor = ClientMonitor,
                execution_ref = ExecutionRef,
                name = StatementName,
                parameters = StatementParameters,
                result = fun (Row) ->
                    pgc_codec:decode_many(ResultTypeIDs, Row, Codec)
                end
            }, ConnectionData, [
                {reply, From, ok}
            ]};
        {_Types, _MissingTypeIDs} ->
            {next_state, #refreshing{}, ConnectionData, [postpone]}
    end;

handle_event({call, From}, Request, #ready{}, #connection{} = _ConnectionData) ->
    {keep_state_and_data, [
        {reply, From, {bad_request, Request}}
    ]};

handle_event(cast, {unprepare, StatementName}, #ready{}, #connection{} = ConnectionData) ->
    {next_state, #unpreparing{name = StatementName}, ConnectionData};

handle_event(cast, _, #ready{}, #connection{} = _ConnectionData) ->
    keep_state_and_data;

handle_event(internal, {pgc_transport, closed}, #ready{}, #connection{} = _ConnectionData) ->
    notify_owner(closed),
    {stop, normal};

% ------------------------------------------------------------------------------
% State: preparing
% ------------------------------------------------------------------------------

handle_event(enter, _, #preparing{} = Preparing, #connection{} = Connection) ->
    #connection{transport = Transport} = Connection,
    #preparing{name = StatementName, statement = Statement} = Preparing,
    ok = send(Transport, [
        #msg_parse{name = StatementName, statement = Statement},
        #msg_describe{type = statement, name = StatementName},
        #msg_sync{}
    ]),
    keep_state_and_data;

handle_event(internal, #msg_parse_complete{}, #preparing{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, #msg_parameter_description{types = Types}, #preparing{} = Preparing, #connection{} = ConnectionData) ->
    {next_state, Preparing#preparing{parameters = Types}, ConnectionData};

handle_event(internal, Message, #preparing{} = Preparing, #connection{} = ConnectionData)
        when is_record(Message, msg_row_description)
            ;is_record(Message, msg_no_data) ->
    #connection{prepared_statements = PreparedStatements} = ConnectionData,
    #preparing{
        from = From,
        name = StatementName,
        statement = Statement,
        parameters = StatementParameters
    } = Preparing,
    PreparedStatement = #prepared_statement{
        name = StatementName,
        statement = Statement,
        parameters = StatementParameters,
        result = case Message of
            #msg_row_description{fields = F} -> F;
            #msg_no_data{} -> []
        end
    },
    {next_state, #syncing{}, ConnectionData#connection{
        prepared_statements = maps:put(StatementName, PreparedStatement, PreparedStatements)
    }, [
        {reply, From, {ok, PreparedStatement}}
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

% ------------------------------------------------------------------------------
% State: unpreparing
% ------------------------------------------------------------------------------

handle_event(enter, _, #unpreparing{} = Unpreparing, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    #unpreparing{name = StatementName} = Unpreparing,
    ok = send(Transport, [
        #msg_close{type = statement, name = StatementName},
        #msg_sync{}
    ]),
    keep_state_and_data;

handle_event(internal, #msg_close_complete{}, #unpreparing{} = Unpreparing, #connection{} = ConnectionData) ->
    #connection{prepared_statements = PreparedStatements} = ConnectionData,
    #unpreparing{name = StatementName} = Unpreparing,
    {next_state, #syncing{}, ConnectionData#connection{
        prepared_statements = maps:remove(StatementName, PreparedStatements)
    }};

handle_event(internal, #msg_error_response{}, #unpreparing{} = Unpreparing, #connection{} = ConnectionData) ->
    #connection{prepared_statements = PreparedStatements} = ConnectionData,
    #unpreparing{name = StatementName} = Unpreparing,
    {next_state, #syncing{}, ConnectionData#connection{
        prepared_statements = maps:remove(StatementName, PreparedStatements)
    }};


% ------------------------------------------------------------------------------
% State: executing
% ------------------------------------------------------------------------------

handle_event(enter, _, #executing{} = Executing, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    #executing{name = StatementName, parameters = Parameters} = Executing,
    ok = send(Transport, [
        #msg_bind{
            statement = StatementName,
            portal = <<>>,
            parameters = Parameters,
            results = [binary]
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
    #executing{execution_ref = ExecutionRef, result = ResultFun} = Executing,
    ExecutionRef ! {data, ExecutionRef, {row, ResultFun(Row)}},
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

handle_event(cast, {cancel, ClientMonitor}, #executing{client_monitor = ClientMonitor}, #connection{} = ConnectionData) ->
    #connection{transport = Transport, key = BackendKey} = ConnectionData,
    _ = erlang:demonitor(ClientMonitor),
    ok = send_cancel_request(Transport, BackendKey),
    {next_state, #syncing{}, ConnectionData};

handle_event(info, {'DOWN', ClientMonitor, process, _ClientPid, _Reason}, #executing{client_monitor = ClientMonitor}, #connection{} = ConnectionData) ->
    #connection{transport = Transport, key = BackendKey} = ConnectionData,
    ok = send_cancel_request(Transport, BackendKey),
    {next_state, #syncing{}, ConnectionData};

handle_event(internal, {pgc_transport, closed}, #executing{} = Executing, #connection{} = _ConnectionData) ->
    #executing{execution_ref = ExecutionRef} = Executing,
    ExecutionRef ! {done, ExecutionRef, {error, pgc_error:disconnected()}},
    % _ = erlang:demonitor(ClientMonitor),
    notify_owner(closed),
    {stop, normal};

% ------------------------------------------------------------------------------
% State: refreshing
% ------------------------------------------------------------------------------

handle_event(enter, #refreshing{}, #refreshing{}, #connection{}) ->
    keep_state_and_data;

handle_event(enter, _, #refreshing{}, #connection{} = ConnectionData) ->
    #connection{transport = Transport, prepared_statements = PreparedStatements} = ConnectionData,
    RefreshTypesCommands = [
        #msg_bind{
            statement = ?refresh_types_statement_name,
            portal = <<>>,
            parameters = [],
            results = [binary, binary, binary, text, text, binary, binary, binary, binary]
        },
        #msg_execute{portal = <<>>},
        #msg_sync{}
    ],
    case PreparedStatements of
        #{?refresh_types_statement_name := _PreparedStatement} ->
            ok = send(Transport, RefreshTypesCommands),
            keep_state_and_data;
        #{} ->
            ok = send(Transport, [
                #msg_parse{
                    name = ?refresh_types_statement_name,
                    statement = ?refresh_types_statement
                } |
                RefreshTypesCommands
            ]),
            keep_state_and_data
    end;

handle_event(internal, #msg_parse_complete{}, #refreshing{}, #connection{} = ConnectionData) ->
    #connection{prepared_statements = PreparedStatements} = ConnectionData,
    {keep_state, ConnectionData#connection{
        prepared_statements = PreparedStatements#{
            ?refresh_types_statement_name => #prepared_statement{
                name = ?refresh_types_statement_name,
                statement = ?refresh_types_statement_name,
                parameters = [],
                result = []
            }
        }
    }};

handle_event(internal, #msg_bind_complete{}, #refreshing{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, #msg_data_row{values = Row}, #refreshing{} = Refreshing, #connection{} = ConnectionData) ->
    #refreshing{types = Types} = Refreshing,
    {next_state, Refreshing#refreshing{
        types = pgc_types:add(decode_type(Row), Types)
    }, ConnectionData};

handle_event(internal, #msg_command_complete{}, #refreshing{} = Refreshing, #connection{} = ConnectionData) ->
    #refreshing{types = Types} = Refreshing,
    {next_state, #syncing{}, ConnectionData#connection{
        types = Types
    }};

handle_event(internal, #msg_error_response{} = Message, #refreshing{}, #connection{}) ->
    {stop, {error, pgc_error:from_message(Message)}};

% ------------------------------------------------------------------------------
% State: syncing
% ------------------------------------------------------------------------------

handle_event(enter, _, #syncing{}, #connection{}) ->
    keep_state_and_data;

handle_event(internal, Message, #syncing{}, #connection{} = _ConnectionData)
        when is_record(Message, msg_bind_complete)
            ;is_record(Message, msg_data_row)
            ;is_record(Message, msg_command_complete)
            ;is_record(Message, msg_empty_query_response)
            ;is_record(Message, msg_close_complete)
            ;is_record(Message, msg_error_response) ->
    %% Can occur after cancelling a request
    keep_state_and_data;

handle_event(internal, #msg_ready_for_query{}, #syncing{}, #connection{} = ConnectionData) ->
    {next_state, #ready{}, ConnectionData};

% ------------------------------------------------------------------------------
% State: *
% ------------------------------------------------------------------------------

handle_event(internal, #msg_parameter_status{name = Name, value = Value}, _, #connection{} = ConnectionData) ->
    {keep_state, ConnectionData#connection{
        parameters = maps:put(binary_to_atom(Name), Value, ConnectionData#connection.parameters)
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

handle_event({call, _}, _Request, State, #connection{} = _ConnectionData) when not is_record(State, ready) ->
    {keep_state_and_data, [postpone]};

handle_event(cast, _Notification, State, #connection{} = _ConnectionData) when not is_record(State, ready) ->
    {keep_state_and_data, [postpone]};

handle_event(info, {DataTag, Source, Data}, _, #connection{transport_tags = {Source, DataTag, _, _, _}} = ConnectionData) ->
    #connection{buffer = Buffer} = ConnectionData,
    {Messages, Rest} = pgc_messages:decode(<<Buffer/binary, Data/binary>>),
    Events = Messages ++ [{pgc_transport, set_active}],
    {keep_state, ConnectionData#connection{buffer = Rest}, [
        {next_event, internal, Event} || Event <- Events
    ]};

handle_event(info, {ClosedTag, Source}, _, #connection{transport_tags = {Source, _, ClosedTag, _, _}}) ->
    {keep_state_and_data, [
        {next_event, internal, {pgc_transport, closed}}
    ]};

handle_event(internal, {pgc_transport, set_active}, _, #connection{} = ConnectionData) ->
    #connection{transport = Transport} = ConnectionData,
    ok = pgc_transport:set_active(Transport, once),
    keep_state_and_data;

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


-spec decode_tag(unicode:unicode_binary() | undefined) -> #{command => atom(), rows => non_neg_integer()}.
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