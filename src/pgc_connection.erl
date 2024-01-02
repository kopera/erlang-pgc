-module(pgc_connection).
-export([
    execute/3,
    execute/4
]).
-export_type([
    options/0
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


% ------------------------------------------------------------------------------
% Data records
% ------------------------------------------------------------------------------

-record(data, {
    owner_pid :: pid(),
    owner_monitor :: reference(),

    connection :: connection() | undefined
}).

-record(connection, {
    transport :: pgc_transport:t(),
    transport_tags :: pgc_transport:tags(),

    key = {0, 0} :: {non_neg_integer(), non_neg_integer()},
    parameters = #{} :: map(),

    buffer :: binary()
}).
-type connection() :: #connection{}.

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
    statement_name :: binary(),
    statement_parameters = [] :: [pgc_type:oid()]
}).


-record(executing, {
    client_pid :: pid(),
    client_monitor :: reference(),
    execution_ref :: reference()
}).

-record(syncing, {}).

% ------------------------------------------------------------------------------
% Internal records
% ------------------------------------------------------------------------------

-record(statement, {
    name :: binary(),
    parameters :: [pgc_type:oid()],
    fields :: [#msg_row_description_field{}]
}).


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
-type execute_options() :: #{timeout => timeout()}.
-type execute_metadata() :: #{command => atom(), rows => non_neg_integer(), notices => [map()]}.
-type execute_rows() :: [map()].
-type execute_error() :: map().
execute(Connection, Statement, Params, _Options) ->
    StatementName = <<>>,
    case gen_statem:call(Connection, {prepare, StatementName, Statement}) of
        {ok, #statement{fields = Fields} = PreparedStatement} ->
            ExecutionRef = erlang:monitor(process, Connection, [
                {alias, demonitor}
            ]),
            case gen_statem:call(Connection, {execute, self(), ExecutionRef, PreparedStatement#statement.name, Params, [binary]}) of
                ok ->
                    Columns = [binary_to_atom(Field#msg_row_description_field.name, utf8) || Field <- Fields],
                    Collector = fun
                        (row, Row, {Notices, Rows}) ->
                            {Notices, [maps:from_list(lists:zip(Columns, Row)) | Rows]};
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
            end;
        {error, Error} ->
            {error, Error}
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
    {ok, _Pid} = gen_statem:start_link(?MODULE, [OwnerPid], []).


%% @private
-spec startup(Connection, Transport, ConnectionOptions) -> ok | {error, pgc_error:t()} when
    Connection :: pid(),
    Transport :: pgc_transport:t(),
    ConnectionOptions :: #{
        user := unicode:chardata(),
        password => unicode:chardata() | fun(() -> unicode:chardata()),
        database := unicode:chardata(),
        parameters => #{atom() => unicode:chardata()}
    }.
-type options() :: #{
    user := unicode:chardata(),
    password => unicode:chardata() | fun(() -> unicode:chardata()),
    database := unicode:chardata(),
    parameters => #{atom() => unicode:chardata()}
}.
startup(Connection, Transport, ConnectionOptions) ->
    gen_statem:call(Connection, {startup, Transport, ConnectionOptions}).

% ------------------------------------------------------------------------------
% gen_statem callbacks
% ------------------------------------------------------------------------------

%% @hidden
init([OwnerPid]) ->
    OwnerMonitor = erlang:monitor(process, OwnerPid),
    {ok, #disconnected{}, #data{
        owner_pid = OwnerPid,
        owner_monitor = OwnerMonitor
    }}.

%% @hidden
callback_mode() ->
    [handle_event_function, state_enter].


%% @hidden

% ------------------------------------------------------------------------------
% State: disconnected
% ------------------------------------------------------------------------------

handle_event({call, From}, {startup, Transport, ConnectionOptions}, #disconnected{}, #data{} = ConnectionData) ->
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
    }, ConnectionData#data{
        connection = #connection{
            transport = Transport,
            transport_tags = pgc_transport:get_tags(Transport),
            key = {0, 0},
            parameters = #{},
            buffer = <<>>
        }
    }};


% ------------------------------------------------------------------------------
% State: authenticating
% ------------------------------------------------------------------------------

handle_event(internal, #msg_auth{type = ok}, #authenticating{} = Authenticating, #data{} = ConnectionData) ->
    #authenticating{
        from = From
    } = Authenticating,
    {next_state, #configuring{from = From}, ConnectionData};

handle_event(internal, #msg_error_response{} = Message, #authenticating{} = Authenticating, #data{}) ->
    #authenticating{
        from = From
    } = Authenticating,
    {stop_and_reply, normal, [
        {reply, From, {error, pgc_error:from_message(Message)}}
    ]};

handle_event(internal, #msg_auth{type = AuthType, data = AuthData}, #authenticating{} = Authenticating, #data{} = ConnectionData) ->
    #data{connection = #connection{transport = Transport}} = ConnectionData,
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

% ------------------------------------------------------------------------------
% State: configuring
% ------------------------------------------------------------------------------

handle_event(internal, #msg_ready_for_query{}, #configuring{from = From}, #data{} = ConnectionData) ->
    {next_state, #ready{}, ConnectionData, [
        {reply, From, ok}
    ]};

handle_event(internal, #msg_backend_key_data{id = Id, secret = Secret}, #configuring{}, #data{connection = #connection{}} = ConnectionData) ->
    #data{connection = Connection} = ConnectionData,
    {keep_state, ConnectionData#data{
        connection = Connection#connection{
            key = {Id, Secret}
        }
    }};

handle_event(internal, #msg_parameter_status{name = Name, value = Value}, #configuring{}, #data{connection = #connection{}} = ConnectionData) ->
    #data{connection = Connection} = ConnectionData,
    {keep_state, ConnectionData#data{
        connection = Connection#connection{
            parameters = maps:put(Name, Value, Connection#connection.parameters)
        }
    }};

handle_event(internal, #msg_notice_response{}, #configuring{}, #data{}) ->
    %% TODO: log notices?
    keep_stat_and_data;

handle_event(internal, #msg_error_response{} = Message, #configuring{from = From}, #data{}) ->
    {stop_and_reply, normal, [
        {reply, From, {error, pgc_error:from_message(Message)}}
    ]};

% ------------------------------------------------------------------------------
% State: ready
% ------------------------------------------------------------------------------

handle_event(enter, _, #ready{}, _ConnectionData) ->
    {keep_state_and_data, [
        {state_timeout, 5000, idle}
    ]};

handle_event({call, From}, {prepare, StatementName, Statement}, #ready{}, #data{} = ConnectionData) ->
    #data{connection = #connection{transport = Transport}} = ConnectionData,
    ok = send(Transport, [
        #msg_parse{name = StatementName, statement = Statement},
        #msg_describe{type = statement, name = StatementName},
        #msg_sync{}
    ]),
    {next_state, #preparing{from = From, statement_name = StatementName}, ConnectionData};

handle_event({call, From}, {execute, ClientPid, ExecutionRef, StatementName, Params, Results}, #ready{}, #data{} = ConnectionData) ->
    #data{connection = #connection{transport = Transport}} = ConnectionData,
    ClientMonitor = erlang:monitor(process, ClientPid, [
        {tag, {'DOWN', client}}
    ]),
    ok = send(Transport, [
        #msg_bind{
            statement = StatementName,
            portal = <<>>,
            parameters = Params,
            results = Results
        },
        #msg_execute{portal = <<>>},
        #msg_sync{}
    ]),
    {next_state, #executing{
        client_pid = ClientPid,
        client_monitor = ClientMonitor,
        execution_ref = ExecutionRef
    }, ConnectionData, [
        {reply, From, ok}
    ]};

handle_event({call, From}, Request, #ready{}, #data{} = _ConnectionData) ->
    {keep_state_and_data, [
        {reply, From, {bad_request, Request}}
    ]};

handle_event(cast, {unprepare, StatementName}, #ready{}, #data{} = ConnectionData) ->
    #data{connection = #connection{transport = Transport}} = ConnectionData,
    ok = send(Transport, [
        #msg_close{type = statement, name = StatementName},
        #msg_sync{}
    ]),
    {next_state, #syncing{}, ConnectionData};

handle_event(cast, _, #ready{}, #data{} = _ConnectionData) ->
    keep_state_and_data;

handle_event(state_timeout, idle, #ready{}, _ConnectionData) ->
    {keep_state_and_data, [
        hibernate
    ]};

% ------------------------------------------------------------------------------
% State: preparing
% ------------------------------------------------------------------------------

handle_event(internal, #msg_parse_complete{}, #preparing{}, #data{}) ->
    keep_state_and_data;

handle_event(internal, #msg_parameter_description{types = Types}, #preparing{} = Preparing, #data{} = ConnectionData) ->
    {next_state, Preparing#preparing{statement_parameters = Types}, ConnectionData};

handle_event(internal, Message, #preparing{} = Preparing, #data{} = ConnectionData)
        when is_record(Message, msg_row_description)
            ;is_record(Message, msg_no_data) ->
    #preparing{
        from = From,
        statement_name = StatementName,
        statement_parameters = StatementParameters
    } = Preparing,
    Statement = #statement{
        name = StatementName,
        parameters = StatementParameters,
        fields = case Message of
            #msg_row_description{fields = F} -> F;
            #msg_no_data{} -> []
        end
    },
    {next_state, #syncing{}, #data{} = ConnectionData, [
        {reply, From, {ok, Statement}}
    ]};

handle_event(internal, #msg_error_response{} = Message, #preparing{} = Preparing, #data{} = ConnectionData) ->
    #preparing{from = From} = Preparing,
    {next_state, #syncing{}, ConnectionData, [
        {reply, From, {error, pgc_error:from_message(Message)}}
    ]};

% ------------------------------------------------------------------------------
% State: executing
% ------------------------------------------------------------------------------

handle_event(internal, #msg_bind_complete{}, #executing{}, #data{}) ->
    keep_state_and_data;

handle_event(internal, #msg_notice_response{fields = Notice}, #executing{} = Executing, #data{} = _ConnectionData) ->
    #executing{execution_ref = ExecutionRef} = Executing,
    ExecutionRef ! {notice, ExecutionRef, Notice},
    keep_state_and_data;

handle_event(internal, #msg_data_row{values = Row}, #executing{} = Executing, #data{} = _ConnectionData) ->
    #executing{execution_ref = ExecutionRef} = Executing,
    ExecutionRef ! {data, ExecutionRef, {row, Row}},
    keep_state_and_data;

handle_event(internal, #msg_command_complete{tag = Tag}, #executing{} = Executing, #data{} = ConnectionData) ->
    #executing{client_monitor = ClientMonitor, execution_ref = ExecutionRef} = Executing,
    ExecutionRef ! {done, ExecutionRef, {ok, Tag}},
    _ = erlang:demonitor(ClientMonitor),
    {next_state, #syncing{}, ConnectionData};

handle_event(internal, #msg_empty_query_response{}, #executing{} = Executing, #data{} = ConnectionData) ->
    #executing{client_monitor = ClientMonitor, execution_ref = ExecutionRef} = Executing,
    ExecutionRef ! {done, ExecutionRef, {ok, undefined}},
    _ = erlang:demonitor(ClientMonitor),
    {next_state, #syncing{}, ConnectionData};

handle_event(internal, #msg_error_response{} = Message, #executing{} = Executing, #data{} = ConnectionData) ->
    #executing{client_monitor = ClientMonitor, execution_ref = ExecutionRef} = Executing,
    ExecutionRef ! {done, ExecutionRef, {error, pgc_error:from_message(Message)}},
    _ = erlang:demonitor(ClientMonitor),
    {next_state, #syncing{}, ConnectionData};

handle_event(cast, {cancel, ClientMonitor}, #executing{client_monitor = ClientMonitor}, #data{} = ConnectionData) ->
    #data{connection = #connection{transport = Transport, key = BackendKey}} = ConnectionData,
    _ = erlang:demonitor(ClientMonitor),
    ok = send_cancel_request(Transport, BackendKey),
    {next_state, #syncing{}, ConnectionData};

handle_event(info, {'DOWN', ClientMonitor, process, _ClientPid, _Reason}, #executing{client_monitor = ClientMonitor}, #data{} = ConnectionData) ->
    #data{connection = #connection{transport = Transport, key = BackendKey}} = ConnectionData,
    ok = send_cancel_request(Transport, BackendKey),
    {next_state, #syncing{}, ConnectionData};


% ------------------------------------------------------------------------------
% State: syncing
% ------------------------------------------------------------------------------

handle_event(internal, Message, #syncing{}, #data{})
        when is_record(Message, msg_bind_complete)
            ;is_record(Message, msg_data_row)
            ;is_record(Message, msg_command_complete)
            ;is_record(Message, msg_empty_query_response)
            ;is_record(Message, msg_error_response) ->
    %% Can occur after cancelling a request
    keep_state_and_data;

handle_event(internal, #msg_ready_for_query{}, #syncing{}, #data{} = ConnectionData) ->
    {next_state, #ready{}, ConnectionData};

% ------------------------------------------------------------------------------
% State: *
% ------------------------------------------------------------------------------

handle_event(enter, _, _State, _ConnectionData) ->
    keep_state_and_data;

handle_event({call, _}, _Request, State, _ConnectionData) when not is_record(State, ready) ->
    {keep_state_and_data, [postpone]};

handle_event(cast, _Notification, State, _ConnectionData) when not is_record(State, ready) ->
    {keep_state_and_data, [postpone]};

handle_event(info, {DataTag, Source, Data}, _,
        #data{connection = #connection{transport_tags = {Source, DataTag, _, _, _}} = Connection} = ConnectionData) ->
    #connection{
        transport = Transport,
        buffer = Buffer
    } = Connection,
    {Messages, Rest} = pgc_messages:decode(<<Buffer/binary, Data/binary>>),
    ok = pgc_transport:set_active(Transport, once),
    {keep_state, ConnectionData#data{connection = Connection#connection{buffer = Rest}}, [
        {next_event, internal, Message} || Message <- Messages
    ]};

handle_event(info, {ClosedTag, Source}, _,
        #data{connection = #connection{transport_tags = {Source, _, ClosedTag, _, _}}} = ConnectionData) ->
    #data{owner_pid = OwnerPid} = ConnectionData,
    %% FIXME: based on the state maybe reply instead of notifying the owner
    OwnerPid ! {?MODULE, self(), closed},
    {stop, normal};

handle_event(info, {'DOWN', OwnerMonitor, process, OwnerPid, _Reason}, _State, #data{owner_pid = OwnerPid, owner_monitor = OwnerMonitor}) ->
    {stop, normal};

handle_event(EventType, EventContent, State, _Data) ->
    ?LOG_WARNING(#{
        label => {?MODULE, unhandled_event},
        state => State,
        event_type => EventType,
        event_content => EventContent
    }),
    keep_state_and_data.


%% @hidden
terminate(_Reason, _State, #data{connection = undefined}) ->
    ok;

terminate(_Reason, _State, #data{connection = #connection{transport = Transport}}) ->
    pgc_transport:close(Transport).


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------


-spec send(pgc_transport:t(), [Message] | Message) -> ok | {error, send_error()} when
    Message :: pgc_message:message_f() | pgc_message:message_fb().
-type send_error() :: pgc_transport:send_error().
send(Transport, Messages) when is_list(Messages) ->
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