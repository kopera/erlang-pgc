-module(pgc_connection).
-export([
    start_link/3,
    start_link/4,
    stop/1,
    call/3,
    cast/2
]).
-export_type([
    start_options/0,
    start_ret/0,
    connection_name/0,
    connection_ref/0,
    connection_info/0
]).

-define(DEFAULT_PING_INTERVAL, 5000).
-define(DEFAULT_HIBERNATE_AFTER, ?DEFAULT_PING_INTERVAL div 2).
-define(DEFAULT_CONNECT_TIMEOUT, 5000).

% -----------------------------------------------------------------------------
% Callbacks
% -----------------------------------------------------------------------------

-doc """
Called once, from inside `c:init/1`'s caller process, with the
`Args` from the `handler` start option.
""".
-callback init(Args :: term()) -> {ok, State :: term()}.


-doc """
The connection reached `ReadyForQuery`.
""".
-callback handle_ready(ConnectionInfo, State) -> {[action()], State} when
    ConnectionInfo :: connection_info(),
    State :: term().

-callback handle_row_data(ConnectionInfo, RowDescription, RowData, State) -> {[action()], State} when
    ConnectionInfo :: connection_info(),
    RowDescription :: [pgc_protocol_message:row_description_field()],
    RowData :: [null | binary()],
    State :: term().

-callback handle_result(ConnectionInfo, Result, State) -> {[action()], State} when
    ConnectionInfo :: connection_info(),
    Result :: {ok, Tag :: binary() | empty} | {error, pgc_protocol_message:error_response_fields()},
    State :: term().

-doc """
A `NoticeResponse` was received.
""".
-callback handle_notice(ConnectionInfo, Notice, State) -> {[action()], State} when
    ConnectionInfo :: connection_info(),
    Notice :: pgc_protocol_message:notice_response_fields(),
    State :: term().

-doc """
A `NotificationResponse` (the result of some session's `NOTIFY`) was received.
""".
-callback handle_notification(ConnectionInfo, SenderId, Channel, Payload, State) -> {[action()], State} when
    ConnectionInfo :: connection_info(),
    SenderId :: non_neg_integer(),
    Channel :: binary(),
    Payload :: binary(),
    State :: term().


-callback handle_call(ConnectionInfo, Request :: term(), gen_statem:from(), State) -> {[action()], State} when
    ConnectionInfo :: connection_info(),
    State :: term().

-callback handle_cast(ConnectionInfo, Request :: term(), State) -> {[action()], State} when
    ConnectionInfo :: connection_info(),
    State :: term().

-callback handle_info(ConnectionInfo, Info :: term(), State) -> {[action()], State} when
    ConnectionInfo :: connection_info(),
    State :: term().

-doc """
The connection is about to stop. No further callbacks follow.
""".
-callback terminate(Reason, State) -> ok when
    Reason :: pgc_connection_statem_termination:reason() | term(),
    State :: term().

-optional_callbacks([
    handle_ready/2,
    handle_row_data/4,
    handle_result/3,
    handle_notice/3,
    handle_notification/5,
    handle_call/4,
    handle_cast/3,
    handle_info/3,
    terminate/2
]).


% -----------------------------------------------------------------------------
% Types
% -----------------------------------------------------------------------------

-type start_options() :: #{
    address := pgc_transport:address(),
    tls => disable | prefer | require,
    tls_options => [ssl:tls_client_option()],
    connect_timeout => timeout(),
    ping_interval => timeout(),

    user := unicode:chardata(),
    password => unicode:chardata() | fun(() -> unicode:chardata()),
    database := unicode:chardata(),
    parameters => #{
        atom() => unicode:chardata()
    }
}.

-type start_ret() :: gen_statem:start_ret().
-type connection_name() :: gen_statem:server_name().
-type connection_ref() :: gen_statem:server_ref().
-type connection_info() :: #{
    % phase := connection_phase(),
    % backend_key := {non_neg_integer(), binary()} | undefined,
    parameters := #{binary() => binary()}
}.

-type action() ::
    {query, Text :: unicode:chardata()}
    % | {query, Text :: unicode:chardata(), Params :: [iodata() | null], Options :: query_options()}
    | {reply, gen_statem:from(), Reply :: term()}.
% -type query_options() :: #{}.


% -----------------------------------------------------------------------------
% API
% -----------------------------------------------------------------------------

-doc """
Starts a new connection process, linked to the caller.
""".
-spec start_link(module(), term(), start_options()) -> start_ret().
start_link(Module, Args, Options) ->
    {StartArgs, StartOptions} = start_params(Module, Args, Options),
    gen_statem:start_link(pgc_connection_statem, StartArgs, StartOptions).


-doc """
Starts a new named connection process, linked to the caller.
""".
-spec start_link(connection_name(), module(), term(), start_options()) -> start_ret().
start_link(ConnectionName, Module, Args, Options) ->
    {StartArgs, StartOptions} = start_params(Module, Args, Options),
    gen_statem:start_link(ConnectionName, pgc_connection_statem, StartArgs, StartOptions).


-spec stop(connection_ref()) -> ok.
stop(ConnectionRef) ->
    gen_statem:stop(ConnectionRef).


-spec call(connection_ref(), term(), timeout()) -> term().
call(ConnectionRef, Request, Timeout) ->
    gen_statem:call(ConnectionRef, Request, Timeout).


-spec cast(connection_ref(), term()) -> ok.
cast(ConnectionRef, Request) ->
    gen_statem:cast(ConnectionRef, Request).


% -----------------------------------------------------------------------------
% Helpers
% -----------------------------------------------------------------------------

start_params(HandlerModule, HandlerArgs, Options) ->
    PingInterval = maps:get(ping_interval, Options, ?DEFAULT_PING_INTERVAL),
    %% Args
    StartArgs = #pgc_connection_statem:args{
        address = maps:get(address, Options),

        connect_options = maps:with([tls, tls_options], Options),
        connect_timeout = maps:get(connect_timeout, Options, ?DEFAULT_CONNECT_TIMEOUT),
        ping_interval = PingInterval,

        user = maps:get(user, Options),
        password = case Options of
            #{password := Password} when is_function(Password, 0) -> Password;
            #{password := Password} -> fun () -> Password end;
            #{} -> fun () -> <<>> end
        end,
        database = maps:get(database, Options),
        parameters = maps:get(parameters, Options, #{}),

        handler_module = HandlerModule,
        handler_args = HandlerArgs
    },
    %% Options
    %%
    HibernateAfter = case PingInterval of
        infinity -> ?DEFAULT_HIBERNATE_AFTER;
        _ -> PingInterval div 2
    end,
    StartOptions = [
        {hibernate_after, HibernateAfter}
    ],
    {StartArgs, StartOptions}.
