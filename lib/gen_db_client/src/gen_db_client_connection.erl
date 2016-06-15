-module(gen_db_client_connection).
-export([
    prepare/3,
    unprepare/3,
    execute/4,
    query/4,
    transaction/3
]).

-export([
    start_link/2,
    stop/1,
    checkout/2,
    checkin/2
]).

-behaviour(gen_connection).
-export([
    init/1,
    connect/2,
    disconnect/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
]).

-export_type([connection/0]).

-callback connect(Opts :: any()) ->
    {ok, State :: any()} |
    {error, Reason :: any()}.

-callback checkout(User :: pid(), State :: any()) ->
    {ok, NewState :: any()} |
    {disconnect, Info :: any(), NewState :: any()}.

-callback checkin(User :: pid(), State :: any()) ->
    {ok, NewState :: any()} |
    {disconnect, Info :: any(), NewState :: any()}.

-callback ping(State :: any()) ->
    {ok, NewState :: any()} |
    {disconnect, Info :: any(), NewState :: any()}.

-callback handle_begin(Opts :: map(), State :: any()) ->
    {ok, Result :: any(), NewState :: any()} |
    {error | disconnect, Info :: any(), NewState :: any()}.

-callback handle_commit(Opts :: map(), State :: any()) ->
    {ok, Result :: any(), NewState :: any()} |
    {error | disconnect, Info :: any(), NewState :: any()}.

-callback handle_rollback(Opts :: map(), State :: any()) ->
    {ok, Result :: any(), NewState :: any()} |
    {error | disconnect, Info :: any(), NewState :: any()}.

-callback handle_prepare(Query :: any(), Opts :: map(), State :: any()) ->
    {ok, Query :: any(), NewState :: any()} |
    {error | disconnect, Info :: any(), NewState :: any()}.

-callback handle_execute(PreparedQuery :: any(), Params :: any(), Opts :: map(), State :: any()) ->
    {ok, Result :: any(), NewState :: any()} |
    {error | disconnect, Info :: any(), NewState :: any()}.

-callback handle_query(Query :: any(), Params :: any(), Opts :: map(), State :: any()) ->
    {ok, Result :: any(), NewState :: any()} |
    {error | disconnect, Info :: any(), NewState :: any()}.

-callback handle_unprepare(PreparedQuery :: any(), Opts :: map(), State :: any()) ->
    {ok, Result :: any(), NewState :: any()} |
    {error | disconnect, Info :: any(), NewState :: any()}.

-callback handle_info(Msg :: any(), State :: any()) ->
    {ok, NewState :: any()} |
    {disconnect, Info :: any(), NewState :: any()}.

-callback disconnect(Info :: any(), State :: any()) ->
    ok.


-record(connection, {mod :: module(),
                     mod_state :: any() | undefined,
                     connect_opts :: any()}).


%% @private
-spec start_link(Mod, ConnectOpts) -> {ok, Conn} | {error, Reason} when
      Mod :: module(),
      ConnectOpts :: any(),
      Conn :: connection(),
      Reason :: any().
-opaque connection() :: pid().
start_link(Mod, ConnectOpts) ->
    gen_connection:start_link(?MODULE, {Mod, ConnectOpts}, []).

%% @private
-spec stop(connection()) -> ok.
stop(Conn) ->
    gen_connection:stop(Conn).

%% @private
-spec checkout(connection(), pid()) -> ok.
checkout(Conn, User) ->
    gen_connection:cast(Conn, {checkout, User}).

%% @private
-spec checkin(connection(), pid()) -> ok.
checkin(Conn, User) ->
    gen_connection:cast(Conn, {checkout, User}).

prepare(Conn, Query, Opts) ->
    gen_connection:call(Conn, {prepare, Query, Opts}, infinity).

unprepare(Conn, PreparedQuery, Opts) ->
    gen_connection:cast(Conn, {unprepare, PreparedQuery, Opts}).

execute(Conn, PreparedQuery, Params, Opts) ->
    gen_connection:call(Conn, {execute, PreparedQuery, Params, Opts}, infinity).

query(Conn, Query, Params, Opts) ->
    gen_connection:call(Conn, {query, Query, Params, Opts}, infinity).

transaction(Conn, Fun, Opts) ->
    {ok, _} = gen_connection:call(Conn, {'begin', Opts}, infinity),
    try
        R = Fun(Conn),
        {ok, _} = gen_connection:call(Conn, {commit, Opts}, infinity),
        R
    catch
        throw:Error ->
            {ok, _} = gen_connection:call(Conn, {rollback, Opts}, infinity),
            Error;
        Class:Error ->
            {ok, _} = gen_connection:call(Conn, {rollback, Opts}, infinity),
            erlang:raise(Class, Error, erlang:get_stacktrace())
    end.



%% @hidden
init({Mod, ConnectOpts}) ->
    {connect, initial, #connection{mod = Mod, connect_opts = ConnectOpts}}.

%% @hidden
connect(_Info, #connection{mod = Mod, connect_opts = ConnectOpts} = Connection) ->
    try Mod:connect(ConnectOpts) of
        Result -> handle_connect_result(Result, Connection)
    catch
        throw:Result -> handle_connect_result(Result, Connection)
    end.

%% @hidden
disconnect(_Info, #connection{mod = Mod, mod_state = State} = Connection) ->
    _ = Mod:disconnect(State),
    {connect, reconnect, Connection#connection{mod_state = undefined}}.

%% @hidden
handle_call({prepare, Query, Opts}, _From, #connection{mod = Mod, mod_state = State} = Connection) ->
    try Mod:handle_prepare(Query, Opts, State) of
        Result ->
            handle_query_result(Result, Connection)
    catch
        throw:Result ->
            handle_query_result(Result, Connection)
    end;

handle_call({execute, PreparedQuery, Params, Opts}, _From, #connection{mod = Mod, mod_state = State} = Connection) ->
    try Mod:handle_execute(PreparedQuery, Params, Opts, State) of
        Result ->
            handle_query_result(Result, Connection)
    catch
        throw:Result ->
            handle_query_result(Result, Connection)
    end;

handle_call({query, Query, Params, Opts}, _From, #connection{mod = Mod, mod_state = State} = Connection) ->
    try Mod:handle_query(Query, Params, Opts, State) of
        Result ->
            handle_query_result(Result, Connection)
    catch
        throw:Result ->
            handle_query_result(Result, Connection)
    end;

handle_call({'begin', Opts}, _From, #connection{mod = Mod, mod_state = State} = Connection) ->
    try Mod:handle_begin(Opts, State) of
        Result ->
            handle_query_result(Result, Connection)
    catch
        throw:Result ->
            handle_query_result(Result, Connection)
    end;

handle_call({commit, Opts}, _From, #connection{mod = Mod, mod_state = State} = Connection) ->
    try Mod:handle_commit(Opts, State) of
        Result ->
            handle_query_result(Result, Connection)
    catch
        throw:Result ->
            handle_query_result(Result, Connection)
    end;

handle_call({rollback, Opts}, _From, #connection{mod = Mod, mod_state = State} = Connection) ->
    try Mod:handle_rollback(Opts, State) of
        Result ->
            handle_query_result(Result, Connection)
    catch
        throw:Result ->
            handle_query_result(Result, Connection)
    end;

handle_call(Req, _From, Connection) ->
    {reply, {unhandled_request, Req}, Connection}.

%% @hidden
handle_cast({checkout, User}, #connection{mod = Mod, mod_state = State} = Connection) ->
    try Mod:checkout(User, State) of
        Result -> handle_notification_result(Result, Connection)
    catch
        throw:Result -> handle_notification_result(Result, Connection)
    end;

handle_cast({checkin, User}, #connection{mod = Mod, mod_state = State} = Connection) ->
    try Mod:checkin(User, State) of
        Result -> handle_notification_result(Result, Connection)
    catch
        throw:Result -> handle_notification_result(Result, Connection)
    end;

handle_cast({unprepare, PreparedQuery, Opts}, #connection{mod = Mod, mod_state = State} = Connection) ->
    try Mod:unprepare(PreparedQuery, Opts, State) of
        Result -> handle_async_query_result(Result, Connection)
    catch
        throw:Result -> handle_async_query_result(Result, Connection)
    end;

handle_cast(_, Connection) ->
    {noreply, Connection}.

%% @hidden
handle_info(Msg, #connection{mod = Mod, mod_state = State} = Connection) ->
    try Mod:handle_info(Msg, State) of
        Result -> handle_notification_result(Result, Connection)
    catch
        throw:Result -> handle_notification_result(Result, Connection)
    end.

%% @hidden
code_change(_, State, _Extra) ->
    {ok, State}.

%% @hidden
terminate(_, _) ->
    ok.


%% Internal

handle_connect_result({ok, State}, Connection) ->
    {ok, Connection#connection{mod_state = State}};
handle_connect_result({error, _Reason}, Connection) ->
    {backoff, 1000, Connection};
handle_connect_result(Other, Connection) ->
    {stop, {bad_return_value, Other}, Connection}.

handle_notification_result({ok, NewState}, Connection) ->
    {ok, Connection#connection{mod_state = NewState}};
handle_notification_result({disconnect, Info, NewState}, Connection) ->
    {disconnect, Info, Connection#connection{mod_state = NewState}};
handle_notification_result(Other, Connection) ->
    {stop, {bad_return_value, Other}, Connection}.

handle_query_result({ok, Reply, NewState}, Connection) ->
    {reply, {ok, Reply}, Connection#connection{mod_state = NewState}};
handle_query_result({error, Info, NewState}, Connection) ->
    {reply, {error, Info}, Connection#connection{mod_state = NewState}};
handle_query_result({disconnect, Info, NewState}, Connection) ->
    {disconnect, Info, {error, Info}, Connection#connection{mod_state = NewState}};
handle_query_result(Other, Connection) ->
    {stop, {bad_return_value, Other}, Connection}.

handle_async_query_result({ok, NewState}, Connection) ->
    {noreply, Connection#connection{mod_state = NewState}};
handle_async_query_result({error, _Info, NewState}, Connection) ->
    {noreply, Connection#connection{mod_state = NewState}};
handle_async_query_result({disconnect, Info, NewState}, Connection) ->
    {disconnect, Info, Connection#connection{mod_state = NewState}};
handle_async_query_result(Other, Connection) ->
    {stop, {bad_return_value, Other}, Connection}.

