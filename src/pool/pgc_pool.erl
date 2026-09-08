-module(pgc_pool).
-export([
    start_link/2,
    start_link/3,
    child_spec/3,
    child_spec/4,
    stop/1
]).
-export([
    info/1,
    with_client/2,
    with_client/3
]).
-export_type([
    options/0,
    info/0,
    pool_ref/0,
    pool_name/0
]).

-behaviour(supervisor).
-export([
    init/1
]).


-spec start_link(pgc_client_options:t(), options()) -> {ok, pid()}.
start_link(ClientOptions, PoolOptions) ->
    {ok, _} = supervisor:start_link(?MODULE, {
        ClientOptions,
        PoolOptions
    }).


-spec start_link(pool_name(), pgc_client_options:t(), options()) -> {ok, pid()}.
-type pool_name() :: supervisor:sup_name().
-type options() :: #{
    max_size => pos_integer()
}.
start_link(Name, ClientOptions, PoolOptions) ->
    {ok, _} = supervisor:start_link(Name, ?MODULE, {
        ClientOptions,
        PoolOptions
    }).


-spec child_spec(Id, pgc_client_options:t(), options()) -> supervisor:child_spec() when
    Id :: term().
child_spec(Id, ClientOptions, PoolOptions) ->
    #{
        id => Id,
        start => {?MODULE, start_link, [ClientOptions, PoolOptions]},
        type => supervisor
    }.


-spec child_spec(Id, pool_name(), pgc_client_options:t(), options()) -> supervisor:child_spec() when
    Id :: term().
child_spec(Id, Name, ClientOptions, PoolOptions) ->
    #{
        id => Id,
        start => {?MODULE, start_link, [Name, ClientOptions, PoolOptions]},
        type => supervisor
    }.


-spec stop(pool_ref()) -> ok.
stop(PoolRef) ->
    supervisor:stop(PoolRef).


-spec info(PoolRef) -> Info when
    PoolRef :: pool_ref(),
    Info :: info().
-type info() :: #{
    available := non_neg_integer(),
    waiting := non_neg_integer(),
    used := non_neg_integer(),
    size := non_neg_integer(),
    max_size := pos_integer()
}.
info(PoolRef) ->
    ManagerRef = manager_pid(PoolRef),
    pgc_pool_manager:info(ManagerRef).


-spec with_client(PoolRef, Action) -> Result when
    PoolRef :: pool_ref(),
    Action :: fun((Connection :: pid()) -> Result).
with_client(PoolRef, Action) ->
    with_client(PoolRef, Action, #{}).


-spec with_client(PoolRef, Action, Options) -> Result when
    PoolRef :: pool_ref(),
    Action :: fun((Connection :: pid()) -> Result),
    Options :: pgc_pool_manager:checkout_options().
with_client(PoolRef, Action, Options) ->
    ManagerRef = manager_pid(PoolRef),
    case pgc_pool_manager:checkout(ManagerRef, Options) of
        {ok, ConnectionPid} ->
            try Action(ConnectionPid) of
                Result -> Result
            after
                pgc_pool_manager:checkin(ManagerRef, ConnectionPid)
            end;
        {error, timeout} ->
            erlang:error({pgc, pool_timeout}, [PoolRef, Action, Options], [{error_info,  #{
                cause => #{
                    general => "Connection checkout timed out"
                }
            }}]);
        {error, #{message := Message}} ->
            erlang:error({pgc, pool_error}, [PoolRef, Action, Options], [{error_info,  #{
                cause => #{
                    general => Message
                }
            }}])
    end.


-spec manager_pid(pool_ref()) -> pid().
-type pool_ref() :: supervisor:sup_ref().
manager_pid(PoolRef) ->
    case supervisor:which_child(PoolRef, manager) of
        {ok, {_Id, ManagerPid, _Type, _Modules}} when is_pid(ManagerPid) -> ManagerPid
    end.


% ------------------------------------------------------------------------------
% Supervisor callbacks
% ------------------------------------------------------------------------------

-doc false.
-spec init({ClientOptions, PoolOptions}) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}} when
    ClientOptions :: pgc_client_options:t(),
    PoolOptions :: options().
init({ClientOptions, PoolOptions}) ->
    Supervisor = self(),
    Flags = #{strategy => one_for_all, auto_shutdown => any_significant},
    Children = [
        #{
            id => client_sup,
            start => {pgc_pool_client_sup, start_link, [ClientOptions]},
            type => supervisor
        },
        #{
            id => manager,
            start => {pgc_pool_manager, start_link, [Supervisor, PoolOptions]},
            restart => transient,
            significant => true
        }
    ],
    {ok, {Flags, Children}}.
