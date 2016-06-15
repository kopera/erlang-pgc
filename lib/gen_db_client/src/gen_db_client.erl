-module(gen_db_client).
-export([
    activity/2,
    activity/3
]).
-export([
    child_spec/5
]).

-behaviour(supervisor).
-export([init/1]).


activity(Client, Fun) ->
    activity(Client, Fun, 5000).

activity(Client, Fun, Timeout) ->
    [Connections] = [Pid || {connections, Pid, _, _} <- supervisor:which_children(Client)],
    {ok, Connection} = gen_db_client_connections:checkout(Connections, Timeout),
    try Fun(Connection) of
        Result -> Result
    after
        gen_db_client_connections:checkin(Connections, Connection)
    end.


child_spec(Id, PoolOpts, QueryMod, ConnMod, ConnOpts) ->
    #{
        id => Id,
        type => supervisor,
        start => {supervisor, start_link, [?MODULE, [PoolOpts, QueryMod, ConnMod, ConnOpts]]}
    }.


%% @hidden
init([PoolOpts, QueryMod, ConnMod, ConnOpts]) ->
    put(gen_db_query, QueryMod), %% TODO: remove once gen_client is a special supervisor
    Flags = #{strategy => one_for_all},
    Children = [
        #{
            id => connections_sup,
            type => supervisor,
            start => {gen_db_client_connections_sup, start_link, [ConnMod, ConnOpts]}
        },
        #{
            id => connections,
            start => {gen_db_client_connections, start_link, [self(), PoolOpts]}
        }
    ],
    {ok, {Flags, Children}}.
