-module(gen_db_client_connections_sup).
-export([
    start_link/2
]).

-behaviour(supervisor).
-export([
    init/1
]).

-spec start_link(module(), any()) -> {ok, Pid :: pid()}.
start_link(ConnMod, ConnOpts) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [ConnMod, ConnOpts]).


%% @hidden
init([ConnMod, ConnOpts]) ->
    Flags = #{strategy => simple_one_for_one},
    Children = [
        #{
            id => connection,
            start => {gen_db_client_connection, start_link, [ConnMod, ConnOpts]},
            restart => temporary
        }
    ],
    {ok, {Flags, Children}}.
