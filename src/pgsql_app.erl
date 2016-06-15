%% @private
-module(pgsql_app).

-behaviour(application).
-export([
    start/2,
    stop/1
]).

%% @hidden
start(_StartType, _StartArgs) ->
    pgsql_sup:start_link().

%% @hidden
stop(_State) ->
    ok.
