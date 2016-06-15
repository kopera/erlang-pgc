-module(client_SUITE).

-include_lib("common_test/include/ct.hrl").
-export([
	all/0,
	init_per_suite/1,
	end_per_suite/1
]).

all() ->
	[].

init_per_suite(Config) ->
	application:load(sasl),
	application:set_env(sasl, errlog_type, error),
	{ok, _} = application:ensure_all_started(sasl),
	{ok, _} = application:ensure_all_started(pgsql),
	Config.

end_per_suite(_Config) ->
	ok = application:stop(pgsql),
	ok.
