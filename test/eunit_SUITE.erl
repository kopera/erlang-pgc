-module(eunit_SUITE).

-include_lib("common_test/include/ct.hrl").
-export([
	all/0
]).
-export([
	eunit/1
]).

all() ->
	[eunit].

eunit(_) ->
	ok = eunit:test({application, pgsql}).
