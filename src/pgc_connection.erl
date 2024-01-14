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
    open/3,
    close/1
]).


-spec execute(Connection, Statement, Parameters) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: pid(),
    Statement :: unicode:chardata(),
    Parameters :: [term()],
    Metadata :: execute_metadata(),
    Rows :: execute_rows(),
    Error :: execute_error().
execute(Connection, Statement, Parameters) ->
    execute(Connection, Statement, Parameters, #{}).


-spec execute(Connection, Statement, Parameters, Options) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: pid(),
    Statement :: unicode:chardata(),
    Parameters :: [term()],
    Options :: execute_options(),
    Metadata :: execute_metadata(),
    Rows :: execute_rows(),
    Error :: execute_error().
-type execute_options() :: #{
    cache => false | {true, atom()},
    row => map | tuple | list | proplist
}.
-type execute_metadata() :: #{
    command := atom(),
    columns := [atom()],
    rows => non_neg_integer(),
    notices := [map()]
}.
-type execute_rows() :: [term()].
-type execute_error() :: map().
execute(Connection, StatementText, Parameters, Options) ->
    StatementName = case Options of
        #{cache := {true, Key}} -> atom_to_binary(Key, utf8);
        % #{cache := true} -> binary:encode_hex(crypto:hash(sha, StatementText));
        #{} -> <<>>
    end,
    case prepare_(Connection, StatementName, StatementText) of
        {ok, PreparedStatement} ->
            execute_(Connection, PreparedStatement, Parameters, Options);
        {error, _} = Error ->
            Error
    end.


%% @private
prepare_(Connection, StatementName, StatementText) ->
    StatementBinary = unicode:characters_to_binary(StatementText),
    StatementHash = crypto:hash(sha256, StatementBinary),
    try gen_statem:call(Connection, {prepare, StatementName, StatementHash, StatementBinary}) of
        Result -> Result
    catch
        exit:{noproc, _} -> exit(noproc)
    end.


%% @private
execute_(Connection, Statement, Parameters, Options) ->
    ExecutionRef = erlang:monitor(process, Connection, [{alias, demonitor}]),
    try gen_statem:call(Connection, {execute, self(), ExecutionRef, Statement, Parameters}) of
        {ok, RowColumns} ->
            RowFormat = maps:get(row, Options, map),
            Collector = fun
                (row, RowValues, {Notices, Rows}) ->
                    Row = case RowFormat of
                        map -> maps:from_list(lists:zip(RowColumns, RowValues));
                        tuple -> list_to_tuple(RowValues);
                        list -> RowValues;
                        proplist -> lists:zip(RowColumns, RowValues)
                    end,
                    {Notices, [Row | Rows]};
                (notice, Notice, {Notices, Rows}) ->
                    {[Notice | Notices], Rows}
            end,
            case execute_collect(Collector, ExecutionRef, {[], []}) of
                {ok, Metadata, {Notices, Rows}} ->
                    {ok, Metadata#{columns => RowColumns, notices => lists:reverse(Notices)}, lists:reverse(Rows)};
                {error, Error, {_Notices, _Rows}} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    catch
        exit:{noproc, _} ->
            exit(noproc)
    after
        erlang:demonitor(ExecutionRef, [flush])
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
    {ok, _Pid} = gen_statem:start_link(pgc_connection_statem, OwnerPid, [
        {hibernate_after, 5000}
    ]).


%% @private
-spec open(Connection, Transport, Options) -> ok | {error, pgc_error:t()} when
    Connection :: pid(),
    Transport :: pgc_transport:t(),
    Options :: options().
-type options() :: #{
    user := unicode:chardata(),
    password => unicode:chardata() | fun(() -> unicode:chardata()),
    database := unicode:chardata(),
    parameters => parameters()
}.
-type parameters() :: #{atom() => unicode:chardata()}.
open(Connection, Transport, Options) ->
    gen_statem:call(Connection, {open, Transport, Options}).

%% @private
-spec close(Connection) -> ok when
    Connection :: pid().
close(Connection) ->
    gen_statem:stop(Connection).


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

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