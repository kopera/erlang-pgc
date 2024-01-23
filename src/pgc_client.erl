%% @private
-module(pgc_client).
-export([
    execute/3,
    execute/4,
    transaction/3,
    rollback/2
]).
-export([
    format_error/2
]).

-spec execute(Connection, Statement, Parameters) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: pid(),
    Statement :: unicode:chardata(),
    Parameters :: [term()],
    Metadata :: pgc:execute_metadata(),
    Rows :: [term()],
    Error :: pgc_error:t().
execute(Connection, StatementText, Parameters) ->
    execute(Connection, StatementText, Parameters, #{}).


-spec execute(Connection, Statement, Parameters, Options) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: pid(),
    Statement :: unicode:chardata(),
    Parameters :: [term()],
    Options :: pgc:execute_options(),
    Metadata :: pgc:execute_metadata(),
    Rows :: [term()],
    Error :: pgc_error:t().
execute(Connection, StatementText, Parameters, Options) ->
    Deadline = pgc_deadline:from_timeout(maps:get(timeout, Options, infinity)),
    StatementName = case Options of
        #{cache := {true, Key}} -> atom_to_binary(Key, utf8);
        % #{cache := true} -> binary:encode_hex(crypto:hash(sha, Statement));
        #{} -> <<>>
    end,
    case prepare_(Connection, StatementName, StatementText, Deadline) of
        {ok, PreparedStatement} ->
            RowFormat = maps:get(row, Options, map),
            execute_(Connection, PreparedStatement, Parameters, Deadline, fun (RowColumns, RowValues) ->
                case RowFormat of
                    map -> maps:from_list(lists:zip(RowColumns, RowValues));
                    tuple -> list_to_tuple(RowValues);
                    list -> RowValues;
                    proplist -> lists:zip(RowColumns, RowValues)
                end
            end);
        {error, #{} = Error} ->
            {error, Error}
    end.


%% @private
-spec execute_simple(Connection, Statement, Options) -> {ok, Metadata} | {error, Error} when
    Connection :: pid(),
    Statement :: unicode:chardata(),
    Options :: pgc:execute_options(),
    Metadata :: pgc:execute_metadata(),
    Error :: pgc_error:t().
execute_simple(Connection, Statement, Options) when is_binary(Statement); is_list(Statement) ->
    Deadline = pgc_deadline:from_timeout(maps:get(timeout, Options, infinity)),
    ExecuteReqId = gen_statem:send_request(Connection, {execute, Statement}),
    case gen_statem:receive_response(ExecuteReqId, pgc_deadline:to_abs_timeout(Deadline)) of
        {reply, {ok, Tag}} ->
            Metadata = decode_tag(Tag),
            {ok, Metadata};
        {reply, {error, #{} = Error}} ->
            % eqwalizer:ignore
            {error, Error};
        {error, {noproc, _}} ->
            exit(noproc);
        timeout ->
            {error, pgc_error:statement_timeout()}
    end.


-spec transaction(Connection, Fun, Options) -> {ok, Result} | {error, dynamic() | pgc_error:t()} when
    Connection :: pid(),
    Fun :: fun(() -> Result),
    Options :: transaction_options().
-type transaction_options() :: #{
    isolation => serializable | repeatable_read | read_committed | read_uncommitted | default,
    access => read_write | read_only | default,
    deferrable => boolean() | default
}.
transaction(Connection, Fun, Options) ->
    case transaction_start(Connection, Options) of
        ok ->
            try Fun() of
                Result ->
                    case transaction_commit(Connection) of
                        ok ->
                            {ok, Result};
                        {error, Reason} ->
                            transaction_rollback(Connection),
                            {error, Reason}
                    end
            catch
                throw:{?MODULE, rollback, Connection, Reason} ->
                    transaction_rollback(Connection),
                    {error, Reason};
                Class:Error:Stacktrace ->
                    try
                        transaction_rollback(Connection)
                    after
                        erlang:raise(Class, Error, Stacktrace)
                    end
            end;
        {error, Error} ->
            {error, Error}
    end.


-spec rollback(Connection, Reason) -> no_return() when
    Connection :: pid(),
    Reason :: dynamic().
rollback(Connection, Reason) ->
    throw({?MODULE, rollback, Connection, Reason}).


%% @private
-spec prepare_(Connection, StatementName, StatementText, Deadline) -> {ok, pgc_connection:statement()} | {error, Error} when
    Connection :: pid(),
    StatementName :: unicode:unicode_binary(),
    StatementText :: unicode:chardata(),
    Deadline :: pgc_deadline:t(),
    Error :: pgc_error:t().
prepare_(Connection, StatementName, StatementText, Deadline) ->
    PrepareReqId = gen_statem:send_request(Connection, {prepare, StatementName, StatementText, #{}}),
    case gen_statem:receive_response(PrepareReqId, pgc_deadline:to_abs_timeout(Deadline)) of
        {reply, {ok, PreparedStatement}} ->
            % eqwalizer:ignore
            {ok, PreparedStatement};
        {reply, {error, #{} = Error}} ->
            % eqwalizer:ignore
            {error, Error};
        {error, {noproc, _}} ->
            exit(noproc);
        timeout ->
            gen_statem:cast(Connection, {unprepare, StatementName}),
            {error, pgc_error:statement_timeout()}
    end.


%% @private
-spec execute_(Connection, Statement, Parameters, Deadline, RowFun) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: pid(),
    Statement :: pgc_connection:statement(),
    Parameters :: [term()],
    Deadline :: pgc_deadline:t(),
    RowFun :: fun((RowColumns :: [atom()], RowValues :: [dynamic()]) -> dynamic()),
    Metadata :: pgc:execute_metadata(),
    Rows :: [term()],
    Error :: pgc_error:t().
execute_(Connection, Statement, Parameters, Deadline, RowFun) ->
    ExecutionRef = erlang:monitor(process, Connection, [{alias, demonitor}]),
    ExecuteReqId = gen_statem:send_request(Connection, {execute, self(), ExecutionRef, Statement, Parameters}),
    try gen_statem:receive_response(ExecuteReqId, pgc_deadline:to_abs_timeout(Deadline)) of
        {reply, {ok, RowColumns}} when is_list(RowColumns) ->
            Collector = fun
                (row, RowValues, {Notices, Rows}) ->
                    %% eqwalizer:ignore
                    {Notices, [RowFun(RowColumns, RowValues) | Rows]};
                (notice, Notice, {Notices, Rows}) ->
                    {[Notice | Notices], Rows}
            end,
            DeadlineTimerRef = pgc_deadline:start_timer(self(), {timeout, ExecutionRef}, Deadline),
            try execute_collect(Collector, ExecutionRef, {[], []}) of
                {ok, Metadata, {Notices, Rows}} ->
                    {ok, Metadata#{columns => RowColumns, notices => lists:reverse(Notices)}, lists:reverse(Rows)};
                {error, Error, {_Notices, _Rows}} ->
                    {error, Error};
                timeout ->
                    gen_statem:cast(Connection, {cancel, ExecutionRef}),
                    {error, pgc_error:statement_timeout()}
            after
                pgc_deadline:cancel_timer(DeadlineTimerRef)
            end;
        {reply, {error, Error}} ->
            %% eqwalizer:ignore
            {error, Error};
        {error, {noproc, _}} ->
            exit(noproc);
        timeout ->
            gen_statem:cast(Connection, {cancel, ExecutionRef}),
            {error, pgc_error:statement_timeout()}
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
        {timeout, ExecutionRef} ->
            timeout;
        {'DOWN', ExecutionRef, _, _, Reason} ->
            exit(Reason)
    end.

%% @private
transaction_start(Connection, Options) ->
    StatementText = [
        <<"start transaction">>,
        case maps:get(isolation, Options, default) of
            serializable -> <<" isolation level serializable">>;
            repeatable_read -> <<" isolation level repeatable read">>;
            read_committed -> <<" isolation level read committed">>;
            read_uncommitted -> <<" isolation level read uncommitted">>;
            default -> <<>>;
            Other -> erlang:error(badarg, [Options], [{error_info,  #{
                cause => #{
                    1 => io_lib:format("invalid isolation level: ~p", [Other])
                }
            }}])
        end,
        case maps:get(access, Options, default) of
            read_write -> <<" read write">>;
            read_only -> <<" read only">>;
            default -> <<>>;
            Other -> erlang:error(badarg, [Options], [{error_info,  #{
                cause => #{
                    1 => io_lib:format("invalid access mode: ~p", [Other])
                }
            }}])
        end,
        case maps:get(deferrable, Options, default) of
            true -> <<" deferrable">>;
            false -> <<" not deferrable">>;
            default -> <<>>;
            Other -> erlang:error(badarg, [Options], [{error_info,  #{
                cause => #{
                    1 => io_lib:format("invalid deferrable option: ~p", [Other])
                }
            }}])
        end
    ],
    case execute_simple(Connection, StatementText, #{}) of
        {ok, #{command := start_transaction}} ->
            ok;
        {error, #{} = Error} ->
            {error, Error}
    end.

%% @private
transaction_commit(Connection) ->
    case execute_simple(Connection, <<"commit">>, #{}) of
        {ok, #{command := commit}} ->
            ok;
        {error, #{} = Error} ->
            {error, Error}
    end.

%% @private
transaction_rollback(Connection) ->
    case execute_simple(Connection, <<"rollback">>, #{}) of
        {ok, #{command := rollback}} ->
            ok;
        {error, #{} = Error} ->
            {error, Error}
    end.

%% @private
format_error(_Reason, [{_M, _F, _As, Info}|_]) ->
    ErrorInfo = proplists:get_value(error_info, Info, #{}),
    maps:get(cause, ErrorInfo).


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

%% @private
-spec decode_tag(undefined) -> #{};
                (unicode:unicode_binary()) -> #{command := atom(), rows => non_neg_integer()}.
decode_tag(undefined) ->
    #{};

decode_tag(<<"START TRANSACTION">>) ->
    #{command => start_transaction};
decode_tag(<<"PREPARE TRANSACTION">>) ->
    #{command => prepare_transaction};
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
