-module(pgc_client).
-export([
    start_link/1,
    start_link/2,
    stop/1
]).
-export([
    execute/3,
    execute/4,
    execute/6,
    transaction/3,
    rollback/2
]).
-export_type([
    parameters/0,
    transaction_options/0
]).

-behaviour(pgc_connection).
-export([
    init/1,
    handle_cast/3,
    handle_prepare_result/3,
    handle_query_result/3,
    handle_execute_result/3,
    handle_row_data/4
]).

-import_record(pgc_protocol_message, [row_description_field]).

-record #req{
    ref :: reference(),
    statement_text :: unicode:chardata(),
    parameters :: pgc_connection_statem_extended_query:execute_parameters()
}.

-record #state{
    types :: ets:table(),
    refreshing_types :: boolean(),
    pending :: [#req{}]
}.

% -----------------------------------------------------------------------------
% API
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
        replication => none(),
        atom() => unicode:chardata()
    }
}.
-spec start_link(start_options()) -> pgc_connection:start_ret().
start_link(Options) ->
    pgc_connection:start_link(?MODULE, [], Options).


-spec start_link(pgc_connection:connection_name(), start_options()) -> pgc_connection:start_ret().
start_link(ClientName, Options) ->
    pgc_connection:start_link(ClientName, ?MODULE, [], Options).


-spec stop(pgc_connection:connection_ref()) -> ok.
stop(ConnectionRef) ->
    pgc_connection:stop(ConnectionRef).


-spec execute(Connection, StatementText, Parameters) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: pgc_connection:connection_ref(),
    StatementText :: unicode:chardata(),
    Parameters :: parameters(),
    Metadata :: result_metadata(),
    Rows :: result_rows(),
    Error :: execute_error().
execute(Connection, StatementText, Parameters) ->
    execute(Connection, StatementText, Parameters, #{}).


-doc """
Runs a parameterized statement through parse/bind/execute, collecting every row into a
list shaped per `Options`' `row` (`map` by default).

`Options`' `timeout`, if given, bounds only the wait for a reply -- on expiry, the query
is cancelled on the server (a `CancelRequest`, per the wire protocol) rather than left to
run to completion unattended, and this exits the same way a timed-out `gen_statem:call/3`
would (`exit({timeout, _})`).
""".
-spec execute(Connection, StatementText, Parameters, Options) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: pgc_connection:connection_ref(),
    StatementText :: unicode:chardata(),
    Parameters :: parameters(),
    Options :: #{
        row => map | list | tuple | proplist,
        timeout => timeout()
    },
    Metadata :: result_metadata(),
    Rows :: result_rows(),
    Error :: execute_error().
-type parameters() :: [iodata() | null].
-type result_metadata() :: #{
    command := binary(),
    rows => non_neg_integer()
}.
-type result_rows() :: [map() | list() | tuple()].
-type execute_error() :: pgc_protocol_message:error_response_fields().
execute(Connection, StatementText, Parameters, Options) ->
    RowFormat = maps:get(row, Options, map),
    RemainingOptions = maps:without([row], Options),
    case execute(Connection, StatementText, Parameters, fun (RowDescription, Values, Acc) ->
        {cont, [format_row(RowFormat, RowDescription, Values) | Acc]}
    end, [], RemainingOptions) of
        {ok, Metadata, Rows} ->
            {ok, Metadata, lists:reverse(Rows)};
        {error, _} = Error -> Error
    end.


-doc """
Runs a parameterized statement through parse/bind/execute, folding `Fun` over each row as
it arrives rather than collecting the whole result set connection-side. A halted fold
cancels the query on the server; see `execute/4` for `Options`' `timeout` semantics.
""".
-spec execute(Connection, StatementText, Parameters, Fun, Acc, Options) -> {ok, Metadata, Acc} | {error, Error} when
    Connection :: pgc_connection:connection_ref(),
    StatementText :: unicode:chardata(),
    Parameters :: parameters(),
    Fun :: fun((pgc_connection:row_description(), [null | binary()], Acc) -> {cont, Acc} | {halt, Acc}),
    Options :: #{timeout => timeout()},
    Metadata :: result_metadata(),
    Error :: execute_error().
execute(Connection, StatementText, Parameters, Fun, Acc, Options) ->
    Timeout = maps:get(timeout, Options, infinity),
    run(Connection, fun (Ref) -> {execute, Ref, StatementText, Parameters} end, Fun, Acc, Timeout).


-doc """
Runs a parameterless statement as a simple query, bypassing parse/bind/execute.
Used internally for `commit`, `rollback` and `start transaction`.
""".
-spec execute_simple(Connection, StatementText) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: pgc_connection:connection_ref(),
    StatementText :: unicode:chardata(),
    Metadata :: result_metadata(),
    Rows :: [term()],
    Error :: pgc_protocol_message:error_response_fields().
execute_simple(Connection, StatementText) ->
    Fun = fun (_RowDescription, _Values, Acc) -> {cont, Acc} end,
    run(Connection, fun (Ref) -> {query, Ref, StatementText} end, Fun, [], infinity).


-doc false.
run(Connection, Request, Fun, Acc, Timeout) ->
    Ref = erlang:monitor(process, Connection, [{alias, demonitor}]),
    try
        ok = pgc_connection:cast(Connection, Request(Ref)),
        collect(Connection, Ref, Fun, Acc, pgc_deadline:from_timeout(Timeout))
    after
        erlang:demonitor(Ref, [flush])
    end.

-doc false.
collect(Connection, Ref, Fun, Acc, Deadline) ->
    receive
        {row, Ref, RowDescription, Values} ->
            case Fun(RowDescription, Values, Acc) of
                {cont, Acc1} ->
                    collect(Connection, Ref, Fun, Acc1, Deadline);
                {halt, Acc1} ->
                    ok = pgc_connection:cast(Connection, {cancel, Ref}),
                    {ok, #{}, Acc1}
            end;
        {done, Ref, {ok, Tag}} ->
            {ok, decode_tag(Tag), Acc};
        {done, Ref, empty} ->
            {ok, #{}, Acc};
        {done, Ref, {error, Fields}} ->
            {error, Fields};
        {'DOWN', Ref, process, _, Reason} ->
            exit(Reason)
    after pgc_deadline:to_timeout(Deadline) ->
        ok = pgc_connection:cast(Connection, {cancel, Ref}),
        exit({timeout, {?MODULE, execute, [Connection]}})
    end.


-type transaction_options() :: #{
    isolation => serializable | repeatable_read | read_committed | read_uncommitted | default,
    access => read_write | read_only | default,
    deferrable => boolean() | default
}.
-spec transaction(Connection, Fun, Options) -> Result when
    Connection :: pgc_connection:connection_ref(),
    Fun :: fun(() -> Result),
    Options :: transaction_options().
transaction(Connection, Fun, Options) ->
    StartStatementText = [
        ~"start transaction",
        transaction_option(isolation, Options, #{
            serializable => ~" isolation level serializable",
            repeatable_read => ~" isolation level repeatable read",
            read_committed => ~" isolation level read committed",
            read_uncommitted => ~" isolation level read uncommitted",
            default => ~""
        }),
        transaction_option(access, Options, #{
            read_write => ~" read write",
            read_only => ~" read only",
            default => ~""
        }),
        transaction_option(deferrable, Options, #{
            true => ~" deferrable",
            false => ~" not deferrable",
            default => ~""
        })
    ],
    case execute_simple(Connection, StartStatementText) of
        {ok, #{command := ~"start transaction"}, []} ->
            ok;
        {error, #{} = StartError} ->
            erlang:error({transaction_start_failed, StartError}, [Connection, Fun, Options])
    end,
    try Fun() of
        Result ->
            case execute_simple(Connection, ~"commit") of
                {ok, #{command := ~"commit"}, []} ->
                    Result;
                {ok, #{command := ~"rollback"}, []} ->
                    erlang:error(bad_transaction, [Connection, Fun, Options], [
                        {error_info, #{
                            cause => #{
                                general => "Transaction fun returned successfully from a failed transaction",
                                2 => "The fun should use rollback/2 upon error to exit the transaction"
                            }
                        }}
                    ]);
                {error, #{} = CommitError} ->
                    erlang:error({transaction_commit_failed, CommitError}, [Connection, Fun, Options])
            end
    catch
        throw:{?MODULE, rollback, Connection, Reason} ->
            {ok, _, []} = execute_simple(Connection, ~"rollback"),
            Reason;
        Class:Error:Stacktrace ->
            {ok, _, []} = execute_simple(Connection, ~"rollback"),
            erlang:raise(Class, Error, Stacktrace)
    end.

-doc false.
-spec transaction_option(Key, #{Key => Value}, #{Value => Text}) -> Text.
transaction_option(Key, Options, Mapping) ->
    Value = maps:get(Key, Options, default),
    case Mapping of
        #{Value := Text} ->
            Text;
        #{} ->
            erlang:error(badarg, [Key, Options, Mapping], [{error_info, #{
                cause => #{
                    2 => io_lib:format("invalid ~w: ~w", [Key, Value])
                }
            }}])
    end.


-spec rollback(Connection, Reason) -> no_return() when
    Connection :: pgc_connection:connection_ref(),
    Reason :: dynamic().
rollback(Connection, Reason) ->
    throw({?MODULE, rollback, Connection, Reason}).

% -----------------------------------------------------------------------------
% pgc_connection behaviour
% -----------------------------------------------------------------------------

-doc false.
init([]) ->
    {ok, #state{types = pgc_client_types:new(), refreshing_types = false, pending = []}}.

-doc false.
handle_cast(_ConnectionInfo, {execute, Ref, StatementText, Parameters}, State) ->
    #state{pending = Pending} = State,
    Req = #req{
        ref = Ref,
        statement_text = StatementText,
        parameters = [{text, Parameter} || Parameter <- Parameters]
    },
    {[{prepare, ~"", StatementText}], State#state{pending = Pending ++ [Req]}};
handle_cast(_ConnectionInfo, {query, Ref, StatementText}, State) ->
    #state{pending = Pending} = State,
    Req = #req{
        ref = Ref,
        statement_text = StatementText,
        parameters = []
    },
    {[{query, StatementText}], State#state{pending = Pending ++ [Req]}};
handle_cast(_ConnectionInfo, {cancel, Ref}, State) ->
    #state{pending = Pending} = State,
    case Pending of
        [#req{ref = Ref} | _] -> {[cancel], State};
        _ -> {[], State}
    end.

-doc false.
handle_prepare_result(_ConnectionInfo, {ok, Name, StatementDescription}, State) ->
    #state{pending = [Req | Pending], types = Types} = State,
    #{parameters_description := ParameterOids, row_description := RowFields} = StatementDescription,
    NeededOids = ParameterOids ++ [Field#row_description_field.type_oid || Field <- RowFields],
    case lists:all(fun (Oid) -> pgc_client_types:member(Types, Oid) end, NeededOids) of
        true ->
            {[{execute, Name, Req#req.parameters, #{}}], State#state{pending = [Req | Pending]}};
        false ->
            % Some of the types this statement needs aren't cached yet -- refresh, then
            % re-prepare (the unnamed statement doesn't survive the refresh's own Query
            % message) once we're back.
            {[{query, refresh_statement_text()}, {prepare, ~"", Req#req.statement_text}],
                State#state{pending = [Req | Pending], refreshing_types = true}}
    end;
handle_prepare_result(_ConnectionInfo, {error, Fields}, State) ->
    #state{pending = [Req | Pending]} = State,
    Req#req.ref ! {done, Req#req.ref, {error, Fields}},
    {[], State#state{pending = Pending}}.

-doc false.
handle_row_data(_ConnectionInfo, RowDescription, Values, State) ->
    case State#state.refreshing_types of
        true ->
            ok = pgc_client_types:insert_row(State#state.types, Values),
            {[], State};
        false ->
            #state{pending = [Req | _]} = State,
            Req#req.ref ! {row, Req#req.ref, RowDescription, Values},
            {[], State}
    end.

-doc false.
handle_query_result(ConnectionInfo, Result, State) ->
    case State#state.refreshing_types of
        true -> {[], State#state{refreshing_types = false}};
        false -> handle_execute_result(ConnectionInfo, Result, State)
    end.

-doc false.
handle_execute_result(_ConnectionInfo, Result, State) ->
    #state{pending = [Req | Pending]} = State,
    Req#req.ref ! {done, Req#req.ref, Result},
    {[], State#state{pending = Pending}}.


% -----------------------------------------------------------------------------
% Helpers
% -----------------------------------------------------------------------------

format_row(map, Fields, Values) ->
    maps:from_list(lists:zip([Field#row_description_field.name || Field <- Fields], Values));
format_row(list, _Fields, Values) ->
    Values;
format_row(tuple, _Fields, Values) ->
    list_to_tuple(Values);
format_row(proplist, Fields, Values) ->
    lists:zip([Field#row_description_field.name || Field <- Fields], Values).


-spec decode_tag(undefined) -> #{};
                 (unicode:unicode_binary()) -> #{command := binary(), rows => non_neg_integer()}.
decode_tag(undefined) ->
    #{};
decode_tag(Tag) ->
    case binary:split(Tag, ~" ", [global]) of
        [~"SELECT", Count] ->
            #{command => ~"select", rows => binary_to_integer(Count)};
        [~"INSERT", _Oid, Count] ->
            #{command => ~"insert", rows => binary_to_integer(Count)};
        [~"UPDATE", Count] ->
            #{command => ~"update", rows => binary_to_integer(Count)};
        [~"DELETE", Count] ->
            #{command => ~"delete", rows => binary_to_integer(Count)};
        [~"MERGE", Count] ->
            #{command => ~"merge", rows => binary_to_integer(Count)};
        [~"MOVE", Count] ->
            #{command => ~"move", rows => binary_to_integer(Count)};
        [~"FETCH", Count] ->
            #{command => ~"fetch", rows => binary_to_integer(Count)};
        [~"COPY", Count] ->
            #{command => ~"copy", rows => binary_to_integer(Count)};
        _ ->
            #{command => string:lowercase(Tag)}
    end.


refresh_statement_text() ->
     ~"""
        select
            pg_type.oid as oid,
            pg_type.typname as name,
            pg_type.typtype as kind,
            pg_type.typreceive as recv,
            pg_type.typsend as send,
            pg_type.typelem as element,
            array(
                select pg_attribute.attname || ':' || pg_attribute.atttypid
                from pg_attribute
                where pg_attribute.attrelid = pg_type.typrelid
                and pg_attribute.attnum > 0
                and not pg_attribute.attisdropped
                order by pg_attribute.attnum
            ) as fields
        from pg_catalog.pg_type
    """.
