-module(pgc_client).
-export([
    start_link/1,
    start_link/2,
    stop/1
]).
-export([
    execute/3,
    execute/4,
    transaction/3,
    rollback/2
]).
-export_type([
    parameters/0,
    row_format/0,
    execute_options/0,
    transaction_options/0
]).

-behaviour(pgc_connection).
-export([
    init/1,
    handle_call/4,
    handle_prepare_result/3,
    handle_query_result/3,
    handle_execute_result/3,
    handle_row_data/4
]).

-import_record(pgc_protocol_message, [row_description_field]).

-record #req{
    from :: gen_statem:from(),
    parameters :: pgc_connection_statem_extended_query:execute_parameters(),
    row_format :: row_format(),
    rows :: [term()]
}.

-record #state{
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


-doc """
Parameters for a parameterized statement -- until codecs land, these are sent to
Postgres as-is (in text format), rather than encoded from arbitrary Erlang terms.
""".
-type parameters() :: [iodata() | null].

-type row_format() :: map | list | tuple | proplist.

-type execute_options() :: #{
    row => row_format()
}.

-spec execute(Connection, StatementText, Parameters) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: pgc_connection:connection_ref(),
    StatementText :: unicode:chardata(),
    Parameters :: parameters(),
    Metadata :: #{command := atom(), rows => non_neg_integer()},
    Rows :: [term()],
    Error :: pgc_protocol_message:error_response_fields().
execute(Connection, StatementText, Parameters) ->
    execute(Connection, StatementText, Parameters, #{}).


-spec execute(Connection, StatementText, Parameters, Options) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: pgc_connection:connection_ref(),
    StatementText :: unicode:chardata(),
    Parameters :: parameters(),
    Options :: execute_options(),
    Metadata :: #{command := atom(), rows => non_neg_integer()},
    Rows :: [term()],
    Error :: pgc_protocol_message:error_response_fields().
execute(Connection, StatementText, Parameters, Options) ->
    RowFormat = maps:get(row, Options, map),
    pgc_connection:call(Connection, {execute, StatementText, Parameters, RowFormat}, infinity).


-doc """
Runs a parameterless statement as a simple query, bypassing parse/bind/execute.
Used internally for `commit`, `rollback` and `start transaction`.
""".
-spec execute_simple(Connection, StatementText) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: pgc_connection:connection_ref(),
    StatementText :: unicode:chardata(),
    Metadata :: #{command := atom(), rows => non_neg_integer()},
    Rows :: [term()],
    Error :: pgc_protocol_message:error_response_fields().
execute_simple(Connection, StatementText) ->
    pgc_connection:call(Connection, {execute, StatementText}, infinity).


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
        {ok, #{command := start_transaction}, []} ->
            ok;
        {error, #{} = StartError} ->
            erlang:error({transaction_start_failed, StartError}, [Connection, Fun, Options])
    end,
    try Fun() of
        Result ->
            case execute_simple(Connection, ~"commit") of
                {ok, #{command := commit}, []} ->
                    Result;
                {ok, #{command := rollback}, []} ->
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
    {ok, #state{pending = []}}.

-doc false.
handle_call(_ConnectionInfo, {execute, StatementText, Parameters, RowFormat}, From, State) ->
    #state{pending = Pending} = State,
    Req = #req{
        from = From,
        parameters = [{text, Parameter} || Parameter <- Parameters],
        row_format = RowFormat,
        rows = []
    },
    {[{prepare, ~"", StatementText}], State#state{pending = Pending ++ [Req]}};
handle_call(_ConnectionInfo, {execute, StatementText}, From, State) ->
    #state{pending = Pending} = State,
    Req = #req{
        from = From,
        parameters = [],
        row_format = map,
        rows = []
    },
    {[{query, StatementText}], State#state{pending = Pending ++ [Req]}}.

-doc false.
handle_prepare_result(_ConnectionInfo, {ok, Name, _StatementDescription}, State) ->
    #state{pending = [Req | _]} = State,
    {[{execute, Name, Req#req.parameters, #{}}], State};
handle_prepare_result(_ConnectionInfo, {error, Fields}, State) ->
    #state{pending = [Req | Pending]} = State,
    {[{reply, Req#req.from, {error, Fields}}], State#state{pending = Pending}}.

-doc false.
handle_row_data(_ConnectionInfo, RowDescription, Values, State) ->
    #state{pending = [Req | Pending]} = State,
    Row = row_value(Req#req.row_format, RowDescription, Values),
    {[], State#state{pending = [Req#req{rows = [Row | Req#req.rows]} | Pending]}}.

-doc false.
handle_query_result(ConnectionInfo, Result, State) ->
    handle_execute_result(ConnectionInfo, Result, State).

-doc false.
handle_execute_result(_ConnectionInfo, Result, State) ->
    #state{pending = [Req | Pending]} = State,
    Reply = case Result of
        {ok, Tag} ->
            {ok, decode_tag(Tag), lists:reverse(Req#req.rows)};
        empty ->
            {ok, #{}, []};
        {error, Fields} ->
            {error, Fields}
    end,
    {[{reply, Req#req.from, Reply}], State#state{pending = Pending}}.


% -----------------------------------------------------------------------------
% Helpers
% -----------------------------------------------------------------------------

row_value(map, Fields, Values) ->
    maps:from_list(lists:zip([Field#row_description_field.name || Field <- Fields], Values));
row_value(list, _Fields, Values) ->
    Values;
row_value(tuple, _Fields, Values) ->
    list_to_tuple(Values);
row_value(proplist, Fields, Values) ->
    lists:zip([Field#row_description_field.name || Field <- Fields], Values).

-spec decode_tag(undefined) -> #{};
                 (unicode:unicode_binary()) -> #{command := atom(), rows => non_neg_integer()}.
decode_tag(undefined) ->
    #{};
decode_tag(~"START TRANSACTION") ->
    #{command => start_transaction};
decode_tag(Tag) ->
    case binary:split(Tag, ~" ", [global]) of
        [~"SELECT", Count] ->
            #{command => select, rows => binary_to_integer(Count)};
        [~"INSERT", _Oid, Count] ->
            #{command => insert, rows => binary_to_integer(Count)};
        [~"UPDATE", Count] ->
            #{command => update, rows => binary_to_integer(Count)};
        [~"DELETE", Count] ->
            #{command => delete, rows => binary_to_integer(Count)};
        [~"MERGE", Count] ->
            #{command => merge, rows => binary_to_integer(Count)};
        [~"MOVE", Count] ->
            #{command => move, rows => binary_to_integer(Count)};
        [~"FETCH", Count] ->
            #{command => fetch, rows => binary_to_integer(Count)};
        [~"COPY", Count] ->
            #{command => copy, rows => binary_to_integer(Count)};
        [Command | _Rest] ->
            #{command => binary_to_atom(string:lowercase(Command))}
    end.
