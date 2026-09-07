-module(pgc_client).
-moduledoc """
A client implementation based on the `m:pgc_connection` behaviour suitable for
sending queries to the PostgreSQL server.
""".

-export([
    start_link/1,
    start_link/2,
    stop/1
]).
-export([
    execute/3,
    execute/4,
    execute_fold/6,
    transaction/3,
    reset/1
]).
-export_type([
    start_options/0,
    statement_text/0,
    statement_parameters/0,
    execute_options/0,
    transaction_options/0
]).

-behaviour(pgc_connection).
-export([
    init/1,
    handle_call/4,
    handle_cast/3,
    handle_info/3,
    handle_prepare_result/4,
    handle_unprepare_result/4,
    handle_query_result/4,
    handle_execute_result/4,
    handle_row_data/5
]).

-import_record(pgc_protocol_message, [row_description_field]).


% -----------------------------------------------------------------------------
% Types
% -----------------------------------------------------------------------------

-record #state{
    types :: pgc_client_types:t(),
    requests :: queue:queue(request()),
    statements :: #{unicode:unicode_binary() => statement()}
}.

-record #request{
    id :: reference(),
    monitor :: reference(),

    statement_name :: unicode:unicode_binary(),
    statement_text :: statement_text(),
    phase :: request_phase(),
    kind :: execute | query | refresh
}.
-type request() :: #request{}.
-type request_phase() ::
    queued
    | preparing
    | unpreparing
    | executing
    | querying
    | awaiting_parameters.

-record #statement{
    hash :: binary(),
    parameters_description :: [pgc_protocol:oid()],
    row_description :: [pgc_protocol_message:row_description_field()]
}.
-type statement() :: #statement{}.
-type statement_text() :: unicode:chardata().
-type statement_parameters() :: [term() | null].


% -----------------------------------------------------------------------------
% API
% -----------------------------------------------------------------------------


-doc """
Start a new postgresql client connection.
""".
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


-doc """
Start a new postgresql client connection registered using
the provided `ClientName`.

`ClientName` specifies the `t:pgc_connection:connection_name/0` to
register for the `m:pgc_connection` process.
""".
-spec start_link(pgc_connection:connection_name(), start_options()) -> pgc_connection:start_ret().
start_link(ClientName, Options) ->
    pgc_connection:start_link(ClientName, ?MODULE, [], Options).


-doc """
Orders the client to exit and waits for it to terminate.
""".
-spec stop(pgc_connection:connection_ref()) -> ok.
stop(ClientRef) ->
    pgc_connection:stop(ClientRef).


-doc """
Runs a parameterized statement through parse/bind/execute, collecting every row into a
list shaped of maps.
""".
-spec execute(ClientRef, Statement, Parameters) -> {ok, Metadata, Rows} | {error, Error} when
    ClientRef :: pgc_connection:connection_ref(),
    Statement :: statement_text(),
    Parameters :: statement_parameters(),
    Metadata :: result_metadata(),
    Rows :: [map()],
    Error :: request_error().
execute(ClientRef, Statement, Parameters) ->
    execute(ClientRef, Statement, Parameters, #{}).


-doc """
Runs a parameterized statement through parse/bind/execute, collecting every row into a
list shaped per `Options`' `row` (`map` by default).

`Options`' `timeout`, if given, on expiry, the query is cancelled on the server and
exits with `exit({timeout, _})`.
""".
-spec execute(ClientRef, Statement, Parameters, Options) -> {ok, Metadata, Rows} | {error, Error} when
    ClientRef :: pgc_connection:connection_ref(),
    Statement :: statement_text(),
    Parameters :: statement_parameters(),
    Options :: execute_options(),
    Metadata :: result_metadata(),
    Rows :: [dynamic()],
    Error :: request_error().
-type execute_options() :: #{
    row => map | list | tuple | proplist,
    timeout => timeout(),
    cache => false | {true, Key :: string() | unicode:unicode_binary() | atom()},
    codec => #{modules => [module()], atom() => term()}
}.
-type result_metadata() :: #{
    command => binary(),
    rows => non_neg_integer()
}.
execute(ClientRef, StatementText, Parameters, Options) ->
    RowFormat = maps:get(row, Options, map),
    RemainingOptions = maps:without([row], Options),
    case execute_fold(ClientRef, StatementText, Parameters, fun (RowDescription, Values, Acc) ->
        {continue, [format_row(RowFormat, RowDescription, Values) | Acc]}
    end, [], RemainingOptions) of
        {ok, Metadata, Rows} ->
            {ok, Metadata, lists:reverse(Rows)};
        {error, _} = Error -> Error
    end.


-spec format_row
    (map, [#row_description_field{}], [term()]) -> #{binary() => term()};
    (list, [#row_description_field{}], [term()]) -> [term()];
    (tuple, [#row_description_field{}], [term()]) -> tuple();
    (proplist, [#row_description_field{}], [term()]) -> [{binary(), term()}].
format_row(map, Fields, Values) ->
    #{Name => Value || #row_description_field{name = Name} <- Fields && Value <- Values};
format_row(list, _Fields, Values) ->
    Values;
format_row(tuple, _Fields, Values) ->
    list_to_tuple(Values);
format_row(proplist, Fields, Values) ->
    [{Name, Value} || #row_description_field{name = Name} <- Fields && Value <- Values].


-doc """
Runs a parameterized statement through parse/bind/execute, folding `Fun` over each row as
it arrives. A halted fold cancels the query on the server; see `execute/4` for `Options`'
`timeout` semantics.
""".
-spec execute_fold(ClientRef, Statement, Parameters, Fun, Acc, Options) -> {ok, Metadata, Acc} | {error, Error} when
    ClientRef :: pgc_connection:connection_ref(),
    Statement :: statement_text(),
    Parameters :: statement_parameters(),
    Fun :: execute_fold_fun(Acc),
    Options :: execute_fold_options(),
    Metadata :: result_metadata(),
    Error :: request_error().
-type execute_fold_fun(Acc) :: fun((pgc_connection:row_description(), [term() | null], Acc) -> {continue, Acc} | {halt, Acc}).
-type execute_fold_options() :: #{
    timeout => timeout(),
    cache => execute_cache_options(),
    codec => #{modules => [module()], atom() => term()}
}.
-type execute_cache_options() :: false | {true, Key :: string() | unicode:unicode_binary() | atom()}.
execute_fold(ClientRef, StatementText, Parameters, Fun, Acc, Options) ->
    Request = {execute, StatementText, Parameters, maps:with([cache], Options)},
    RequestOptions = maps:without([cache], Options),
    request(ClientRef, Request, RequestOptions, Fun, Acc).


-doc """
Runs a parameterless statement as a simple query, bypassing parse/bind/execute.
Used internally for `commit`, `rollback` and `start transaction`.
""".
-spec execute_simple(ClientRef, Statement) -> {ok, Metadata} | {error, Error} when
    ClientRef :: pgc_connection:connection_ref(),
    Statement :: statement_text(),
    Metadata :: result_metadata(),
    Error :: pgc_protocol_message:error_response_fields().
execute_simple(ClientRef, StatementText) ->
    Request = {query, StatementText},
    Fun = fun (_RowDescription, _Values, Acc) -> {continue, Acc} end,
    case request(ClientRef, Request, #{}, Fun, []) of
        {ok, Metadata, _Rows} ->
            {ok, Metadata};
        {error, _Reason} = Error ->
            Error
    end.


-doc """
Encoding and decoding are deliberately done here, in the caller's own process, rather than in
`handle_row_data/5`/`handle_prepare_result/4` (which run in the connection process): the
connection is a shared, serializing bottleneck, while `Types` (`pgc_client_types:t()`) wraps a
`protected` ets table specifically so any number of callers can read it -- and therefore encode
and decode -- concurrently, off the connection's own execution stack. The connection only ever
mediates the wire and owns the (write side of the) type cache; the caller builds its own
`pgc_client_codec:t()` from `Types` and this call's `codec` option (see `collect/7`).
""".
-spec request(ClientRef, Request, Options, Fun, Acc) -> {ok, result_metadata(), Acc} | {error, Error} when
    ClientRef :: pgc_connection:connection_ref(),
    Request :: QueryRequest | ExecuteRequest,
    QueryRequest :: {query, statement_text()},
    ExecuteRequest :: {execute, statement_text(), statement_parameters(), ExecuteRequestOptions},
    ExecuteRequestOptions :: #{cache => execute_cache_options()},
    Options :: #{
        codec => #{modules => [module()], atom() => term()},
        timeout => timeout()
    },
    Fun :: fun((pgc_connection:row_description(), [term() | null], Acc) -> {continue, Acc} | {halt, Acc}),
    Error :: request_error().
-type request_error() :: pgc_protocol_message:error_response_fields().
request(ClientRef, Request, Options, Fun, Acc) when is_pid(ClientRef) ->
    Parameters = case Request of
        {query, _StatementText} -> [];
        {execute, _StatementText, StatementParameters, _ExecuteOptions} -> StatementParameters
    end,
    CodecOptions = maps:get(codec, Options, #{}),
    Timeout = maps:get(timeout, Options, infinity),
    Deadline = pgc_deadline:from_timeout(Timeout),
    RequestId = erlang:monitor(process, ClientRef, [{alias, demonitor}]),
    try
        ok = pgc_connection:cast(ClientRef, {request, self(), RequestId, Request}),
        request_statem(ClientRef, RequestId, Parameters, CodecOptions, Deadline, Fun, Acc)
    after
        erlang:demonitor(RequestId, [flush])
    end.

-doc false.
-spec request_statem(ClientRef, RequestId, Parameters, Codec | CodecOptions, Deadline, Fun, Acc) -> {ok, result_metadata(), Acc} | {error, Error} when
    ClientRef :: pgc_connection:connection_ref(),
    RequestId :: reference(),
    Parameters ::  [term()],
    Codec :: pgc_client_codec:t(),
    CodecOptions :: #{
        modules => [module()], atom() => term()
    },
    Deadline :: pgc_deadline:t(),
    Fun :: fun((pgc_connection:row_description(), [term() | null], Acc) -> {continue, Acc} | {halt, Acc}),
    Error :: request_error().
request_statem(ClientRef, RequestId, Parameters, CodecOrCodecOptions, Deadline, Fun, Acc) ->
    receive
        {parameters_required, RequestId, Types, ParametersDescription} when is_map(CodecOrCodecOptions) ->
            Codec = ensure_codec(Types, CodecOrCodecOptions),
            EncodedParameters = encode_parameters(Parameters, ParametersDescription, Codec),
            ok = pgc_connection:cast(ClientRef, {parameters, RequestId, EncodedParameters}),
            request_statem(ClientRef, RequestId, Parameters, Codec, Deadline, Fun, Acc);
        {row, RequestId, Types, RowDescription, RowData} ->
            Codec = ensure_codec(Types, CodecOrCodecOptions),
            Row = decode_row(RowDescription, RowData, Codec),
            case Fun(RowDescription, Row, Acc) of
                {continue, Acc1} ->
                    request_statem(ClientRef, RequestId, Parameters, CodecOrCodecOptions, Deadline, Fun, Acc1);
                {halt, Acc1} ->
                    ok = pgc_connection:cast(ClientRef, {cancel, RequestId}),
                    {ok, #{}, Acc1}
            end;
        {done, RequestId, {ok, Tag}} ->
            {ok, decode_tag(Tag), Acc};
        {done, RequestId, empty} ->
            {ok, #{}, Acc};
        {done, RequestId, {error, Fields}} ->
            {error, Fields};
        {'DOWN', RequestId, process, _, Reason} ->
            exit(Reason)
    after pgc_deadline:to_timeout(Deadline) ->
        ok = pgc_connection:cast(ClientRef, {cancel, RequestId}),
        exit({timeout, {?MODULE, execute, [ClientRef]}})
    end.


-spec ensure_codec(pgc_client_types:t(), pgc_client_codec:t() | map()) -> pgc_client_codec:t().
ensure_codec(_Types, Codec) when is_record(Codec, pgc_client_codec, codec) ->
    Codec;
ensure_codec(Types, Options) when is_map(Options) ->
    pgc_client_codec:new(Types, Options).


-spec encode_parameters([term()], [pgc_client_types:id()], pgc_client_codec:t()) -> [{binary, iodata() | null}].
encode_parameters(Parameters, ParametersDescription, Codec) ->
    [{binary, pgc_client_codec:encode(TypeId, Parameter, Codec)} ||
        TypeId <:- ParametersDescription && Parameter <:- Parameters].


-spec decode_row(pgc_connection:row_description(), [binary() | null], pgc_client_codec:t()) -> [term()].
decode_row(RowDescription, RowData, Codec) ->
    [
        pgc_client_codec:decode(TypeId, Data, Codec) ||
            #row_description_field{type_oid = TypeId} <:- RowDescription &&
            Data <:- RowData
    ].


-type transaction_options() :: #{
    isolation => serializable | repeatable_read | read_committed | read_uncommitted | default,
    access => read_write | read_only | default,
    deferrable => boolean() | default
}.
-spec transaction(ClientRef, Fun, Options) -> Result when
    ClientRef :: pgc_connection:connection_ref(),
    Fun :: fun(() -> {commit | rollback, Result}),
    Options :: transaction_options().
transaction(ClientRef, Fun, Options) ->
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
    case execute_simple(ClientRef, StartStatementText) of
        {ok, #{command := ~"start transaction"}} ->
            ok;
        {error, #{} = StartError} ->
            erlang:error({transaction_start_failed, StartError}, [ClientRef, Fun, Options])
    end,
    try Fun() of
        {commit, Result} ->
            case execute_simple(ClientRef, ~"commit") of
                {ok, #{command := ~"commit"}} ->
                    Result;
                {ok, #{command := ~"rollback"}} ->
                    erlang:error(bad_transaction, [ClientRef, Fun, Options], [
                        {error_info, #{
                            cause => #{
                                general => "Transaction fun returned successfully from a failed transaction",
                                2 => "The fun should use rollback/2 upon error to exit the transaction"
                            }
                        }}
                    ]);
                {error, #{} = CommitError} ->
                    erlang:error({transaction_commit_failed, CommitError}, [ClientRef, Fun, Options])
            end;
        {rollback, Result} ->
            case execute_simple(ClientRef, ~"rollback") of
                {ok, #{command := ~"rollback"}} ->
                    Result;
                {error, #{} = RollbackError} ->
                    erlang:error({transaction_rollback_failed, RollbackError}, [ClientRef, Fun, Options])
            end
    catch
        Class:Error:Stacktrace ->
            {ok, _} = execute_simple(ClientRef, ~"rollback"),
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


-doc """
Brings `ClientRef` back to a clean session state -- rolls back any transaction `Action` left
open, then clears any session-level `set` it left behind. Meant to be called whenever a
connection is handed back to a pool, between one caller and the next.
""".
-spec reset(ClientRef) -> ok when
    ClientRef :: pgc_connection:connection_ref().
reset(ClientRef) ->
    {ok, _} = execute_simple(ClientRef, ~"rollback"),
    {ok, _} = execute_simple(ClientRef, ~"reset all"),
    ok.


% -----------------------------------------------------------------------------
% pgc_connection behaviour
% -----------------------------------------------------------------------------

-doc false.
init(_Args) ->
    {ok, #state{
        types = pgc_client_types:new(),
        requests = queue:new(),
        statements = #{}
    }}.


-doc false.
handle_call(_ConnectionInfo, Call, From, #state{} = State) ->
    {[{reply, From, {error, {unknown_call, Call}}}], State}.


-doc """
Every request -- `execute`, `query`, and the internally-triggered `refresh` (see
`handle_prepare_result/4`) -- goes through here. Only the *head* of `State#state.requests` is
ever dispatched to the wire; everything else just sits there (`phase = queued`) until the head
finishes, one way or another, and pops. That's the whole safety property this module leans on:
since only one request is ever mid-flight, nothing ever decides whether a statement name is a
cache hit from a `Statements` snapshot another, still in-flight request is already invalidating --
by the time anything reads `Statements`, whatever was running before is already fully done.
""".
handle_cast(ConnectionInfo, {request, FromPid, RequestId, {execute, StatementText, _StatementParameters, Options}}, State) ->
    process_request(ConnectionInfo, FromPid, RequestId, execute, statement_name(Options), StatementText, State);

handle_cast(ConnectionInfo, {request, FromPid, RequestId, {query, StatementText}}, State) ->
    process_request(ConnectionInfo, FromPid, RequestId, query, ~"", StatementText, State);

handle_cast(_ConnectionInfo, {parameters, RequestId, EncodedParameters}, State) ->
    process_parameters(RequestId, EncodedParameters, State);

handle_cast(ConnectionInfo, {cancel, RequestId}, State) ->
    process_cancel(ConnectionInfo, RequestId, State).


handle_info(ConnectionInfo, {{'DOWN', RequestId}, _MonitorRef, process, _Pid, _Reason}, #state{} = State) ->
    handle_cast(ConnectionInfo, {cancel, RequestId}, State).


process_request(ConnectionInfo, FromPid, RequestId, Kind, StatementName, StatementText, #state{requests = Requests} = State) ->
    Request = #request{
        id = RequestId,
        statement_name = StatementName,
        statement_text = StatementText,
        kind = Kind,
        phase = queued,
        monitor = erlang:monitor(process, FromPid, [{tag, {'DOWN', RequestId}}])
    },
    case queue:is_empty(Requests) of
        true -> dispatch(ConnectionInfo, State#state{requests = queue:in(Request, Requests)});
        false -> {[], State#state{requests = queue:in(Request, Requests)}}
    end.


process_parameters(RequestId, EncodedParameters, #state{requests = Requests} = State) ->
    {value, #request{id = RequestId, statement_name = StatementName} = Request} = queue:peek(Requests),
    {{value, _}, Rest} = queue:out(Requests),
    {[
        {execute, RequestId, StatementName, EncodedParameters, #{result_format => binary}}
    ], State#state{requests = queue:in_r(Request#request{phase = executing}, Rest)}}.


process_cancel(ConnectionInfo, RequestId, #state{requests = Requests} = State) ->
    case queue:peek(Requests) of
        {value, #request{id = RequestId, phase = awaiting_parameters, monitor = Monitor}} ->
            % Nothing outstanding on the wire to cancel -- we're just waiting on the caller,
            % who's given up.
            erlang:demonitor(Monitor, [flush]),
            {{value, _}, Rest} = queue:out(Requests),
            case queue:is_empty(Rest) of
                true -> {[], State#state{requests = Rest}};
                false -> dispatch(ConnectionInfo, State#state{requests = Rest})
            end;
        {value, #request{id = RequestId}} ->
            {[{cancel, RequestId}], State};
        _ ->
            % Not the running request -- if it's still waiting in line, nothing was ever sent
            % for it, so just drop it locally; if it's not there at all (already finished, or
            % this cancel lost the race), the wire cancel below is a harmless no-op.
            case lists:partition(fun (#request{id = Id}) -> Id =:= RequestId end, queue:to_list(Requests)) of
                {[#request{monitor = Monitor}], Rest} ->
                    erlang:demonitor(Monitor, [flush]),
                    {[], State#state{requests = queue:from_list(Rest)}};
                {[], _} ->
                    {[{cancel, RequestId}], State}
            end
    end.


-doc """
Dispatches whatever is now at the head of the queue -- a fresh arrival with nothing running
ahead of it, or the next one in line once the previous head finished. `query` is the only kind
that never touches `Statements`' cache (it always invalidates the unnamed slot instead, see
`dispatch_query/2`); `execute` and `refresh` share the exact same cache-hit /
hash-mismatch-unprepare / not-cached-prepare logic -- a `refresh` request is just an unnamed,
parameterless statement like any other, the only two places anything cares that it's a refresh
are where its rows land (`handle_row_data/5`) and whether it gets a reply (`handle_execute_result/4`).
""".
dispatch(ConnectionInfo, State) ->
    {value, Request} = queue:peek(State#state.requests),
    case Request#request.kind of
        query -> dispatch_query(Request, State);
        _ -> dispatch_execute(ConnectionInfo, Request, State)
    end.

dispatch_execute(ConnectionInfo, #request{id = RequestId, statement_name = StatementName, statement_text = StatementText} = Request, State) ->
    #state{requests = Requests, statements = Statements} = State,
    StatementHash = statement_hash(StatementText),
    case Statements of
        #{StatementName := #statement{hash = StatementHash, parameters_description = ParametersDescription, row_description = RowDescription}} ->
            % Cached and matching -- handle it as if we'd just gotten a successful prepare.
            StatementDescription = #{parameters_description => ParametersDescription, row_description => RowDescription},
            {{value, _}, Rest} = queue:out(Requests),
            handle_prepare_result(ConnectionInfo, RequestId, {ok, StatementName, StatementDescription},
                State#state{requests = queue:in_r(Request#request{phase = preparing}, Rest)});
        #{StatementName := #statement{}} ->
            % Cached and not matching -- close it first, then re-prepare (handle_unprepare_result/4).
            {{value, _}, Rest} = queue:out(Requests),
            {[{unprepare, RequestId, StatementName}], State#state{requests = queue:in_r(Request#request{phase = unpreparing}, Rest)}};
        #{} ->
            {{value, _}, Rest} = queue:out(Requests),
            {[{prepare, RequestId, StatementName, StatementText}], State#state{requests = queue:in_r(Request#request{phase = preparing}, Rest)}}
    end.

dispatch_query(#request{id = RequestId, statement_text = StatementText} = Request, State) ->
    #state{requests = Requests, statements = Statements} = State,
    {{value, _}, Rest} = queue:out(Requests),
    {[{query, RequestId, StatementText}], State#state{
        requests = queue:in_r(Request#request{phase = querying}, Rest),
        % A plain Query message also destroys the server's unnamed statement/portal, no matter
        % what it runs -- keep the cache honest about that.
        statements = maps:remove(~"", Statements)
    }}.


-doc false.
handle_prepare_result(ConnectionInfo, RequestId, {ok, StatementName, StatementDescription}, State) ->
    #state{types = Types, requests = Requests, statements = Statements} = State,
    {value, #request{id = RequestId} = Request} = queue:peek(Requests),
    #{parameters_description := ParametersDescription, row_description := RowDescription} = StatementDescription,
    Statement = #statement{
        hash = statement_hash(Request#request.statement_text),
        parameters_description = ParametersDescription,
        row_description = RowDescription
    },
    NewState = State#state{statements = Statements#{StatementName => Statement}},
    NeededOids = ordsets:from_list(ParametersDescription ++ [
        Field#row_description_field.type_oid || Field <- RowDescription
    ]),
    case lists:all(fun (Id) -> pgc_client_types:has(Id, Types) end, NeededOids) of
        true when ParametersDescription =/= [] ->
            RequestId ! {parameters_required, RequestId, Types, ParametersDescription},
            {{value, _}, Rest} = queue:out(NewState#state.requests),
            {[], NewState#state{requests = queue:in_r(Request#request{phase = awaiting_parameters}, Rest)}};
        true when ParametersDescription =:= [] ->
            {{value, _}, Rest} = queue:out(NewState#state.requests),
            {[{execute, RequestId, StatementName, [], #{result_format => binary}}],
                NewState#state{requests = queue:in_r(Request#request{phase = executing}, Rest)}};
        false ->
            % Missing types -- run the refresh ahead of this request (still holding its own
            % now-cached #statement{} right behind it) rather than in its place. Once the
            % refresh finishes and pops, this same request comes back up as the new head and
            % dispatch_execute/3 finds its own statement already cached -- the exact same
            % cache-hit replay used for an ordinary repeat call.
            Refresh = #request{
                id = make_ref(),
                statement_name = ~"",
                statement_text = refresh_statement_text(),
                kind = refresh,
                phase = queued,
                % No caller to actually monitor -- self() just satisfies the field (a refresh's
                % monitor never fires; demonitored like any other at completion regardless).
                monitor = erlang:monitor(process, self())
            },
            dispatch(ConnectionInfo, NewState#state{requests = queue:in_r(Refresh, NewState#state.requests)})
    end;

handle_prepare_result(ConnectionInfo, RequestId, {error, Fields}, State) ->
    handle_execute_result(ConnectionInfo, RequestId, {error, Fields}, State).


-doc false.
handle_unprepare_result(_ConnectionInfo, RequestId, {ok, Name}, State) ->
    % Only ever reached to reclaim a name for a re-prepare with new text -- the Close is now
    % confirmed, so the name is free and `Statements` can drop the stale entry.
    #state{requests = Requests, statements = Statements} = State,
    {value, #request{id = RequestId} = Request} = queue:peek(Requests),
    {{value, _}, Rest} = queue:out(Requests),
    {[{prepare, RequestId, Name, Request#request.statement_text}], State#state{
        statements = maps:remove(Name, Statements),
        requests = queue:in_r(Request#request{phase = preparing}, Rest)
    }};

handle_unprepare_result(ConnectionInfo, RequestId, {error, Fields}, State) ->
    % Close didn't actually happen (a cancel beat it) -- the old statement is still live under
    % this name, so `Statements` is left untouched.
    handle_execute_result(ConnectionInfo, RequestId, {error, Fields}, State).


-doc false.
handle_row_data(_ConnectionInfo, RequestId, RowDescription, RowData, State) ->
    {value, #request{id = RequestId, kind = Kind}} = queue:peek(State#state.requests),
    case Kind of
        refresh ->
            ok = process_refresh_types_result(RowDescription, RowData, State#state.types),
            {[], State};
        execute ->
            % Decoding happens caller-side (see request_statem/7) -- just forward the raw wire
            % values plus the shared (protected, multi-reader) type cache they need.
            RequestId ! {row, RequestId, State#state.types, RowDescription, RowData},
            {[], State};
        query ->
            % execute_simple/2's own Fun already treats rows as a no-op fold -- nothing sent
            % over the simple protocol today ever actually returns any, but if it did, dropping
            % them here matches that contract instead of crashing on a missing case clause.
            {[], State}
    end.

-doc """
`query` requests (the only ones running over the simple protocol -- `execute_simple/2`'s
`commit`/`rollback`/`start transaction`/`reset all`) never have rows worth folding into anything
beyond their own command tag, so this is just `handle_execute_result/4` under another name.
""".
handle_query_result(ConnectionInfo, RequestId, Result, State) ->
    handle_execute_result(ConnectionInfo, RequestId, Result, State).

-doc """
The terminal point for every request, whatever kind it is: `execute` and `query` reply to their
caller and let the next one in line start; `refresh` has no caller to reply to (it's
self-monitored, not alias-monitored, so sending to it would crash) -- it just lets the next one
in line start, which is the paused request it was refreshing types for.
""".
handle_execute_result(ConnectionInfo, RequestId, Result, State) ->
    #state{requests = Requests} = State,
    {value, #request{id = RequestId, kind = Kind, monitor = Monitor}} = queue:peek(Requests),
    erlang:demonitor(Monitor, [flush]),
    case Kind of
        refresh -> ok;
        _ -> RequestId ! {done, RequestId, Result}
    end,
    {{value, _}, Rest} = queue:out(Requests),
    case queue:is_empty(Rest) of
        true -> {[], State#state{requests = Rest}};
        false -> dispatch(ConnectionInfo, State#state{requests = Rest})
    end.


% -----------------------------------------------------------------------------
% Helpers
% -----------------------------------------------------------------------------

statement_name(#{cache := {true, Key}}) when is_atom(Key) ->
    atom_to_binary(Key);
statement_name(#{cache := {true, Key}}) ->
    case unicode:characters_to_binary(Key) of
        Binary when is_binary(Binary) -> Binary;
        {error, _, _} -> erlang:error(badarg, [Key]);
        {incomplete, _, _} -> erlang:error(badarg, [Key])
    end;
statement_name(#{}) ->
    ~"".

statement_hash(StatementText) ->
    crypto:hash(sha256, StatementText).


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


-doc """
Decodes one row of `refresh_statement_text/0`'s result set through the same
`pgc_client_codec` path as any other query's rows -- binary format throughout, so no bespoke
text/array parsing is needed here -- and folds it straight into `Types`. Runs connection-side
(unlike every other row, decoded caller-side, see `request/5`'s doc) since a refresh has no
caller to do it instead.
""".
-spec process_refresh_types_result(pgc_connection:row_description(), [binary() | null], pgc_client_types:t()) -> ok.
process_refresh_types_result(RowDescription, RowData, Types) ->
    Codec = pgc_client_codec:new(Types, #{}),
    [Oid, Namespace, Name, Kind, Send, Recv, ElementType, ParentType, FieldNames, FieldTypes] = [
        pgc_client_codec:decode(TypeId, Data, Codec) ||
            #row_description_field{type_oid = TypeId} <:- RowDescription &&
            Data <:- RowData
    ],
    pgc_client_types:add(Oid, #{
        namespace => Namespace,
        name => Name,
        kind => case Kind of
            ~"b" -> base;
            ~"c" -> composite;
            ~"d" -> domain;
            ~"e" -> enum;
            ~"p" -> pseudo;
            ~"r" -> range;
            ~"m" -> multirange;
            _ -> other
        end,
        recv => Recv,
        send => Send,
        element => case ElementType of 0 -> undefined; _ -> ElementType end,
        parent => case ParentType of 0 -> undefined; _ -> ParentType end,
        fields => case FieldNames of [] -> []; _ -> lists:zip(FieldNames, FieldTypes) end
    }, Types).


refresh_statement_text() ->
     ~"""
        select
            pg_type.oid as oid,
            pg_namespace.nspname::text as namespace,
            pg_type.typname::text as name,
            pg_type.typtype::text as type,
            pg_type.typsend::text as send,
            pg_type.typreceive::text as recv,
            pg_type.typelem as element_type,
            coalesce(pg_range.rngsubtype, nullif(pg_type.typbasetype, 0), 0) as parent_type,
            array (
                select pg_attribute.attname::text
                from pg_attribute
                where pg_attribute.attrelid = pg_type.typrelid
                and pg_attribute.attnum > 0
                and not pg_attribute.attisdropped
                order by pg_attribute.attnum
            ) as fields_names,
            array (
                select pg_attribute.atttypid
                from pg_attribute
                where pg_attribute.attrelid = pg_type.typrelid
                and pg_attribute.attnum > 0
                and not pg_attribute.attisdropped
                order by pg_attribute.attnum
            ) as fields_types
        from pg_catalog.pg_type
        left join pg_catalog.pg_range on pg_range.rngtypid = pg_type.oid or pg_range.rngmultitypid = pg_type.oid
        left join pg_catalog.pg_namespace on pg_namespace.oid = pg_type.typnamespace
    """.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

test_state(RequestList, Statements) ->
    #state{types = pgc_client_types:new(), requests = queue:from_list(RequestList), statements = Statements}.

test_request(Id, StatementText, Phase, Kind) ->
    #request{id = Id, statement_name = ~"", statement_text = StatementText, phase = Phase, kind = Kind, monitor = erlang:monitor(process, self())}.

-doc """
A `refresh` request that errors out (instead of completing) must not try to reply to anything --
it has no caller, `id` isn't an alias -- and must still let the paused request behind it start.
""".
handle_prepare_result_refresh_error_test() ->
    RefreshId = make_ref(),
    PausedId = make_ref(),
    Refresh = test_request(RefreshId, ~"select ...", preparing, refresh),
    Paused = test_request(PausedId, ~"select $1", preparing, execute),
    State = test_state([Refresh, Paused], #{}),
    {Actions, NewState} = handle_prepare_result(#{}, RefreshId, {error, #{}}, State),
    % The paused request is unnamed with nothing cached -- it should now be dispatched fresh.
    ?assertEqual([{prepare, PausedId, ~"", ~"select $1"}], Actions),
    ?assertEqual(1, queue:len(NewState#state.requests)),
    ?assertMatch({value, #request{id = PausedId, phase = preparing}}, queue:peek(NewState#state.requests)),
    ?assertEqual(timeout, receive Message -> Message after 0 -> timeout end).

-doc """
A `refresh` request that completes successfully also has nothing to reply to -- just lets the
paused request behind it start (with the exact same cache-hit path a fresh dispatch would take
for anything already sitting in `Statements`).
""".
handle_execute_result_refresh_success_test() ->
    RefreshId = make_ref(),
    PausedId = make_ref(),
    Refresh = test_request(RefreshId, ~"select ...", executing, refresh),
    Paused = test_request(PausedId, ~"select 1", preparing, execute),
    State = test_state([Refresh, Paused], #{}),
    {Actions, NewState} = handle_execute_result(#{}, RefreshId, {ok, ~"SELECT 1"}, State),
    ?assertEqual([{prepare, PausedId, ~"", ~"select 1"}], Actions),
    ?assertMatch({value, #request{id = PausedId}}, queue:peek(NewState#state.requests)),
    ?assertEqual(timeout, receive Message -> Message after 0 -> timeout end).

-doc """
Rows are unreachable for a `query`-kind request in current usage (`execute_simple/2`'s own
statements never return any), but nothing rules it out -- must be dropped, not crash the
connection with a missing case clause.
""".
handle_row_data_query_test() ->
    Id = make_ref(),
    Req = test_request(Id, ~"select 1", executing, query),
    State = test_state([Req], #{}),
    ?assertEqual({[], State}, handle_row_data(#{}, Id, [], [], State)).

-doc """
Cancelling a request that's merely waiting its turn (never dispatched, no `#request{}` ever sent
anywhere) just drops it -- no wire action, the running request is left alone.
""".
handle_cast_cancel_queued_test() ->
    RunningId = make_ref(),
    QueuedId = make_ref(),
    Running = test_request(RunningId, ~"select 1", executing, execute),
    Queued = test_request(QueuedId, ~"select 2", queued, execute),
    State = test_state([Running, Queued], #{}),
    {Actions, NewState} = handle_cast(#{}, {cancel, QueuedId}, State),
    ?assertEqual([], Actions),
    ?assertEqual([RunningId], [Id || #request{id = Id} <- queue:to_list(NewState#state.requests)]).

-doc """
A cancel landing while a cache-collision unprepare is in flight (see `dispatch_execute/3`)
aborts the Close before Postgres runs it, so Postgres reports it as an error rather than a
CloseComplete -- this should abandon the request rather than assuming the close succeeded and
blindly retrying the prepare nobody's waiting on anymore.
""".
handle_unprepare_result_cancelled_test() ->
    % A plain make_ref/0 isn't a valid send target, and monitoring self() doesn't mint a
    % working alias either -- mirror `request/5`'s production pattern of monitoring some *other*
    % process, so `Ref ! Message` has a real alias to deliver into this mailbox.
    Standin = spawn(fun () -> receive stop -> ok end end),
    Ref = erlang:monitor(process, Standin, [{alias, demonitor}]),
    Req = #request{id = Ref, statement_name = ~"my_statement", statement_text = ~"select 1", phase = unpreparing, kind = execute, monitor = erlang:monitor(process, self())},
    % The old statement (whatever it was before this collision-driven unprepare was sent) is
    % still live on the wire since the Close never actually ran -- `Statements` must still
    % reflect that, not have already dropped it when the unprepare was merely dispatched.
    Cached = #statement{hash = ~"old-hash", parameters_description = [], row_description = []},
    State = test_state([Req], #{~"my_statement" => Cached}),
    {Actions, NewState} = handle_unprepare_result(#{}, Ref, {error, #{}}, State),
    ?assertEqual([], Actions),
    ?assertEqual(true, queue:is_empty(NewState#state.requests)),
    ?assertEqual(#{~"my_statement" => Cached}, NewState#state.statements),
    ?assertEqual({done, Ref, {error, #{}}}, receive Message -> Message after 0 -> timeout end),
    erlang:demonitor(Ref, [flush]),
    Standin ! stop.
-endif.
