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
    handle_prepare_result/4,
    handle_unprepare_result/4,
    handle_query_result/4,
    handle_execute_result/4,
    handle_row_data/5
]).

-import_record(pgc_protocol_message, [row_description_field]).

-type request_phase() :: preparing | unpreparing | executing | querying | refreshing_types | awaiting_parameters.

-record #request{
    statement_text :: unicode:chardata(),
    phase :: request_phase()
}.

-record #statement{
    hash :: binary(),
    parameters_description :: [pgc_protocol:oid()],
    row_description :: [pgc_protocol_message:row_description_field()]
}.

-record #state{
    types :: pgc_client_types:t(),
    requests :: #{reference() => #request{}},
    statements :: #{unicode:unicode_binary() => #statement{}}
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
    },

    codecs => #{binary() => module()}
}.
-spec start_link(start_options()) -> pgc_connection:start_ret().
start_link(Options) ->
    pgc_connection:start_link(?MODULE, maps:get(codecs, Options, #{}), Options).


-spec start_link(pgc_connection:connection_name(), start_options()) -> pgc_connection:start_ret().
start_link(ClientName, Options) ->
    pgc_connection:start_link(ClientName, ?MODULE, maps:get(codecs, Options, #{}), Options).


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
        timeout => timeout(),
        cache => false | {true, Key :: string() | unicode:unicode_binary() | atom()},
        codecs => #{atom() => term()}
    },
    Metadata :: result_metadata(),
    Rows :: result_rows(),
    Error :: execute_error().
-type parameters() :: [term() | null].
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
        {continue, [format_row(RowFormat, RowDescription, Values) | Acc]}
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
    Fun :: fun((pgc_connection:row_description(), [term() | null], Acc) -> {continue, Acc} | {halt, Acc}),
    Options :: #{
        timeout => timeout(),
        cache => false | {true, Key :: string() | unicode:unicode_binary() | atom()},
        codecs => #{atom() => term()}
    },
    Metadata :: result_metadata(),
    Error :: execute_error().
execute(Connection, StatementText, Parameters, Fun, Acc, Options) ->
    Timeout = maps:get(timeout, Options, infinity),
    request(Connection, {execute, StatementText, Options}, Parameters, Options, Fun, Acc, Timeout).


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
    Fun = fun (_RowDescription, _Values, Acc) -> {continue, Acc} end,
    request(Connection, {query, StatementText}, [], #{}, Fun, [], infinity).


-doc """
Encoding and decoding are deliberately done here, in the caller's own process, rather than in
`handle_row_data/5`/`handle_prepare_result/4` (which run in the connection process): the
connection is a shared, serializing bottleneck, while `Types` (`pgc_client_types:t()`) is a
`protected` ets table specifically so any number of callers can read it -- and therefore encode
and decode -- concurrently, off the connection's own execution stack. The connection only ever
mediates the wire and owns the (write side of the) type cache.
""".
request(Connection, Request, Parameters, Options, Fun, Acc, Timeout) ->
    Ref = erlang:monitor(process, Connection, [{alias, demonitor}]),
    try
        ok = pgc_connection:cast(Connection, {request, Ref, Request}),
        collect(Connection, Ref, Parameters, Options, Fun, Acc, pgc_deadline:from_timeout(Timeout))
    after
        erlang:demonitor(Ref, [flush])
    end.

-doc false.
collect(Connection, Ref, Parameters, Options, Fun, Acc, Deadline) ->
    receive
        {encode_parameters, Ref, Name, ParametersDescription, Types} ->
            % Sent exactly once, right after the statement's parameter oids become known
            % (fresh prepare or cache hit) -- encode locally, then hand the connection the
            % finished bytes so it can actually dispatch `execute`.
            CallTypes = pgc_client_types:with_options(Types, maps:get(codecs, Options, #{})),
            EncodedParameters = encode_parameters(ParametersDescription, Parameters, CallTypes),
            ok = pgc_connection:cast(Connection, {parameters, Ref, Name, EncodedParameters}),
            collect(Connection, Ref, Parameters, Options, Fun, Acc, Deadline);
        {row, Ref, RowDescription, Values, Types} ->
            CallTypes = pgc_client_types:with_options(Types, maps:get(codecs, Options, #{})),
            DecodedValues = decode_values(RowDescription, Values, CallTypes),
            case Fun(RowDescription, DecodedValues, Acc) of
                {continue, Acc1} ->
                    collect(Connection, Ref, Parameters, Options, Fun, Acc1, Deadline);
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

encode_parameters(ParametersDescription, Parameters, Types) ->
    [
        {binary, pgc_client_codec:encode(Value, type_descriptor(Oid, Types), Types)}
        || {Oid, Value} <- lists:zip(ParametersDescription, Parameters)
    ].

decode_values(RowDescription, Values, Types) ->
    [
        pgc_client_codec:decode(Value, type_descriptor(Field#row_description_field.type_oid, Types), Types)
        || {Field, Value} <- lists:zip(RowDescription, Values)
    ].

type_descriptor(Oid, Types) ->
    {ok, Descriptor} = pgc_client_types:lookup(Oid, Types),
    Descriptor.


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
init(Codecs) ->
    {ok, #state{types = pgc_client_types:new(Codecs), requests = #{}, statements = #{}}}.


-doc false.
handle_cast(_ConnectionInfo, {request, Ref, {execute, StatementText, Options}}, #state{} = State) ->
    #state{requests = Requests, statements = Statements} = State,
    Req = #request{statement_text = StatementText, phase = preparing},
    NewState = State#state{requests = Requests#{Ref => Req}},
    case maps:get(cache, Options, false) of
        false ->
            {[{prepare, Ref, ~"", StatementText}], NewState};
        {true, Key} ->
            Name = cache_statement_name(Key),
            Hash = statement_hash(StatementText),
            case Statements of
                #{Name := #statement{hash = Hash, parameters_description = ParametersDescription}} ->
                    dispatch_execute(Ref, Name, ParametersDescription, State#state.types, NewState);
                #{Name := #statement{}} ->
                    % Different text under the same name -- close it first, but leave
                    % `Statements` alone until handle_unprepare_result/4 confirms the Close
                    % actually happened (a cancel can abort it before Postgres runs it).
                    UnpreparingReq = Req#request{phase = unpreparing},
                    {[{unprepare, Ref, Name}], NewState#state{requests = Requests#{Ref => UnpreparingReq}}};
                #{} ->
                    {[{prepare, Ref, Name, StatementText}], NewState}
            end
    end;

handle_cast(_ConnectionInfo, {request, Ref, {query, StatementText}}, #state{} = State) ->
    #state{requests = Requests} = State,
    Req = #request{statement_text = StatementText, phase = querying},
    {[
        {query, Ref, StatementText}
    ], State#state{requests = Requests#{Ref => Req}}};

handle_cast(_ConnectionInfo, {parameters, Ref, Name, EncodedParameters}, #state{} = State) ->
    #state{requests = Requests} = State,
    #{Ref := Req} = Requests,
    NewState = State#state{requests = Requests#{Ref => Req#request{phase = executing}}},
    {[{execute, Ref, Name, EncodedParameters, #{result_format => binary}}], NewState};

handle_cast(_ConnectionInfo, {cancel, Ref}, #state{requests = Requests} = State) ->
    % A request `awaiting_parameters` has nothing on the wire to cancel, and the caller has
    % already given up -- it'll never send `{parameters, ...}`, so nothing else will ever remove
    % this entry. Drop it here instead of leaking it.
    NewRequests = case Requests of
        #{Ref := #request{phase = awaiting_parameters}} -> maps:remove(Ref, Requests);
        #{} -> Requests
    end,
    {[{cancel, Ref}], State#state{requests = NewRequests}}.


-doc false.
handle_prepare_result(_ConnectionInfo, Ref, {ok, Name, StatementDescription}, State) ->
    #state{requests = Requests, types = Types, statements = Statements} = State,
    #{Ref := Req} = Requests,
    #{parameters_description := ParametersDescription, row_description := RowDescription} = StatementDescription,
    NeededOids = ParametersDescription ++ [Field#row_description_field.type_oid || Field <- RowDescription],
    % The statement now exists on the wire under Name either way -- record it before
    % branching on whether a type refresh has to happen first.
    CachedStatement = #statement{
        hash = statement_hash(Req#request.statement_text),
        parameters_description = ParametersDescription,
        row_description = RowDescription
    },
    NewState = State#state{statements = Statements#{Name => CachedStatement}},
    case lists:all(fun (Oid) -> pgc_client_types:has(Oid, Types) end, NeededOids) of
        true ->
            dispatch_execute(Ref, Name, ParametersDescription, Types, NewState);
        false ->
            % Missing types -- refresh first, reusing this request's Ref so a cancel still
            % reaches it. handle_query_result/4 picks up from there.
            {[{query, Ref, refresh_statement_text()}],
                NewState#state{requests = Requests#{Ref => Req#request{phase = refreshing_types}}}}
    end;

handle_prepare_result(_ConnectionInfo, Ref, {error, Fields}, State) ->
    #state{requests = Requests} = State,
    {_, NewRequests} = maps:take(Ref, Requests),
    Ref ! {done, Ref, {error, Fields}},
    {[], State#state{requests = NewRequests}}.


% No parameters -- nothing to hand off, dispatch directly. Otherwise the caller encodes its
% own parameters (see collect/7), and this request is `awaiting_parameters` until it replies.
dispatch_execute(Ref, Name, [], _Types, State) ->
    #state{requests = Requests} = State,
    #{Ref := Req} = Requests,
    NewState = State#state{requests = Requests#{Ref => Req#request{phase = executing}}},
    {[{execute, Ref, Name, [], #{result_format => binary}}], NewState};
dispatch_execute(Ref, Name, ParametersDescription, Types, State) ->
    #state{requests = Requests} = State,
    #{Ref := Req} = Requests,
    Ref ! {encode_parameters, Ref, Name, ParametersDescription, Types},
    {[], State#state{requests = Requests#{Ref => Req#request{phase = awaiting_parameters}}}}.


-doc false.
handle_unprepare_result(_ConnectionInfo, Ref, {ok, Name}, State) ->
    % Only ever reached to reclaim a name for a re-prepare with new text -- the Close is now
    % confirmed, so the name is free and `Statements` can drop the stale entry.
    #state{requests = Requests, statements = Statements} = State,
    #{Ref := Req} = Requests,
    NewReq = Req#request{phase = preparing},
    NewState = State#state{
        statements = maps:remove(Name, Statements),
        requests = Requests#{Ref => NewReq}
    },
    {[{prepare, Ref, Name, NewReq#request.statement_text}], NewState};

handle_unprepare_result(_ConnectionInfo, Ref, {error, Fields}, State) ->
    % Close didn't actually happen (a cancel beat it) -- the old statement is still live under
    % this name, so `Statements` is left untouched. Abandon the request like any other cancel.
    #state{requests = Requests} = State,
    {_, NewRequests} = maps:take(Ref, Requests),
    Ref ! {done, Ref, {error, Fields}},
    {[], State#state{requests = NewRequests}}.


-doc false.
handle_row_data(_ConnectionInfo, Ref, RowDescription, Values, State) ->
    #{Ref := Req} = State#state.requests,
    case Req#request.phase of
        refreshing_types ->
            [Oid, Namespace, Name, Kind, Recv, Send, ElementType, ParentType, FieldNamesArray, FieldTypesArray] = Values,
            TypeId = binary_to_integer(Oid),
            ok = pgc_client_types:add(TypeId, #{
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
                element => case ElementType of
                    ~"0" -> undefined;
                    _ -> binary_to_integer(ElementType)
                end,
                parent => case ParentType of
                    ~"0" -> undefined;
                    _ -> binary_to_integer(ParentType)
                end,
                fields => case FieldNamesArray of
                    ~"{}" -> [];
                    _ ->
                        FieldNames = decode_array(FieldNamesArray),
                        FieldTypes = [binary_to_integer(Type) || Type <- decode_array(FieldTypesArray)],
                        lists:zip(FieldNames, FieldTypes)
                end
            }, State#state.types),
            {[], State};
        executing ->
            % Decoding happens caller-side (see collect/7) -- just forward the raw wire
            % values plus the shared (protected, multi-reader) type cache they need.
            Ref ! {row, Ref, RowDescription, Values, State#state.types},
            {[], State}
    end.

-doc false.
handle_query_result(ConnectionInfo, Ref, Result, State) ->
    #{Ref := Req} = State#state.requests,
    case Req#request.phase of
        refreshing_types ->
            #state{requests = Requests} = State,
            case Result of
                {ok, _Tag} ->
                    % Refresh actually completed -- now it's safe to retry the prepare
                    % (the unnamed statement doesn't survive the refresh's own Query
                    % message, hence re-preparing rather than resuming the old one).
                    NewReq = Req#request{phase = preparing},
                    {[{prepare, Ref, ~"", NewReq#request.statement_text}],
                        State#state{requests = Requests#{Ref => NewReq}}};
                {error, Fields} ->
                    % Cancelled (or otherwise failed) mid-refresh -- abandon this request
                    % the same way a cancel during a real execute already does, instead of
                    % blindly retrying a prepare nobody's waiting on anymore.
                    Ref ! {done, Ref, {error, Fields}},
                    {[], State#state{requests = maps:remove(Ref, Requests)}}
            end;
        querying ->
            handle_execute_result(ConnectionInfo, Ref, Result, State)
    end.

-doc false.
handle_execute_result(_ConnectionInfo, Ref, Result, State) ->
    #state{requests = Requests} = State,
    {_, NewRequests} = maps:take(Ref, Requests),
    Ref ! {done, Ref, Result},
    {[], State#state{requests = NewRequests}}.


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

decode_array(<<"{}">>) ->
    [];
decode_array(<<"{", Rest/binary>>) ->
    decode_array_(Rest).

decode_array_(Bin) ->
    case decode_array_element(Bin) of
        {Element, ~"}"} -> [Element];
        {Element, <<",", Tail/binary>>} -> [Element | decode_array_(Tail)]
    end.

decode_array_element(<<"\"", Rest/binary>>) ->
    decode_array_element_quoted(Rest, <<>>);
decode_array_element(Bin) ->
    decode_array_element_unquoted(Bin, <<>>).

%% Extract a quoted element
decode_array_element_quoted(<<"\"", Rest/binary>>, Acc) ->
    {Acc, Rest}; % Return the element and whatever binary is left
decode_array_element_quoted(<<"\\", Char, Rest/binary>>, Acc) ->
    decode_array_element_quoted(Rest, <<Acc/binary, Char>>);
decode_array_element_quoted(<<Char, Rest/binary>>, Acc) ->
    decode_array_element_quoted(Rest, <<Acc/binary, Char>>).

%% Extract an unquoted element
decode_array_element_unquoted(<<"}", _/binary>> = Rest, Acc) ->
    {Acc, Rest};
decode_array_element_unquoted(<<",", _/binary>> = Rest, Acc) ->
    {Acc, Rest};
decode_array_element_unquoted(<<Char, Rest/binary>>, Acc) ->
    decode_array_element_unquoted(Rest, <<Acc/binary, Char>>).


cache_statement_name(Key) when is_atom(Key) ->
    atom_to_binary(Key);
cache_statement_name(Key) ->
    to_binary(Key).

statement_hash(StatementText) ->
    crypto:hash(sha256, to_binary(StatementText)).

to_binary(Chardata) ->
    case unicode:characters_to_binary(Chardata) of
        Binary when is_binary(Binary) -> Binary;
        {error, _, _} -> erlang:error(badarg, [Chardata]);
        {incomplete, _, _} -> erlang:error(badarg, [Chardata])
    end.

refresh_statement_text() ->
     ~"""
        select
            pg_type.oid as oid,
            pg_namespace.nspname as namespace,
            pg_type.typname as name,
            pg_type.typtype as type,
            pg_type.typsend as send,
            pg_type.typreceive as recv,
            pg_type.typelem as element_type,
            coalesce(pg_range.rngsubtype, nullif(pg_type.typbasetype, 0), 0) as parent_type,
            array (
                select pg_attribute.attname
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
        left join pg_catalog.pg_range on pg_range.rngtypid = pg_type.oid
        left join pg_catalog.pg_namespace on pg_namespace.oid = pg_type.typnamespace
    """.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-doc """
A cancel landing while a types-refresh is in flight aborts the refresh (not the caller's
actual statement, which hasn't been re-issued yet) -- this should abandon the request the
same way a cancel during a real execute does, not blindly retry the prepare nobody's
waiting on anymore.
""".
handle_query_result_refresh_cancelled_test() ->
    % A plain make_ref/0 isn't a valid send target, and monitoring self() doesn't mint a
    % working alias either -- mirror `request/5`'s production pattern of monitoring some *other*
    % process, so `Ref ! Message` has a real alias to deliver into this mailbox.
    Standin = spawn(fun () -> receive stop -> ok end end),
    Ref = erlang:monitor(process, Standin, [{alias, demonitor}]),
    Req = #request{statement_text = ~"select 1", phase = refreshing_types},
    State = #state{types = pgc_client_types:new(), requests = #{Ref => Req}, statements = #{}},
    {Actions, NewState} = handle_query_result(#{}, Ref, {error, #{}}, State),
    ?assertEqual([], Actions),
    ?assertEqual(#{}, NewState#state.requests),
    ?assertEqual({done, Ref, {error, #{}}}, receive Message -> Message after 0 -> timeout end),
    erlang:demonitor(Ref, [flush]),
    Standin ! stop.

-doc """
A refresh that actually completes retries the prepare, since the unnamed statement doesn't
survive the refresh's own Query message.
""".
handle_query_result_refresh_succeeded_test() ->
    Ref = make_ref(),
    Req = #request{statement_text = ~"select 1", phase = refreshing_types},
    State = #state{types = pgc_client_types:new(), requests = #{Ref => Req}, statements = #{}},
    {Actions, NewState} = handle_query_result(#{}, Ref, {ok, ~"SELECT 1"}, State),
    ?assertEqual([{prepare, Ref, ~"", ~"select 1"}], Actions),
    ?assertEqual(#{Ref => Req#request{phase = preparing}}, NewState#state.requests).

-doc """
A cancel landing while a cache-collision unprepare is in flight (see the cache-collision
branch of handle_cast/3) aborts the Close before Postgres runs it, so Postgres reports it as
an error rather than a CloseComplete -- this should abandon the request rather than assuming
the close succeeded and blindly retrying the prepare nobody's waiting on anymore.
""".
handle_unprepare_result_cancelled_test() ->
    Standin = spawn(fun () -> receive stop -> ok end end),
    Ref = erlang:monitor(process, Standin, [{alias, demonitor}]),
    Req = #request{statement_text = ~"select 1", phase = unpreparing},
    % The old statement (whatever it was before this collision-driven unprepare was sent) is
    % still live on the wire since the Close never actually ran -- `prepared` must still
    % reflect that, not have already dropped it when the unprepare was merely dispatched.
    Cached = #statement{hash = ~"old-hash", parameters_description = [], row_description = []},
    State = #state{types = pgc_client_types:new(), requests = #{Ref => Req}, statements = #{~"my_statement" => Cached}},
    {Actions, NewState} = handle_unprepare_result(#{}, Ref, {error, #{}}, State),
    ?assertEqual([], Actions),
    ?assertEqual(#{}, NewState#state.requests),
    ?assertEqual(#{~"my_statement" => Cached}, NewState#state.statements),
    ?assertEqual({done, Ref, {error, #{}}}, receive Message -> Message after 0 -> timeout end),
    erlang:demonitor(Ref, [flush]),
    Standin ! stop.
-endif.
