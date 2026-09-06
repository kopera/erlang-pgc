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

-record #req{
    ref :: reference(),
    statement_text :: unicode:chardata(),
    parameters :: pgc_connection:execute_parameters(),
    refreshing_types :: boolean()
}.

-record #state{
    types :: pgc_client_types:t(),
    pending :: #{reference() => #req{}},
    prepared :: #{unicode:unicode_binary() => binary()}
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
        timeout => timeout(),
        cache => false | {true, Key :: string() | unicode:unicode_binary() | atom()}
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
    Fun :: fun((pgc_connection:row_description(), [null | binary()], Acc) -> {continue, Acc} | {halt, Acc}),
    Options :: #{
        timeout => timeout(),
        cache => false | {true, Key :: string() | unicode:unicode_binary() | atom()}
    },
    Metadata :: result_metadata(),
    Error :: execute_error().
execute(Connection, StatementText, Parameters, Fun, Acc, Options) ->
    Timeout = maps:get(timeout, Options, infinity),
    Cache = maps:get(cache, Options, false),
    request(Connection, {execute, StatementText, Parameters, Cache}, Fun, Acc, Timeout).


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
    request(Connection, {query, StatementText}, Fun, [], infinity).


-doc false.
request(Connection, Request, Fun, Acc, Timeout) ->
    Ref = erlang:monitor(process, Connection, [{alias, demonitor}]),
    try
        ok = pgc_connection:cast(Connection, {request, Ref, Request}),
        collect(Connection, Ref, Fun, Acc, pgc_deadline:from_timeout(Timeout))
    after
        erlang:demonitor(Ref, [flush])
    end.

-doc false.
collect(Connection, Ref, Fun, Acc, Deadline) ->
    receive
        {row, Ref, RowDescription, Values} ->
            case Fun(RowDescription, Values, Acc) of
                {continue, Acc1} ->
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
    {ok, #state{types = pgc_client_types:new(), pending = #{}, prepared = #{}}}.


-doc false.
handle_cast(_ConnectionInfo, {request, Ref, {execute, StatementText, Parameters, Cache}}, #state{} = State) ->
    #state{pending = Pending, prepared = Prepared} = State,
    Req = #req{
        ref = Ref,
        statement_text = StatementText,
        parameters = [{text, Parameter} || Parameter <- Parameters],
        refreshing_types = false
    },
    NewState = State#state{pending = Pending#{Ref => Req}},
    case Cache of
        false ->
            {[{prepare, Ref, ~"", StatementText}], NewState};
        {true, Key} ->
            Name = cache_statement_name(Key),
            Hash = statement_hash(StatementText),
            case Prepared of
                #{Name := Hash} ->
                    % Already prepared under this name with this exact text -- skip straight
                    % to execute, no parse/describe round-trip needed.
                    {[{execute, Ref, Name, Req#req.parameters, #{}}], NewState};
                #{Name := _OtherHash} ->
                    % Same cache key, different statement text -- the old prepared statement
                    % must be closed before the name can be reused. Drop it from the cache now
                    % rather than after confirmation: Close always succeeds at the wire level,
                    % and nothing else can run concurrently to observe the stale entry.
                    {[{unprepare, Ref, Name}], NewState#state{prepared = maps:remove(Name, Prepared)}};
                #{} ->
                    {[{prepare, Ref, Name, StatementText}], NewState}
            end
    end;

handle_cast(_ConnectionInfo, {request, Ref, {query, StatementText}}, #state{} = State) ->
    #state{pending = Pending} = State,
    Req = #req{
        ref = Ref,
        statement_text = StatementText,
        parameters = [],
        refreshing_types = false
    },
    {[
        {query, Ref, StatementText}
    ], State#state{pending = Pending#{Ref => Req}}};

handle_cast(_ConnectionInfo, {cancel, Ref}, #state{} = State) ->
    % Whether this actually interrupts anything is pgc_connection's call to make -- it tracks
    % which Ref is genuinely on the wire, we don't need our own head-of-queue guess here.
    {[{cancel, Ref}], State}.


-doc false.
handle_prepare_result(_ConnectionInfo, Ref, {ok, Name, StatementDescription}, State) ->
    #state{pending = Pending, types = Types, prepared = Prepared} = State,
    #{Ref := Req} = Pending,
    #{parameters_description := ParameterOids, row_description := RowFields} = StatementDescription,
    NeededOids = ParameterOids ++ [Field#row_description_field.type_oid || Field <- RowFields],
    % The statement now exists on the wire under Name either way -- record it before
    % branching on whether a type refresh has to happen first.
    NewState = State#state{prepared = Prepared#{Name => statement_hash(Req#req.statement_text)}},
    case lists:all(fun (Oid) -> pgc_client_types:has(Oid, Types) end, NeededOids) of
        true ->
            {[{execute, Ref, Name, Req#req.parameters, #{}}], NewState};
        false ->
            % Some of the types this statement needs aren't cached yet -- refresh first;
            % handle_query_result/4 below decides what to do once it's actually done,
            % rather than pre-committing to a re-prepare that a cancel can't then stop.
            % Reuses this request's own Ref -- from pgc_connection's point of view the
            % refresh's `query` action *is* this request's currently in-flight action,
            % which is what lets a caller's cancel interrupt it.
            {[{query, Ref, refresh_statement_text()}],
                NewState#state{pending = Pending#{Ref => Req#req{refreshing_types = true}}}}
    end;

handle_prepare_result(_ConnectionInfo, Ref, {error, Fields}, State) ->
    #state{pending = Pending} = State,
    #{Ref := Req} = Pending,
    Req#req.ref ! {done, Req#req.ref, {error, Fields}},
    {[], State#state{pending = maps:remove(Ref, Pending)}}.


-doc false.
handle_unprepare_result(_ConnectionInfo, Ref, {ok, Name}, State) ->
    % pgc_client only ever unprepares a statement to reclaim its name for a re-prepare with
    % new text (see the cache-collision branch of handle_cast/3 above) -- so this is always
    % followed by a prepare, never a terminal result on its own.
    #state{pending = Pending} = State,
    #{Ref := Req} = Pending,
    {[{prepare, Ref, Name, Req#req.statement_text}], State}.


-doc false.
handle_row_data(_ConnectionInfo, Ref, RowDescription, Values, State) ->
    #{Ref := Req} = State#state.pending,
    case Req#req.refreshing_types of
        true ->
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
        false ->
            Req#req.ref ! {row, Req#req.ref, RowDescription, Values},
            {[], State}
    end.

-doc false.
handle_query_result(ConnectionInfo, Ref, Result, State) ->
    #{Ref := Req} = State#state.pending,
    case Req#req.refreshing_types of
        true ->
            #state{pending = Pending} = State,
            case Result of
                {ok, _Tag} ->
                    % Refresh actually completed -- now it's safe to retry the prepare
                    % (the unnamed statement doesn't survive the refresh's own Query
                    % message, hence re-preparing rather than resuming the old one).
                    NewReq = Req#req{refreshing_types = false},
                    {[{prepare, Ref, ~"", NewReq#req.statement_text}],
                        State#state{pending = Pending#{Ref => NewReq}}};
                {error, Fields} ->
                    % Cancelled (or otherwise failed) mid-refresh -- abandon this request
                    % the same way a cancel during a real execute already does, instead of
                    % blindly retrying a prepare nobody's waiting on anymore.
                    Req#req.ref ! {done, Req#req.ref, {error, Fields}},
                    {[], State#state{pending = maps:remove(Ref, Pending)}}
            end;
        false ->
            handle_execute_result(ConnectionInfo, Ref, Result, State)
    end.

-doc false.
handle_execute_result(_ConnectionInfo, Ref, Result, State) ->
    #state{pending = Pending} = State,
    #{Ref := Req} = Pending,
    Req#req.ref ! {done, Req#req.ref, Result},
    {[], State#state{pending = maps:remove(Ref, Pending)}}.


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
            coalesce(pg_range.rngsubtype, 0) as parent_type,
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
    % process, so `Req#req.ref ! Message` has a real alias to deliver into this mailbox.
    Standin = spawn(fun () -> receive stop -> ok end end),
    Ref = erlang:monitor(process, Standin, [{alias, demonitor}]),
    Req = #req{ref = Ref, statement_text = ~"select 1", parameters = [], refreshing_types = true},
    State = #state{types = pgc_client_types:new(), pending = #{Ref => Req}, prepared = #{}},
    {Actions, NewState} = handle_query_result(#{}, Ref, {error, #{}}, State),
    ?assertEqual([], Actions),
    ?assertEqual(#{}, NewState#state.pending),
    ?assertEqual({done, Ref, {error, #{}}}, receive Message -> Message after 0 -> timeout end),
    erlang:demonitor(Ref, [flush]),
    Standin ! stop.

-doc """
A refresh that actually completes retries the prepare, since the unnamed statement doesn't
survive the refresh's own Query message.
""".
handle_query_result_refresh_succeeded_test() ->
    Ref = make_ref(),
    Req = #req{ref = Ref, statement_text = ~"select 1", parameters = [], refreshing_types = true},
    State = #state{types = pgc_client_types:new(), pending = #{Ref => Req}, prepared = #{}},
    {Actions, NewState} = handle_query_result(#{}, Ref, {ok, ~"SELECT 1"}, State),
    ?assertEqual([{prepare, Ref, ~"", ~"select 1"}], Actions),
    ?assertEqual(#{Ref => Req#req{refreshing_types = false}}, NewState#state.pending).
-endif.
