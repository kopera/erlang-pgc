-module(pgc_client).
-export([
    start_link/1,
    start_link/2,
    stop/1
]).
-export([
    % execute/3,
    % execute/4,
    % transaction/3,
    % rollback/2
]).

-behaviour(pgc_connection).
-export([
    init/1
]).

-record #state{}.

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


% -spec execute(pgc_connection:connection_ref()) -> ok.
% execute(ConnectionRef, Statement, Parameters) ->
%     execute(ConnectionRef, Statement, Parameters, #{}).

% -spec execute(pgc_connection:connection_ref()) -> ok.
% execute(ConnectionRef, Statement, Parameters, Options) ->
%     pgc_connection:call(ConnectionRef, {execute, Statement, Parameters, Options}).

% -----------------------------------------------------------------------------
% pgc_connection behaviour
% -----------------------------------------------------------------------------

init([]) ->
    #state{}.
