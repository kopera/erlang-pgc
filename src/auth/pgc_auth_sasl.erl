-module(pgc_auth_sasl).
-moduledoc false.

-callback init(Args) -> {ok, InitialResponse, State}
    when
        Args :: list(),
        InitialResponse :: iodata() | undefined,
        State :: term().
-callback continue(Data, State) -> ok | {ok, Response, State} | {error, Error}
    when
        Data :: list(),
        Response :: iodata(),
        Error :: term(),
        State :: term().
