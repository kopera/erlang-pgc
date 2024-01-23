%% @private
-module(pgc_auth_scram).
-export([
    init/3,
    continue/2
]).

-define(gs2_header, <<"n,,">>).

-record(scram, {
    hashing_algorithm :: sha256,
    username :: unicode:unicode_binary(),
    password_fun :: fun(() -> unicode:unicode_binary()),
    c_nonce :: binary(),
    c_first_message_bare :: iodata(),
    s_signature :: binary() | undefined
}).

-type error() ::
    invalid_encoding |
    extensions_not_supported |
    invalid_proof |
    channel_bindings_dont_match |
    server_does_support_channel_binding |
    channel_binding_not_supported |
    unsupported_channel_binding_type |
    unknown_user |
    invalid_username_encoding |
    no_resources |
    other_error.


init(HashingAlgorithm, Username, PasswordFun) ->
    ClientNonce = crypto:strong_rand_bytes(24),
    ClientFirstMessageBare = encode_attributes([
        {username, Username},
        {nonce, ClientNonce}
    ]),
    {ok, [?gs2_header | ClientFirstMessageBare], #scram{
        hashing_algorithm = HashingAlgorithm,
        username = Username,
        password_fun = PasswordFun,
        c_nonce = ClientNonce,
        c_first_message_bare = ClientFirstMessageBare
    }}.

continue(ServerFirstMessage, #scram{s_signature = undefined} = State) ->
    #scram{
        hashing_algorithm = HashingAlgorithm,
        password_fun = PasswordFun,
        c_nonce = ClientNonce,
        c_first_message_bare = ClientFirstMessageBare
    } = State,
    ClientNonceLength = erlang:byte_size(ClientNonce),
    case decode_attributes(ServerFirstMessage) of
        #{error := Error} ->
            {error, Error};
        #{nonce := <<ClientNonce:ClientNonceLength/binary, _/binary>> = ServerNonce, salt := Salt, iterations := Iterations} ->
            ClientFinalMessageWithoutProof = encode_attributes([
                {binding, ?gs2_header},
                {nonce, ServerNonce}
            ]),
            Password = pgc_string:to_binary(PasswordFun()),
            SaltedPassword = hi(HashingAlgorithm, Password, Salt, Iterations),
            ClientKey = crypto:mac(hmac, HashingAlgorithm, SaltedPassword, <<"Client Key">>),
            StoredKey = crypto:hash(HashingAlgorithm, ClientKey),
            AuthMessage = [ClientFirstMessageBare, ",", ServerFirstMessage, ",", ClientFinalMessageWithoutProof],
            ClientSignature = crypto:mac(hmac, HashingAlgorithm, StoredKey, AuthMessage),
            ClientProof = crypto:exor(ClientKey, ClientSignature),
            ServerKey = crypto:mac(hmac, HashingAlgorithm, SaltedPassword, <<"Server Key">>),
            ServerSignature = crypto:mac(hmac, HashingAlgorithm, ServerKey, AuthMessage),
            ClientFinalMessage = encode_attributes([
                {binding, ?gs2_header},
                {nonce, ServerNonce},
                {proof, ClientProof}
            ]),
            {ok, ClientFinalMessage, State#scram{s_signature = ServerSignature}}
    end;
continue(ServerFinalMessage, #scram{s_signature = ServerSignature}) when is_binary(ServerSignature) ->
    case decode_attributes(ServerFinalMessage) of
        #{error := Error} ->
            {error, Error};
        #{verifier := Verifier} ->
            case crypto:hash_equals(ServerSignature, Verifier) of
                true ->
                    ok;
                false ->
                    {error, invalid_proof}
            end
    end.


% ------------------------------------------------------------------------------
% Codecs
% ------------------------------------------------------------------------------

%% @private
decode_attributes(String) ->
    Tokens = binary:split(String, <<",">>, [global]),
    maps:from_list([decode_attribute(Token) || Token <- Tokens]).

%% @private
decode_attribute(<<"n=", Username/binary>>) -> {username, decode_username(Username)};
decode_attribute(<<"r=", Nonce/binary>>) -> {nonce, base64:decode(Nonce)};
decode_attribute(<<"c=", Binding/binary>>) -> {binding, base64:decode(Binding)};
decode_attribute(<<"s=", Salt/binary>>) -> {salt, base64:decode(Salt)};
decode_attribute(<<"i=", Iterations/binary>>) -> {iterations, binary_to_integer(Iterations)};
decode_attribute(<<"p=", Proof/binary>>) -> {proof, base64:decode(Proof)};
decode_attribute(<<"v=", Verifier/binary>>) -> {verifier, base64:decode(Verifier)};
decode_attribute(<<"e=", Error/binary>>) -> {error, decode_error(Error)}.


%% @private
encode_attributes(Attributes) when is_list(Attributes) ->
    lists:join($,, [encode_attribute(Name, Value) || {Name, Value} <- Attributes]).


%% @private
encode_attribute(username, Username) -> [<<"n=">>, encode_username(Username)];
encode_attribute(nonce, Nonce) -> [<<"r=">>, base64:encode(Nonce)];
encode_attribute(binding, Binding) -> [<<"c=">>, base64:encode(Binding)];
encode_attribute(salt, Salt) -> [<<"s=">>, base64:encode(Salt)];
encode_attribute(iterations, Iterations) -> [<<"i=">>, integer_to_binary(Iterations)];
encode_attribute(proof, Proof) -> [<<"p=">>, base64:encode(Proof)];
encode_attribute(verifier, Verifier) -> [<<"v=">>, base64:encode(Verifier)];
encode_attribute(error, Error) -> [<<"e=">>, encode_error(Error)].


%
% Username codec
%


%% @private
-spec decode_username(binary()) -> binary().
decode_username(Data) ->
    case decode_username(Data, <<>>) of
        error -> erlang:error(invalid_username_encoding, [Data]);
        {ok, Username} -> Username
    end.

%% @private
decode_username(<<"=2C", Rest/binary>>, Acc) ->
    decode_username(Rest, <<Acc/binary, $,>>);
decode_username(<<"=3D", Rest/binary>>, Acc) ->
    decode_username(Rest, <<Acc/binary, $=>>);
decode_username(<<$,, _Rest/binary>>, _Acc) ->
    error;
decode_username(<<$=, _Rest/binary>>, _Acc) ->
    error;
decode_username(<<C/utf8, Rest/binary>>, Acc) ->
    decode_username(Rest, <<Acc/binary, C/utf8>>);
decode_username(<<>>, Acc) ->
    {ok, Acc}.


%% @private
-spec encode_username(unicode:chardata()) -> binary().
encode_username(Username) ->
    encode_username(pgc_string:to_binary(Username), <<>>).

%% @private
encode_username(<<$,, Rest/binary>>, Acc) ->
    encode_username(Rest, <<Acc/binary, "=2C">>);
encode_username(<<$=, Rest/binary>>, Acc) ->
    encode_username(Rest, <<Acc/binary, "=3D">>);
encode_username(<<C/utf8, Rest/binary>>, Acc) ->
    encode_username(Rest, <<Acc/binary, C/utf8>>);
encode_username(<<>>, Acc) ->
    Acc.


%
% Errors codec
%

%% @private
-spec decode_error(binary()) -> error().
decode_error(<<"invalid-encoding">>) -> invalid_encoding;
decode_error(<<"extensions-not-supported">>) -> extensions_not_supported;
decode_error(<<"invalid-proof">>) -> invalid_proof;
decode_error(<<"channel-bindings-dont-match">>) -> channel_bindings_dont_match;
decode_error(<<"server-does-support-channel-binding">>) -> server_does_support_channel_binding;
decode_error(<<"channel-binding-not-supported">>) -> channel_binding_not_supported;
decode_error(<<"unsupported-channel-binding-type">>) -> unsupported_channel_binding_type;
decode_error(<<"unknown-user">>) -> unknown_user;
decode_error(<<"invalid-username-encoding">>) -> invalid_username_encoding;
decode_error(<<"no-resources">>) -> no_resources;
decode_error(<<"other-error">>) -> other_error;
decode_error(_) -> other_error.


%% @private
-spec encode_error(error()) -> binary().
encode_error(invalid_encoding)-> <<"invalid-encoding">>;
encode_error(extensions_not_supported)-> <<"extensions-not-supported">>;
encode_error(invalid_proof)-> <<"invalid-proof">>;
encode_error(channel_bindings_dont_match)-> <<"channel-bindings-dont-match">>;
encode_error(server_does_support_channel_binding)-> <<"server-does-support-channel-binding">>;
encode_error(channel_binding_not_supported)-> <<"channel-binding-not-supported">>;
encode_error(unsupported_channel_binding_type)-> <<"unsupported-channel-binding-type">>;
encode_error(unknown_user)-> <<"unknown-user">>;
encode_error(invalid_username_encoding)-> <<"invalid-username-encoding">>;
encode_error(no_resources)-> <<"no-resources">>;
encode_error(other_error)-> <<"other-error">>.


% ------------------------------------------------------------------------------
% Crypto
% ------------------------------------------------------------------------------

-spec hi(sha256, binary(), binary(), pos_integer()) -> binary().
hi(HashingAlgorithm, Password, Salt, Iterations) ->
    #{size := KeyLen} = crypto:hash_info(HashingAlgorithm),
    crypto:pbkdf2_hmac(HashingAlgorithm, Password, Salt, Iterations, KeyLen).