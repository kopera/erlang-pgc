%% @private
-module(pgc_messages).
-export([
    encode/1,
    decode/1
]).


% @doc encode 1 or more protocol messages.
-spec encode([pgc_message:message_f() | pgc_message:message_fb()]) -> iolist().
encode(Messages) ->
    [encode_message(Message) || Message <- Messages].

encode_message(Message) ->
    {Type, Payload} = pgc_message:encode(Message),
    [Type, <<(iolist_size(Payload) + 4):32/integer>>, Payload].


% @doc decode 0 or more protocol messages.
-spec decode(binary()) -> {[pgc_message:t()], Rest :: binary()}.
decode(Data) ->
    {Messages, Rest} = decode(Data, []),
    {Messages, binary:copy(Rest)}.

decode(Data, Acc) ->
    case decode_message(Data) of
        {ok, Message, Rest} ->
            decode(Rest, [Message | Acc]);
        {incomplete, _} ->
            {lists:reverse(Acc), Data}
    end.

decode_message(<<Code:8/integer, Size:32/integer, Available/binary>>) ->
    PayloadSize = Size - 4,
    AvailableSize = byte_size(Available),
    if
        PayloadSize =< AvailableSize ->
            <<Payload:PayloadSize/binary, Rest/binary>> = Available,
            {ok, pgc_message:decode(Code, Payload), Rest};
        PayloadSize > AvailableSize ->
            {incomplete, PayloadSize - AvailableSize}
    end;
decode_message(Data) ->
    {incomplete, 5 - byte_size(Data)}.
