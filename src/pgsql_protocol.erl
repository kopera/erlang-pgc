-module(pgsql_protocol).
-export([
    encode_messages/1,
    decode_messages/1
]).

-spec encode_messages([pgsql_protocol_messages:message()]) -> iolist().
encode_messages(Messages) ->
    [encode_message(Message) || Message <- Messages].

encode_message(Message) ->
    {Type, Payload} = pgsql_protocol_messages:encode(Message),
    [Type, <<(iolist_size(Payload) + 4):32/integer>>, Payload].

-spec decode_messages(binary()) -> {[pgsql_protocol_messages:message()], Rest :: binary()}.
decode_messages(Data) ->
    decode_messages(Data, []).

decode_messages(Data, Acc) ->
    case decode_message(Data) of
        {ok, Message, Rest} ->
            decode_messages(Rest, [Message | Acc]);
        {incomplete, _} ->
            {lists:reverse(Acc), Data}
    end.

decode_message(<<Code:8/integer, Size:32/integer, Available/binary>>) ->
    PayloadSize = Size - 4,
    AvailableSize = byte_size(Available),
    if
        PayloadSize =< AvailableSize ->
            <<Payload:PayloadSize/binary, Rest/binary>> = Available,
            {ok, pgsql_protocol_messages:decode(Code, Payload), Rest};
        PayloadSize > AvailableSize ->
            {incomplete, PayloadSize - AvailableSize}
    end;
decode_message(Data) ->
    {incomplete, 5 - byte_size(Data)}.
