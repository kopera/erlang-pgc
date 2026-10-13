-module(pgc_protocol_messages).
-moduledoc false.

-export([
    encode/1,
    decode/1
]).

-doc "Encodes 1 or more protocol messages, framing each with its wire header.".
-spec encode([pgc_protocol_message:message_f() | pgc_protocol_message:message_fb()]) -> iolist().
encode(Messages) ->
    [encode_message(Message) || Message <- Messages].

encode_message(Message) ->
    {Type, Payload} = pgc_protocol_message:encode(Message),
    [Type, <<(iolist_size(Payload) + 4):32/integer>>, Payload].


-doc "Decodes 0 or more protocol messages out of `Data`, returning the messages decoded so far and the unconsumed remainder.".
-spec decode(binary()) -> {[pgc_protocol_message:t()], Rest :: binary()}.
decode(Data) ->
    case decode_message(Data) of
        {ok, Message, Rest} ->
            {Messages, FinalRest} = decode(Rest),
            {[Message | Messages], FinalRest};
        incomplete ->
            {[], Data}
    end.

-spec decode_message(binary()) -> {ok, pgc_protocol_message:t(), binary()} | incomplete.
decode_message(<<Code:8/integer, Size:32/integer, Available/binary>>) ->
    PayloadSize = Size - 4,
    case Available of
        <<Payload:PayloadSize/binary, Rest/binary>> ->
            {ok, pgc_protocol_message:decode(Code, Payload), Rest};
        _ ->
            incomplete
    end;
decode_message(_Data) ->
    incomplete.
