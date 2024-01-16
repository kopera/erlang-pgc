-ifndef(PGC_MESSAGE_HRL).
-define(PGC_MESSAGE_HRL, true).

-include("./pgc_row.hrl").

-record(msg_auth, {
    type ::  ok
        | kerberos
        | cleartext
        | md5
        | gss
        | gss_continue
        | sspi
        | sasl
        | sasl_continue
        | sasl_final
        | byte(),
    data = <<>> :: binary()
}).
-record(msg_backend_key_data, {
    id :: non_neg_integer(),
    secret :: non_neg_integer()
}).
-record(msg_bind, {
    portal = "" :: iodata(),
    statement = "" :: iodata(),
    parameters = [] :: [{binary | text, iodata() | null}],
    results = [] :: [binary | text]
}).
-record(msg_bind_complete, {
}).
-record(msg_cancel_request, {
    id = 0 :: non_neg_integer(),
    secret = 0 :: non_neg_integer()
}).
-record(msg_close, {
    type = statement :: statement | portal,
    name = "" :: iodata()
}).
-record(msg_close_complete, {
}).
-record(msg_command_complete, {
    tag = <<>> :: binary()
}).
-record(msg_copy_data, {
    data :: iodata()
}).
-record(msg_copy_done, {
}).
-record(msg_copy_fail, {
    message = <<>> :: binary()
}).
-record(msg_copy_in_response, {
    format :: text | binary,
    columns :: [text | binary]
}).
-record(msg_copy_out_response, {
    format :: text | binary,
    columns :: [text | binary]
}).
-record(msg_copy_both_response, {
    format :: text | binary,
    columns :: [text | binary]
}).
-record(msg_data_row, {
    count = 0 :: non_neg_integer(),
    values :: [null | binary()]
}).
-record(msg_describe, {
    type = statement :: statement | portal,
    name = "" :: iodata()
}).
-record(msg_empty_query_response, {
}).
-record(msg_error_response, {
    fields = #{} :: pgc_message:error_fields()
}).
-record(msg_execute, {
    portal = "" :: iodata(),
    limit = 0 :: non_neg_integer()
}).
-record(msg_flush, {
}).
% -record(msg_function_call, {
%     oid = 1 :: pgc_type:oid(),
%     parameters = [] :: [{binary | text, iodata()}],
%     result = text :: binary | text
% }).
% -record(msg_function_call_response, {
%     result = undefined :: undefined | binary()
% }).
-record(msg_no_data, {
}).
-record(msg_notice_response, {
    fields = #{} :: pgc_message:notice_fields()
}).
-record(msg_notification_response, {
    id = 0 :: non_neg_integer(),
    channel = <<>> :: binary(),
    payload = <<>> :: binary()
}).
-record(msg_parameter_description, {
    count = 0 :: non_neg_integer(),
    types = [] :: [pgc_type:oid()]
}).
-record(msg_parameter_status, {
    name = <<>> :: binary(),
    value = <<>> :: binary()
}).
-record(msg_parse, {
    name = "" :: iodata(),
    statement = "" :: iodata(),
    types = [] :: [pgc_type:oid() | 0]
}).
-record(msg_parse_complete, {
}).
-record(msg_password, {
    password = "" :: iodata()
}).
-record(msg_portal_suspended, {
}).
-record(msg_query, {
    query = "" :: iodata()
}).
-record(msg_ready_for_query, {
    status = idle :: idle | transaction | error
}).
-record(msg_row_description, {
    count :: non_neg_integer(),
    fields :: [#pgc_row_field{}]
}).
-record(msg_sasl_initial_response, {
    mechanism :: binary(),
    data :: iodata() | undefined
}).
-record(msg_sasl_response, {
    data :: iodata()
}).
-record(msg_startup, {
    parameters = #{} :: #{atom() => unicode:chardata()}
}).
-record(msg_sync, {
}).
-record(msg_terminate, {
}).

-endif.