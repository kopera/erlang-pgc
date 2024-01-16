-define(internal_statement_name_prefix, "_pgc_connection_:").
-define(refresh_types_statement_name, <<?internal_statement_name_prefix, "refresh_types">>).
-define(refresh_types_statement_text, <<
"select
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
  left join pg_catalog.pg_namespace on pg_namespace.oid = pg_type.typnamespace"
>>).