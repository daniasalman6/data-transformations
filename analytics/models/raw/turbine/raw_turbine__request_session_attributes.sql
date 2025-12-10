{{
    config(
        materialized = "view",
    )
}}
SELECT
    toUUID(request_id) AS request_id,
    toString(attribute) AS attribute,
    toString(value) AS value,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(turbine_pg_creds, table='request_session_attributes')
