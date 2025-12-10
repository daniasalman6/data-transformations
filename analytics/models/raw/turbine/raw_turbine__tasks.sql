{{
    config(
        materialized = "view",
    )
}}
SELECT
    toUUID(id) AS id,
    toUUID(request_id) AS request_id,
    toString(event) AS event,
    toString(parent) AS parent,
    toString(status) AS status,
    toBool(join_parent) AS join_parent,
    toString(reference) AS reference,
    toString(task_metadata) AS task_metadata,
    toString(status_reason) AS status_reason,
    toDateTime64(create_date, 6, 'GMT') AS create_date,
    toDateTime64(update_date, 6, 'GMT') AS update_date,
    toUUID(executor_id) AS executor_id
FROM postgresql(turbine_pg_creds, table='tasks')
