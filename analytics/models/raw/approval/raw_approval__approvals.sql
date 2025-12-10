SELECT
    toUUID(id) AS id,
    toUUID(request_id) AS request_id,
    toString(status) AS status,
    toString(method) AS method,
    toString(approvers) AS approvers,
    toString(terminal_actions) AS terminal_actions,
    toString(immediate_actions) AS immediate_actions,
    toString(metadata) AS metadata,
    toString(create_metadata) AS create_metadata
FROM postgresql(approval_pg_creds, table='approvals')
