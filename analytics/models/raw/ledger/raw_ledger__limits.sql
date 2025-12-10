SELECT
    toUUID(id) AS id,
    toUUID(entity_id) AS entity_id,
    toString(entity_type) AS entity_type,
    toString(context) AS context,
    toString(limit_metadata) AS limit_metadata,
    toString(dimensions) AS dimensions,
    toDecimal256(value, 38) AS value,
    toString(denomination) AS denomination,
    toString(utilization_change) AS utilization_change,
    toDecimal256(upper_tolerance, 38) AS upper_tolerance,
    toDecimal256(lower_tolerance, 38) AS lower_tolerance,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(ledger_pg_creds, table='limits')
