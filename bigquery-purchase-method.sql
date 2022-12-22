#  Replace target table name
SELECT method, COUNT(event_name) as purchases, SUM(value) as revenue
FROM (
SELECT
    event_name,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'method') AS method,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transaction_id') AS transaction_id,
    (SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'value') AS value
FROM
    # Replace source table name
    `your_project.analytics_123456789.events_*`
WHERE
    event_name IN ('server_purchase')
)

GROUP BY method
ORDER BY method