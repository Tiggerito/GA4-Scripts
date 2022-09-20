# Replace target table name
# Order Status Materialised Table v3.0
# https://github.com/Tiggerito/GA4-Scripts/blob/main/monetization/bigquery-materialize-table-order-status.sql

CREATE OR REPLACE TABLE `your-project.analytics_123456789.order_status`
  PARTITION BY DATE(event_timestamp)
  CLUSTER BY order_status
AS
SELECT
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    ecommerce.transaction_id AS transaction_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'order_status') AS order_status
FROM
  # Replace source table name
  `your-project.analytics_123456789.events_*`
WHERE
  event_name = 'order_status_update'
