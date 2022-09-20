# Replace target table name
# Monetization Materialised Table v3.0
# https://github.com/Tiggerito/GA4-Scripts/blob/main/monetization/bigquery-materialize-table-transactions.sql

CREATE OR REPLACE TABLE `your-project.analytics_123456789.monetization`
  PARTITION BY DATE(event_timestamp)
AS
SELECT 
  IFNULL(purchase_transaction_id, server_purchase_transaction_id) AS transaction_id,
  IFNULL(purchase_event_timestamp, server_purchase_event_timestamp) AS event_timestamp,
  DATE_TRUNC(IFNULL(purchase_event_timestamp, server_purchase_event_timestamp), DAY) AS event_date,
  purchase_event_timestamp,
  purchase_revenue,
  purchase_events,
  server_purchase_event_timestamp,
  server_purchase_revenue,
  server_purchase_events,
  device_browser,
  device_browser_version,
  device_category,
  device_operating_system,
  device_operating_system_version
FROM
  (SELECT 
    TIMESTAMP_MICROS(ANY_VALUE(event_timestamp)) AS purchase_event_timestamp,
    ecommerce.transaction_id AS purchase_transaction_id,
    ANY_VALUE(ecommerce.purchase_revenue) AS purchase_revenue,
    COUNT(*) AS purchase_events,
    ANY_VALUE(device.web_info.browser) AS device_browser,
    ANY_VALUE(device.web_info.browser_version) AS device_browser_version,
    ANY_VALUE(device.category) AS device_category,
    ANY_VALUE(device.operating_system) AS device_operating_system,
    ANY_VALUE(device.operating_system_version) AS device_operating_system_version   
  FROM `your-project.analytics_123456789.events_*` # Replace source table name
  WHERE event_name = 'purchase'
  GROUP BY purchase_transaction_id
  )
FULL OUTER JOIN 
  (SELECT 
    TIMESTAMP_MICROS(ANY_VALUE(event_timestamp)) AS server_purchase_event_timestamp,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transaction_id') AS server_purchase_transaction_id,
    ANY_VALUE((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'value')) AS server_purchase_revenue,
    COUNT(*) AS server_purchase_events,
  FROM `your-project.analytics_123456789.events_*` # Replace source table name
  WHERE event_name = 'server_purchase'
  GROUP BY server_purchase_transaction_id
  )
ON purchase_transaction_id = server_purchase_transaction_id