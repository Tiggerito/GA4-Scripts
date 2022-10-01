
# Website Errors Materialised Table v3.1
# https://github.com/Tiggerito/GA4-Scripts/blob/main/error-tracking/bigquery-materialize-table-website-errors.sql

# Replace all occurances of DatasetID with your Dataset ID

CREATE OR REPLACE TABLE `DatasetID.website_errors` # Replace DatasetID with your Dataset ID
  PARTITION BY DATE(event_timestamp)
AS
SELECT 
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), DAY) AS event_date,
    device.web_info.browser AS device_browser,
    device.web_info.browser_version AS device_browser_version,
    device.category AS device_category,
    device.mobile_marketing_name AS device_mobile_marketing_name,
    device.mobile_brand_name AS device_mobile_brand_name,
    device.mobile_model_name AS device_mobile_model_name,
    device.operating_system AS device_operating_system,
    device.operating_system_version AS device_operating_system_version,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    (SELECT COALESCE(value.string_value, CAST(value.int_value AS STRING)) FROM UNNEST(event_params) WHERE key = 'page_type') AS page_type,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'error_message') AS error_message,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'error_type') AS error_type,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'error_filename') AS error_filename,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'error_lineno') AS error_lineno,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'error_colno') AS error_colno,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'error_object_type') AS error_object_type
  FROM `DatasetID.events_*` # Replace DatasetID with your Dataset ID
  WHERE event_name = 'exception'
