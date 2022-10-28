# Web Vitals Materialised Table for Tag Rocket v4.0
# https://github.com/Tiggerito/GA4-Scripts/blob/main/core-web-vitals/bigquery-materialize-table-tag-rocket-web-vitals.sql

# Replace all occurances of DatasetID with your Dataset ID

CREATE OR REPLACE TABLE `DatasetID.web_vitals_summary` # Replace DatasetID with your Dataset ID
  PARTITION BY DATE(event_timestamp)
  CLUSTER BY metric_name
AS
SELECT
  ga_session_id,
  IF(
    EXISTS(SELECT 1 FROM UNNEST(events) AS e WHERE e.event_name = 'first_visit'),
    'New user',
    'Returning user') AS user_type,
  IF(
    (SELECT MAX(session_engaged) FROM UNNEST(events)) > 0, 'Engaged', 'Not engaged')
    AS session_engagement,
  evt.* EXCEPT (session_engaged, event_name),
  event_name AS metric_name,
  FORMAT_TIMESTAMP('%Y%m%d', event_timestamp) AS event_date,
  
  # Tony's additions 1 START
  CASE metric_rating
    WHEN 'good' THEN 'Good'
    WHEN 'ni' THEN 'Needs Improvement'
    WHEN 'needs-improvement' THEN 'Needs Improvement'
    WHEN 'poor' THEN 'Poor'
    ELSE metric_rating
  END AS metric_status
  # Tony's additions 1 END

FROM
  (
    SELECT
      ga_session_id,
      ARRAY_AGG(custom_event) AS events
    FROM
      (
        SELECT
          ga_session_id,
          STRUCT(
            country,
            device_category,
            device_os,
            traffic_medium,
            traffic_name,
            traffic_source,
            page_path,
            # Tony's modification to support long debug_target
            IF(debug_target2 IS NULL, debug_target, CONCAT(debug_target, debug_target2)) AS debug_target,
            event_timestamp,
            event_name,
            metric_id,
            # Tony's modification to also support TTFB and FCP
            IF(event_name = 'LCP' OR event_name = 'TTFB' OR event_name = 'FCP', metric_value / 1000, metric_value) AS metric_value,
            user_pseudo_id,
            session_engaged,
            session_revenue,

            # Tony's additions 2 START
            metric_rating,
            page_location,
            page_type,
            continent,
            region,
            device_browser,
            effective_connection_type,
            save_data,
            width,
            height
            # Tony's additions 2 END

            ) AS custom_event
        FROM
          (
            SELECT
              (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
                AS ga_session_id,
              (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'metric_id')
                AS metric_id,
              ANY_VALUE(device.category) AS device_category,
              ANY_VALUE(device.operating_system) AS device_os,
              ANY_VALUE(traffic_source.medium) AS traffic_medium,
              ANY_VALUE(traffic_source.name) AS traffic_name,
              ANY_VALUE(traffic_source.source) AS traffic_source,
              ANY_VALUE(
                REGEXP_SUBSTR(
                  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
                  r'^[^?]+')) AS page_path,
              
              ANY_VALUE(
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'debug_target'))
                AS debug_target,
                # Tony's modification to support long debug_target values (over 100 characters)
              ANY_VALUE(
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'debug_target2'))
                AS debug_target2,
              ANY_VALUE(user_pseudo_id) AS user_pseudo_id,
              ANY_VALUE(geo.country) AS country,
              ANY_VALUE(event_name) AS event_name,
              SUM(ecommerce.purchase_revenue) AS session_revenue,
              MAX(
                (
                  SELECT
                    COALESCE(
                      value.double_value, value.int_value, CAST(value.string_value AS NUMERIC))
                  FROM UNNEST(event_params)
                  WHERE key = 'session_engaged'
                )) AS session_engaged,
              TIMESTAMP_MICROS(MAX(event_timestamp)) AS event_timestamp,
              MAX(
                (
                  SELECT COALESCE(value.double_value, value.int_value)
                  FROM UNNEST(event_params)
                  WHERE key = 'metric_value'
                )) AS metric_value,

                # Tony's additions 3 START
                ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'metric_rating')) AS metric_rating,
                ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')) AS page_location,
                ANY_VALUE((SELECT COALESCE(value.string_value, CAST(value.int_value AS STRING)) FROM UNNEST(event_params) WHERE key = 'page_type')) AS page_type,
                ANY_VALUE(geo.continent) AS continent,
                ANY_VALUE(geo.region) AS region,
                ANY_VALUE(device.web_info.browser) AS device_browser,
                ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'effective_connection_type')) AS effective_connection_type,
                ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'save_data')) AS save_data,
                ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'width')) AS width,
                ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'height')) AS height,
                TIMESTAMP_MICROS(ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'page_timestamp'))) AS page_timestamp
                # Tony's additions 3 END
            FROM
              `DatasetID.events_*` # Replace DatasetID with your Dataset ID
            WHERE
              # Tony's modification to support TTFB and FCP and INP
              event_name IN ('LCP', 'FID', 'CLS', 'TTFB', 'FCP', 'INP', 'first_visit', 'purchase')
            GROUP BY
              1, 2
          )
      )
    WHERE
      ga_session_id IS NOT NULL
    GROUP BY ga_session_id
  )
CROSS JOIN UNNEST(events) AS evt
WHERE evt.event_name NOT IN ('first_visit', 'purchase');

# Purchases Materialised Table v3.1
# https://github.com/Tiggerito/GA4-Scripts/blob/main/monetization/bigquery-materialize-table-purchases.sql

# Replace all occurances of DatasetID with your Dataset ID

CREATE OR REPLACE TABLE `DatasetID.purchases` # Replace DatasetID with your Dataset ID
  PARTITION BY DATE(event_timestamp)
AS
SELECT 
  IFNULL(purchase_transaction_id, server_purchase_transaction_id) AS transaction_id,
  IFNULL(purchase_event_timestamp, server_purchase_event_timestamp) AS event_timestamp,
  DATE_TRUNC(IFNULL(purchase_event_timestamp, server_purchase_event_timestamp), DAY) AS event_date,
  purchase_event_timestamp,
  purchase_revenue,
  purchase_shipping_value,
  purchase_tax_value,
  purchase_refund_value,
  purchase_events,
  server_purchase_event_timestamp,
  server_purchase_revenue,
  server_purchase_method,
  server_purchase_events,
  device_browser,
  device_browser_version,
  device_category,
  device_operating_system,
  device_operating_system_version,
  traffic_medium,
  traffic_name,
  traffic_source,
  user_ltv_revenue,
  user_ltv_currency
FROM
  (SELECT 
    TIMESTAMP_MICROS(ANY_VALUE(event_timestamp)) AS purchase_event_timestamp,
    ecommerce.transaction_id AS purchase_transaction_id,
    ANY_VALUE(ecommerce.purchase_revenue) AS purchase_revenue,
    ANY_VALUE(ecommerce.shipping_value) AS purchase_shipping_value,
    ANY_VALUE(ecommerce.tax_value) AS purchase_tax_value,
    ANY_VALUE(ecommerce.refund_value) AS purchase_refund_value,
    COUNT(*) AS purchase_events,
    ANY_VALUE(device.web_info.browser) AS device_browser,
    ANY_VALUE(device.web_info.browser_version) AS device_browser_version,
    ANY_VALUE(device.category) AS device_category,
    ANY_VALUE(device.operating_system) AS device_operating_system,
    ANY_VALUE(device.operating_system_version) AS device_operating_system_version,
    ANY_VALUE(traffic_source.medium) AS traffic_medium,
    ANY_VALUE(traffic_source.name) AS traffic_name,
    ANY_VALUE(traffic_source.source) AS traffic_source, 
    ANY_VALUE(user_ltv.revenue) AS user_ltv_revenue, 
    ANY_VALUE(user_ltv.currency) AS user_ltv_currency
  FROM `DatasetID.events_*` # Replace DatasetID with your Dataset ID
  WHERE event_name = 'purchase'
  GROUP BY purchase_transaction_id
  )
FULL OUTER JOIN 
  (SELECT 
    TIMESTAMP_MICROS(ANY_VALUE(event_timestamp)) AS server_purchase_event_timestamp,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transaction_id') AS server_purchase_transaction_id,
    ANY_VALUE((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'value')) AS server_purchase_revenue,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'method') AS server_purchase_method,
    COUNT(*) AS server_purchase_events,
  FROM `DatasetID.events_*` # Replace DatasetID with your Dataset ID
  WHERE event_name = 'server_purchase'
  GROUP BY server_purchase_transaction_id
  )
ON purchase_transaction_id = server_purchase_transaction_id;


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
  WHERE event_name = 'exception';
  
# Missing Page Materialised Table v3.2
# https://github.com/Tiggerito/GA4-Scripts/blob/main/error-tracking/bigquery-materialize-table-missing-pages.sql

# Replace all occurances of DatasetID with your Dataset ID

CREATE OR REPLACE TABLE `DatasetID.missing_pages` # Replace DatasetID with your Dataset ID
  PARTITION BY DATE(event_timestamp)
AS
SELECT 
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), DAY) AS event_date,
    # traffic_source.medium AS traffic_medium, # user level
    # traffic_source.name AS traffic_name, # user level
    # traffic_source.source AS traffic_source, # user level
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS campaign,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    (SELECT COALESCE(value.string_value, CAST(value.int_value AS STRING)) FROM UNNEST(event_params) WHERE key = 'page_type') AS page_type,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') AS page_referrer,
  FROM `DatasetID.events_*` # Replace DatasetID with your Dataset ID
  WHERE event_name = 'page_view';
  
