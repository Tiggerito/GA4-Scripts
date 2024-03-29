# Web Vitals Materialised Table v3.4
# https://github.com/Tiggerito/GA4-Scripts/blob/main/core-web-vitals/bigquery-materialize-table-web-vitals.sql

# Replace all occurances of DatasetID with your Dataset ID

CREATE OR REPLACE TABLE `${ProjectID}.${DatasetID}.web_vitals_summary` # Replace DatasetID with your Dataset ID
  PARTITION BY DATE(event_timestamp)
  CLUSTER BY metric_name
AS
SELECT
  CURRENT_TIMESTAMP() AS last_updated,
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
            call_timestamp,
            call_sequence,
            page_timestamp,
            device_category,
            device_os,
            traffic_medium,
            traffic_name,
            traffic_source,
            page_path,
            # Tony's modification to support long debug_target
            IF(debug_target2 IS NULL, debug_target, CONCAT(debug_target, debug_target2)) AS debug_target,
            event_timestamp,
            event_date,
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
              (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id, # can be null in consent mode
              (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'metric_id') AS metric_id,

              SAFE.TIMESTAMP_MILLIS(ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_timestamp'))) AS call_timestamp, # not sure what use this has. We also have event_timestamp
              ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_sequence')) AS call_sequence, # not sure what use this has especially when set to ANY_VALUE
              SAFE.TIMESTAMP_MILLIS(ANY_VALUE((SELECT CAST(COALESCE(value.double_value, value.int_value) AS INT64) FROM UNNEST(event_params) WHERE key = 'page_timestamp'))) AS page_timestamp,

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
              MAX(PARSE_DATE('%Y%m%d', event_date)) AS event_date,
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
                ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'height')) AS height
                # Tony's additions 3 END

            FROM
              `${ProjectID}.${DatasetID}.events_*` # Replace DatasetID with your Dataset ID
            WHERE
              # Tony's modification to support TTFB and FCP and INP
              event_name IN ('LCP', 'FID', 'CLS', 'TTFB', 'FCP', 'INP', 'first_visit', 'purchase')
            GROUP BY
              1, 2
          )
      )
    WHERE
      ga_session_id IS NOT NULL
    GROUP BY ga_session_id # can be null in consent mode ?
  )
CROSS JOIN UNNEST(events) AS evt
WHERE evt.event_name NOT IN ('first_visit', 'purchase');

