#  Replace target table name
CREATE OR REPLACE TABLE `your-project.analytics_123456789.web_vitals_summary`
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
  CASE event_name
    WHEN 'CLS' THEN 
        CASE 
            WHEN metric_value <= 0.1 THEN 'Good'
            WHEN metric_value <= 0.25 THEN 'Needs Improvement'
            ELSE 'Poor'
        END
    WHEN 'LCP' THEN 
        CASE 
            WHEN metric_value <= 2.500 THEN 'Good'
            WHEN metric_value <= 4.000 THEN 'Needs Improvement'
            ELSE 'Poor'
        END
    WHEN 'FID' THEN 
        CASE 
            WHEN metric_value <= 100 THEN 'Good'
            WHEN metric_value <= 300 THEN 'Needs Improvement'
            ELSE 'Poor'
        END
    WHEN 'TTFB' THEN 
        CASE 
            WHEN metric_value <= 0.800 THEN 'Good'
            WHEN metric_value <= 1.800 THEN 'Needs Improvement'
            ELSE 'Poor'
        END
    WHEN 'FCP' THEN 
        CASE 
            WHEN metric_value <= 1.800 THEN 'Good'
            WHEN metric_value <= 3.000 THEN 'Needs Improvement'
            ELSE 'Poor'
        END
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
            debug_target,
            event_timestamp,
            event_name,
            metric_id,
            # Tony's modification to support TTFB and FCP
            IF(event_name = 'LCP' OR event_name = 'TTFB' OR event_name = 'FCP', metric_value / 1000, metric_value) AS metric_value,
            user_pseudo_id,
            session_engaged,
            session_revenue,

            # Tony's additions 2 START
            page_location,
            page_type,
            continent,
            region,
            device_browser,
            effective_connection_type
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
                ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')) AS page_location,
                ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_type')) AS page_type,
                ANY_VALUE(geo.continent) AS continent,
                ANY_VALUE(geo.region) AS region,
                ANY_VALUE(device.web_info.browser) AS device_browser,
                ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'effective_connection_type')) AS effective_connection_type
                # Tony's additions 3 END

            FROM
              # Replace source table name
              `your-project.analytics_123456789.events_*`
            WHERE
              # Tony's modification to support TTFB and FCP
              event_name IN ('LCP', 'FID', 'CLS', 'TTFB', 'FCP', 'first_visit', 'purchase')
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