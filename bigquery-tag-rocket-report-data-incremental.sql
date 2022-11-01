# Website Errors Materialised Table Incremental v1.0
# https://github.com/Tiggerito/GA4-Scripts/blob/main/error-tracking/bigquery-materialize-table-website-errors.sql

# Replace all occurances of DatasetID with your Dataset ID

BEGIN
  # The first run with gather all data. After that it will gather new data and merge the last 2 (or 3?) days of data

  DECLARE datetogather DEFAULT CURRENT_TIMESTAMP();

  # Web Vitals

  # If schema is changed then update both references to the version so the table gets re-built
  IF NOT EXISTS(
    SELECT
      1
    FROM
      `DatasetID.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'web_vitals_summary_incremental'
      AND option_name = 'description'
      AND option_value LIKE "%Version 1.0%"
  ) 
  THEN
    CREATE OR REPLACE TABLE `DatasetID.web_vitals_summary_incremental` (
      last_updated TIMESTAMP,
      user_pseudo_id	STRING,
      call_timestamp TIMESTAMP,
      call_sequence INT64,
      page_timestamp TIMESTAMP,
      ga_session_id	INT64,			
      user_type	STRING,			
      session_engagement	STRING,			
      country	STRING,			
      device_category	STRING,			
      device_os	STRING,			
      traffic_medium	STRING,			
      traffic_name	STRING,			
      traffic_source	STRING,			
      page_path	STRING,			
      debug_target	STRING,			
      event_timestamp	TIMESTAMP,			
      metric_id	STRING,			
      metric_value	FLOAT64,					
      session_revenue	FLOAT64,			
      metric_rating	STRING,			
      page_location	STRING,			
      page_type	STRING,			
      continent	STRING,			
      region	STRING,			
      device_browser	STRING,			
      effective_connection_type	STRING,			
      save_data	STRING,			
      width	INT64,			
      height	INT64,			
      metric_name	STRING,			
      event_date	TIMESTAMP,			
      metric_status	STRING
    )
    PARTITION BY DATE(event_timestamp)
    CLUSTER BY metric_name
    OPTIONS (description = 'Version 1.0'); 
  END IF;

  # 10MB min per query makes this look expensive for small tables.
  SET datetogather = (SELECT TIMESTAMP_TRUNC(TIMESTAMP_ADD(MAX(event_date), INTERVAL -2 DAY), DAY) FROM `DatasetID.web_vitals_summary_incremental`);

  MERGE INTO `DatasetID.web_vitals_summary_incremental` A 
  USING (
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
      DATE_TRUNC(event_timestamp, DAY) AS event_date,
      
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

                  TIMESTAMP_MICROS(ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_timestamp'))) AS call_timestamp,
                  ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_sequence')) AS call_sequence,
                  TIMESTAMP_MICROS(ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'page_timestamp'))) AS page_timestamp,
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
                    ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'height')) AS height

                    # Tony's additions 3 END
                FROM
                  `DatasetID.events_*`
                WHERE
                  # Tony's modification to support TTFB and FCP and INP
                  event_name IN ('LCP', 'FID', 'CLS', 'TTFB', 'FCP', 'INP', 'first_visit', 'purchase')
                  AND (datetogather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',datetogather) AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
                GROUP BY
                  1, 2
              )
          )
        WHERE
          ga_session_id IS NOT NULL
        GROUP BY ga_session_id
      )
    CROSS JOIN UNNEST(events) AS evt
    WHERE evt.event_name NOT IN ('first_visit', 'purchase')
  ) B
  ON 
    ((A.user_pseudo_id IS NULL AND B.user_pseudo_id IS NULL) OR (A.user_pseudo_id = B.user_pseudo_id))
    AND ((A.call_timestamp IS NULL AND B.call_timestamp IS NULL) OR (A.call_timestamp = B.call_timestamp))
    AND ((A.call_sequence IS NULL AND B.call_sequence IS NULL) OR (A.call_sequence = B.call_sequence))
    AND A.event_timestamp = B.event_timestamp # backup
    AND A.metric_id = B.metric_id
  #  AND (datetogather IS NULL OR TIMESTAMP_TRUNC(B.event_timestamp, DAY) > datetogather)
  #WHEN MATCHED THEN UPDATE SET  # what we gather should never change. 
  #  A.last_updated = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (
    last_updated,
    user_pseudo_id,
    call_timestamp,
    call_sequence,
    page_timestamp,
    ga_session_id,			
    user_type,			
    session_engagement,			
    country,			
    device_category,			
    device_os,			
    traffic_medium,			
    traffic_name,			
    traffic_source,			
    page_path,			
    debug_target,			
    event_timestamp,			
    metric_id,			
    metric_value,						
    session_revenue,			
    metric_rating,			
    page_location,			
    page_type,			
    continent,			
    region,			
    device_browser,			
    effective_connection_type,			
    save_data,			
    width,			
    height,			
    metric_name,			
    event_date,			
    metric_status	
  )
  VALUES (
    CURRENT_TIMESTAMP(),
    user_pseudo_id,
    call_timestamp,
    call_sequence,
    page_timestamp,
    ga_session_id,			
    user_type,			
    session_engagement,			
    country,			
    device_category,			
    device_os,			
    traffic_medium,			
    traffic_name,			
    traffic_source,			
    page_path,			
    debug_target,			
    event_timestamp,			
    metric_id,			
    metric_value,						
    session_revenue,			
    metric_rating,			
    page_location,			
    page_type,			
    continent,			
    region,			
    device_browser,			
    effective_connection_type,			
    save_data,			
    width,			
    height,			
    metric_name,			
    event_date,			
    metric_status	
  );

  # Purchases 
  # If schema is changed then update both references to the version so the table gets re-built
  IF NOT EXISTS(
    SELECT
      1
    FROM
      `DatasetID.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'purchases_incremental'
      AND option_name = 'description'
      AND option_value LIKE "%Version 1.0%"
  ) 
  THEN
    CREATE OR REPLACE TABLE `DatasetID.purchases_incremental` (
      last_updated TIMESTAMP,
      user_pseudo_id	STRING,
      call_timestamp TIMESTAMP,
      call_sequence INT64,
      transaction_id	STRING,			
      event_timestamp	TIMESTAMP,			
      event_date	TIMESTAMP,			
      purchase_event_timestamp	TIMESTAMP,			
      purchase_revenue	FLOAT64,		
      purchase_shipping_value FLOAT64,		
      purchase_tax_value FLOAT64,		
      purchase_refund_value FLOAT64,		
      purchase_events	INT64,			
      server_purchase_event_timestamp	TIMESTAMP,			
      server_purchase_revenue	FLOAT64,   
      server_purchase_method	STRING,
      server_purchase_events	INT64,			
      device_browser	STRING,			
      device_browser_version	STRING,			
      device_category	STRING,			
      device_operating_system	STRING,			
      device_operating_system_version	STRING,			
      traffic_medium	STRING,			
      traffic_name	STRING,			
      traffic_source	STRING,			
      user_ltv_revenue	FLOAT64,			
      user_ltv_currency	STRING
    )
    # or maybe month? each partition should be 1GB https://medium.com/dataseries/costs-and-performance-lessons-after-using-bigquery-with-terabytes-of-data-54a5809ac912
    PARTITION BY TIMESTAMP_TRUNC(event_timestamp, DAY)
    OPTIONS (description = 'Version 1.0'); 
  END IF;

  # 10MB min per query makes this look expensive for small tables.
  SET datetogather = (SELECT TIMESTAMP_TRUNC(TIMESTAMP_ADD(MAX(event_date), INTERVAL -2 DAY), DAY) FROM `DatasetID.purchases_incremental`);

  MERGE INTO `DatasetID.purchases_incremental` A 
  USING (
    SELECT 
      user_pseudo_id,
      call_timestamp,
      call_sequence,
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
        ANY_VALUE(user_pseudo_id) AS user_pseudo_id,
        TIMESTAMP_MICROS(ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_timestamp'))) AS call_timestamp,
        ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_sequence')) AS call_sequence,
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
      FROM `DatasetID.events_*` 
      WHERE event_name = 'purchase'
      AND (datetogather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',datetogather) AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
      GROUP BY purchase_transaction_id
      )
    FULL OUTER JOIN 
      (SELECT 
        TIMESTAMP_MICROS(ANY_VALUE(event_timestamp)) AS server_purchase_event_timestamp,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transaction_id') AS server_purchase_transaction_id,
        ANY_VALUE((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'value')) AS server_purchase_revenue,
        ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'method')) AS server_purchase_method,
        COUNT(*) AS server_purchase_events,
      FROM `DatasetID.events_*` 
      WHERE event_name = 'server_purchase'
      GROUP BY server_purchase_transaction_id
      )
    ON purchase_transaction_id = server_purchase_transaction_id
  ) B
  ON 
    ((A.user_pseudo_id IS NULL AND B.user_pseudo_id IS NULL) OR (A.user_pseudo_id = B.user_pseudo_id))
    AND ((A.call_timestamp IS NULL AND B.call_timestamp IS NULL) OR (A.call_timestamp = B.call_timestamp))
    AND ((A.call_sequence IS NULL AND B.call_sequence IS NULL) OR (A.call_sequence = B.call_sequence))
    AND ((A.purchase_event_timestamp IS NULL AND B.purchase_event_timestamp IS NULL) OR (A.purchase_event_timestamp = B.purchase_event_timestamp))
    AND ((A.server_purchase_event_timestamp IS NULL AND B.server_purchase_event_timestamp IS NULL) OR (A.server_purchase_event_timestamp = B.server_purchase_event_timestamp)) 
    AND A.transaction_id = B.transaction_id 
  #  AND (datetogather IS NULL OR TIMESTAMP_TRUNC(B.event_timestamp, DAY) > datetogather)
  #WHEN MATCHED THEN UPDATE SET  # what we gather should never change. 
  #  A.last_updated = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (
    last_updated,
    user_pseudo_id,
    call_timestamp,
    call_sequence,
    transaction_id,
    event_timestamp,
    event_date,
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
  )
  VALUES (
    CURRENT_TIMESTAMP(),
    user_pseudo_id,
    call_timestamp,
    call_sequence,
    transaction_id,
    event_timestamp,
    event_date,
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
  );

  # Website Errors 

  # If schema is changed then update both references to the version so the table gets re-built
  IF NOT EXISTS(
    SELECT
      1
    FROM
      `DatasetID.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'website_errors_incremental'
      AND option_name = 'description'
      AND option_value LIKE "%Version 1.0%"
  ) 
  THEN
    CREATE OR REPLACE TABLE `DatasetID.website_errors_incremental` (
      last_updated TIMESTAMP,
      user_pseudo_id	STRING,
      call_timestamp TIMESTAMP,
      call_sequence INT64,
      event_timestamp TIMESTAMP,
      event_date	TIMESTAMP,
      device_browser	STRING,
      device_browser_version	STRING,
      device_category	STRING,
      device_mobile_marketing_name	STRING,
      device_mobile_brand_name	STRING,
      device_mobile_model_name	STRING,
      device_operating_system	STRING,
      device_operating_system_version	STRING,			
      page_location	STRING,
      page_type	STRING,
      error_message	STRING,
      error_type	STRING,
      error_filename	STRING,
      error_lineno	STRING,
      error_colno	STRING,
      error_object_type	STRING	
    )
    # or maybe month? each partition should be 1GB https://medium.com/dataseries/costs-and-performance-lessons-after-using-bigquery-with-terabytes-of-data-54a5809ac912
    PARTITION BY TIMESTAMP_TRUNC(event_timestamp, DAY)
    OPTIONS (description = 'Version 1.0'); 
  END IF;

  # 60 days
  # first run <12GB
  # second run 680MB - yay
  # original <12GB

  # 10MB min per query makes this look expensive for small tables.
  SET datetogather = (SELECT TIMESTAMP_TRUNC(TIMESTAMP_ADD(MAX(event_date), INTERVAL -2 DAY), DAY) FROM `DatasetID.website_errors_incremental`);

  MERGE INTO `DatasetID.website_errors_incremental` A 
  USING (
    SELECT 
      user_pseudo_id,
      TIMESTAMP_MICROS((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_timestamp')) AS call_timestamp,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_sequence') AS call_sequence,
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
    FROM `DatasetID.events_*` 
    WHERE event_name = 'exception'
    #AND (datetogather IS NULL OR TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), DAY) > datetogather)
    AND (datetogather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',datetogather) AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
  ) B
  ON 
    ((A.user_pseudo_id IS NULL AND B.user_pseudo_id IS NULL) OR (A.user_pseudo_id = B.user_pseudo_id))
    AND ((A.call_timestamp IS NULL AND B.call_timestamp IS NULL) OR (A.call_timestamp = B.call_timestamp))
    AND ((A.call_sequence IS NULL AND B.call_sequence IS NULL) OR (A.call_sequence = B.call_sequence))
    AND A.event_timestamp = B.event_timestamp # backup
    AND A.error_message = B.error_message # backup
    AND A.page_location = B.page_location # backup
  #  AND (datetogather IS NULL OR TIMESTAMP_TRUNC(B.event_timestamp, DAY) > datetogather)
  #WHEN MATCHED THEN UPDATE SET  # what we gather should never change. 
  #  A.last_updated = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (
    last_updated,
    user_pseudo_id,
    call_timestamp,
    call_sequence,
    event_timestamp,
    event_date,
    device_browser,
    device_browser_version,
    device_category,
    device_mobile_marketing_name,
    device_mobile_brand_name,
    device_mobile_model_name,
    device_operating_system,
    device_operating_system_version,			
    page_location,
    page_type,
    error_message,
    error_type,
    error_filename,
    error_lineno,
    error_colno,
    error_object_type	
  )
  VALUES (
    CURRENT_TIMESTAMP(),
    user_pseudo_id,
    call_timestamp,
    call_sequence,
    event_timestamp,
    event_date,
    device_browser,
    device_browser_version,
    device_category,
    device_mobile_marketing_name,
    device_mobile_brand_name,
    device_mobile_model_name,
    device_operating_system,
    device_operating_system_version,	
    page_location,
    page_type,
    error_message,
    error_type,
    error_filename,
    error_lineno,
    error_colno,
    error_object_type	
  );

  # Missing Pages

  # If schema is changed then update both references to the version so the table gets re-built
  IF NOT EXISTS(
    SELECT
      1
    FROM
      `DatasetID.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'missing_pages_incremental'
      AND option_name = 'description'
      AND option_value LIKE "%Version 1.0%"
  ) 
  THEN
    CREATE OR REPLACE TABLE `DatasetID.missing_pages_incremental` (
      last_updated TIMESTAMP,
      user_pseudo_id	STRING,
      call_timestamp TIMESTAMP,
      call_sequence INT64,
      event_timestamp TIMESTAMP,
      event_date	TIMESTAMP,
      source	STRING,
      medium	STRING,
      campaign	STRING,
      page_location	STRING,
      page_type	STRING,
      page_referrer	STRING
    )
    # or maybe month? each partition should be 1GB https://medium.com/dataseries/costs-and-performance-lessons-after-using-bigquery-with-terabytes-of-data-54a5809ac912
    PARTITION BY TIMESTAMP_TRUNC(event_timestamp, DAY)
    OPTIONS (description = 'Version 1.0');
  END IF;

  SET datetogather = (SELECT TIMESTAMP_TRUNC(TIMESTAMP_ADD(MAX(event_date), INTERVAL -2 DAY), DAY) FROM `DatasetID.missing_pages_incremental`);

  MERGE INTO `DatasetID.missing_pages_incremental` A 
  USING (
    SELECT 
      user_pseudo_id,
      TIMESTAMP_MICROS((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_timestamp')) AS call_timestamp,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_sequence') AS call_sequence,
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
    FROM `DatasetID.events_*` 
    WHERE event_name = 'page_view'
    AND (datetogather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',datetogather) AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
  ) B
  ON 
    ((A.user_pseudo_id IS NULL AND B.user_pseudo_id IS NULL) OR (A.user_pseudo_id = B.user_pseudo_id))
    AND ((A.call_timestamp IS NULL AND B.call_timestamp IS NULL) OR (A.call_timestamp = B.call_timestamp))
    AND ((A.call_sequence IS NULL AND B.call_sequence IS NULL) OR (A.call_sequence = B.call_sequence))
    AND A.event_timestamp = B.event_timestamp # backup
    AND A.page_location = B.page_location # backup
  #  AND (datetogather IS NULL OR TIMESTAMP_TRUNC(B.event_timestamp, DAY) > datetogather)
  #WHEN MATCHED THEN UPDATE SET  # what we gather should never change. 
  #  A.last_updated = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (
    last_updated,
    user_pseudo_id,
    call_timestamp,
    call_sequence,
    event_timestamp,
    event_date,
    source,
    medium,
    campaign,
    page_location,
    page_type,
    page_referrer
  )
  VALUES (
    CURRENT_TIMESTAMP(),
    user_pseudo_id,
    call_timestamp,
    call_sequence,
    event_timestamp,
    event_date,
    source,
    medium,
    campaign,
    page_location,
    page_type,
    page_referrer
  );
END;