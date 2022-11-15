# Tag Rocket Report Data Incremental 2 v4.2
# https://github.com/Tiggerito/GA4-Scripts/blob/main/bigquery-tag-rocket-report-data-incremental2.sql

# Replace all occurances of DatasetID with your Dataset ID for the GA4 export. Something like analytics_1234567890

# make sure you run this using the same location as your analytics dataset

BEGIN
  # The first run with gather all data. After that it will gather new data and merge the last 3 days of data

  # To update the version search for queryVersion and replace the version number near it. And the one at the top.

  DECLARE lookbackDays DEFAULT 2; 
  # this means lookbackDays+1 days of GA4 export data are processed each time. 
  # GA4 states they add events that happen 72 hours after the fact, and then re-export the relevant days table 
  # 2+1=3 days should capture most late processed events.
  # The queries look back based on the last day that had been processed, so there should be no issues if the GA4 export gets delayed.
  # If you are reaching your monthly free limit you could reduce the lookback at the risk of losing a bit of data
  # If you have plenty of alowance you could increase it. Probably no need to go beyond lookbackDays being 3 or 4.

  # partitions and the bq_logs dataset will be deleted when older than 65 days, you can update this by searching for ExpirationDays and updating the number of days. NULL for never

  DECLARE maxDaysToLookBackOnInitialQuery DEFAULT 65; # extra days from today to cover the delay in GA4 exporting data. No use in having it larger than ExpirationDays

  DECLARE datetogather DEFAULT CURRENT_TIMESTAMP(); # dummy value. gets updated before every use

  CREATE SCHEMA IF NOT EXISTS tag_rocket
  OPTIONS (
    default_partition_expiration_days = 65, # ExpirationDays
    description = 'Data for the Tag Rocket Report'
  );

  CREATE SCHEMA IF NOT EXISTS bq_logs
  OPTIONS (
    default_table_expiration_days = 65, # ExpirationDays
    description = 'Destination for the Log Sink of billed queries'
  );

  # meta data
  
  CREATE OR REPLACE TABLE `tag_rocket.meta_data` (
      schedule_frequency STRING,
      scheduled_by	STRING,
      store_front_name STRING,
      store_front_url STRING,
      notification1_title STRING,
      notification1_content STRING,
      notification1_type STRING, 
      notification2_title STRING,
      notification2_content STRING,
      notification2_type STRING,
      notification3_title STRING,
      notification3_content STRING,
      notification3_type STRING,
      last_exported_date	STRING,			     
      partition_expiration STRING,	
      bigquery_project_id STRING,	
      ga4_account_id STRING,	
      ga4_property_id STRING,		
      query_version	STRING,		
      last_run_timestamp	TIMESTAMP
    )
  OPTIONS (description = 'Version 4.2') # queryVersion
  AS  
  SELECT * FROM (SELECT AS VALUE STRUCT(
    '', # schedule_frequency: how frequently the query is scheduled to run. e.g. "monthly", "every Monday", "manually"
    '', # scheduled_by:  e.g. "BigQuery"
    '', # store_front_name
    '', # store_front_url
    '', # notification1_title 
    '', # notification1_content 
    '', # notification1_type: normal, warning, error
    '', # notification2_title 
    '', # notification2_content
    '', # notification2_type 
    '', # notification3_title
    '', # notification3_content
    '', # notification3_type 
    '', # last_exported_date: set when using Tag Rocket to run the query
    '', # partition_expiration
    '', # bigquery_project_id
    '', # ga4_account_id
    '', # ga4_property_id
    '4.2', # query_version queryVersion
    CURRENT_TIMESTAMP() # last_run_timestamp
  ));

  # 60 days

  # Incremental 2
  # first run 48GB
  # second run 3GB

  # Incremental 1
  # first run 48GB
  # second run 3GB
  # this seems to be more accurage than #2

  # issue: purchase seems to be duplicating on each run

  # original 48GB every time



  # Web Vitals

  # If schema is changed then update both references to the version so the table gets re-built
  IF NOT EXISTS(
    SELECT
      1
    FROM
      `tag_rocket.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'web_vitals'
      AND option_name = 'description'
      AND option_value LIKE "%Version 4.2%" # queryVersion
  ) 
  THEN
    CREATE OR REPLACE TABLE `tag_rocket.web_vitals` (
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
      event_date	STRING,			
      metric_status	STRING
    )
    PARTITION BY DATE(event_timestamp)
    CLUSTER BY metric_name
    OPTIONS (description = 'Version 4.2');  # queryVersion
  END IF;

  ALTER TABLE `tag_rocket.web_vitals`
  SET OPTIONS (partition_expiration_days = 65); # ExpirationDays

  # 10MB min per query makes this look expensive for small tables.
  SET datetogather = (SELECT TIMESTAMP_TRUNC(TIMESTAMP_ADD(MAX(PARSE_TIMESTAMP("%Y%m%d",event_date)), INTERVAL -lookbackDays DAY), DAY) FROM `tag_rocket.web_vitals`);

  IF datetogather IS NOT NULL THEN
    DELETE FROM `tag_rocket.web_vitals` WHERE PARSE_TIMESTAMP("%Y%m%d",event_date) >= datetogather;
  ELSE
    IF maxDaysToLookBackOnInitialQuery IS NOT NULL THEN
      SET datetogather = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL maxDaysToLookBackOnInitialQuery DAY);
    END IF;
  END IF;

  INSERT `tag_rocket.web_vitals` 
  (    
    last_updated,
    ga_session_id,
    user_type,
    session_engagement,	
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
    debug_target,			
    event_timestamp,	
    event_date,			
    metric_id,			
    metric_value,	
    user_pseudo_id,					
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
    metric_status	
  )
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
  
    CASE metric_rating
      WHEN 'good' THEN 'Good'
      WHEN 'ni' THEN 'Needs Improvement'
      WHEN 'needs-improvement' THEN 'Needs Improvement'
      WHEN 'poor' THEN 'Poor'
      ELSE metric_rating
    END AS metric_status

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
              IF(debug_target2 IS NULL, debug_target, CONCAT(debug_target, debug_target2)) AS debug_target,
              event_timestamp,
              event_date,
              event_name,
              metric_id,
              IF(event_name = 'LCP' OR event_name = 'TTFB' OR event_name = 'FCP', metric_value / 1000, metric_value) AS metric_value,
              user_pseudo_id,
              session_engaged,
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
              height
              ) AS custom_event
          FROM
            (
              SELECT
                (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
                  AS ga_session_id,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'metric_id')
                  AS metric_id,

                SAFE.TIMESTAMP_MICROS(ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_timestamp'))) AS call_timestamp,
                ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_sequence')) AS call_sequence,
                SAFE.TIMESTAMP_MICROS(ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'page_timestamp'))) AS page_timestamp,
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
                SAFE.TIMESTAMP_MICROS(MAX(event_timestamp)) AS event_timestamp,
                MAX(event_date) AS event_date,
                MAX(
                  (
                    SELECT COALESCE(value.double_value, value.int_value)
                    FROM UNNEST(event_params)
                    WHERE key = 'metric_value'
                  )) AS metric_value,


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
              FROM
                `DatasetID.events_*`
              WHERE
                event_name IN ('LCP', 'FID', 'CLS', 'TTFB', 'FCP', 'INP', 'first_visit', 'purchase')
                # Gather one more day than datetogather so we get cross midnight joins working
                AND (datetogather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(datetogather, INTERVAL 1 DAY)) AND FORMAT_DATE('%Y%m%d',CURRENT_DATE()))                  
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
  AND (datetogather IS NULL OR PARSE_TIMESTAMP("%Y%m%d",event_date) >= datetogather);
 

  # Purchases 
  # If schema is changed then update both references to the version so the table gets re-built
  IF NOT EXISTS(
    SELECT
      1
    FROM
      `tag_rocket.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'purchases'
      AND option_name = 'description'
      AND option_value LIKE "%Version 4.2%" # queryVersion
  ) 
  THEN
    CREATE OR REPLACE TABLE `tag_rocket.purchases` (
      last_updated TIMESTAMP,
      user_pseudo_id	STRING,
      call_timestamp TIMESTAMP,
      call_sequence INT64,
      transaction_id	STRING,			
      event_timestamp	TIMESTAMP,			
      event_date	STRING,			
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
    OPTIONS (description = 'Version 4.2');  # queryVersion
  END IF;

  ALTER TABLE `tag_rocket.purchases`
  SET OPTIONS (partition_expiration_days = 65); # ExpirationDays

  # 10MB min per query makes this look expensive for small tables.
  SET datetogather = (SELECT TIMESTAMP_TRUNC(TIMESTAMP_ADD(MAX(PARSE_TIMESTAMP("%Y%m%d",event_date)), INTERVAL -lookbackDays DAY), DAY) FROM `tag_rocket.purchases`);

  IF datetogather IS NOT NULL THEN
    DELETE FROM `tag_rocket.purchases` WHERE PARSE_TIMESTAMP("%Y%m%d",event_date) >= datetogather;
  ELSE
    IF maxDaysToLookBackOnInitialQuery IS NOT NULL THEN
      SET datetogather = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL maxDaysToLookBackOnInitialQuery DAY);
    END IF;
  END IF;

 INSERT `tag_rocket.purchases`  
  (
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
  SELECT 
    CURRENT_TIMESTAMP(),
    user_pseudo_id,
    call_timestamp,
    call_sequence,
    IFNULL(purchase_transaction_id, server_purchase_transaction_id) AS transaction_id,
    IFNULL(purchase_event_timestamp, server_purchase_event_timestamp) AS event_timestamp,
    IFNULL(purchase_event_date, server_purchase_event_date) AS event_date,
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
      SAFE.TIMESTAMP_MICROS(ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_timestamp'))) AS call_timestamp,
      ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_sequence')) AS call_sequence,
      SAFE.TIMESTAMP_MICROS(ANY_VALUE(event_timestamp)) AS purchase_event_timestamp,
      MAX(event_date) AS purchase_event_date,
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
    AND (datetogather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(datetogather, INTERVAL 1 DAY)) AND FORMAT_DATE('%Y%m%d',CURRENT_DATE())) # one more day than datetogather so we get cross midnight joins working. 
    GROUP BY purchase_transaction_id
    )
  FULL OUTER JOIN 
    (SELECT 
      SAFE.TIMESTAMP_MICROS(ANY_VALUE(event_timestamp)) AS server_purchase_event_timestamp,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transaction_id') AS server_purchase_transaction_id,
      ANY_VALUE((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'value')) AS server_purchase_revenue,
      ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'method')) AS server_purchase_method,
      COUNT(*) AS server_purchase_events,
      MAX(event_date) AS server_purchase_event_date,
    FROM `DatasetID.events_*` 
    WHERE event_name = 'server_purchase'
    AND (datetogather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(datetogather, INTERVAL 1 DAY)) AND FORMAT_DATE('%Y%m%d',CURRENT_DATE())) # one more day than datetogather so we get cross midnight joins working. 
    GROUP BY server_purchase_transaction_id
    )
  ON purchase_transaction_id = server_purchase_transaction_id
  WHERE (datetogather IS NULL OR PARSE_TIMESTAMP("%Y%m%d",IFNULL(purchase_event_date, server_purchase_event_date)) >= datetogather);

  # Website Errors 

  # If schema is changed then update both references to the version so the table gets re-built
  IF NOT EXISTS(
    SELECT
      1
    FROM
      `tag_rocket.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'website_errors'
      AND option_name = 'description'
      AND option_value LIKE "%Version 4.2%" # queryVersion
  ) 
  THEN
    CREATE OR REPLACE TABLE `tag_rocket.website_errors` (
      last_updated TIMESTAMP,
      user_pseudo_id	STRING,
      call_timestamp TIMESTAMP,
      call_sequence INT64,
      event_timestamp TIMESTAMP,
      event_date	STRING,
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
    OPTIONS (description = 'Version 4.2');  # queryVersion
  END IF;

  ALTER TABLE `tag_rocket.website_errors`
  SET OPTIONS (partition_expiration_days = 65); # ExpirationDays

  # 10MB min per query makes this look expensive for small tables.
  SET datetogather = (SELECT TIMESTAMP_TRUNC(TIMESTAMP_ADD(MAX(PARSE_TIMESTAMP("%Y%m%d",event_date)), INTERVAL -lookbackDays DAY), DAY) FROM `tag_rocket.website_errors`);

  IF datetogather IS NOT NULL THEN
    DELETE FROM `tag_rocket.website_errors` WHERE PARSE_TIMESTAMP("%Y%m%d",event_date) >= datetogather;
  ELSE
    IF maxDaysToLookBackOnInitialQuery IS NOT NULL THEN
      SET datetogather = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL maxDaysToLookBackOnInitialQuery DAY);
    END IF;
  END IF;

  INSERT `tag_rocket.website_errors` 
  (
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
  SELECT 
    CURRENT_TIMESTAMP(),
    user_pseudo_id,
    SAFE.TIMESTAMP_MICROS((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_timestamp')) AS call_timestamp,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_sequence') AS call_sequence,
    SAFE.TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    event_date,
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
  AND (datetogather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',datetogather) AND FORMAT_DATE('%Y%m%d',CURRENT_DATE()));

  # Missing Pages

  # If schema is changed then update both references to the version so the table gets re-built
  IF NOT EXISTS(
    SELECT
      1
    FROM
      `tag_rocket.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'missing_pages'
      AND option_name = 'description'
      AND option_value LIKE "%Version 4.2%" # queryVersion
  ) 
  THEN
    CREATE OR REPLACE TABLE `tag_rocket.missing_pages` (
      last_updated TIMESTAMP,
      user_pseudo_id	STRING,
      call_timestamp TIMESTAMP,
      call_sequence INT64,
      event_timestamp TIMESTAMP,
      event_date	STRING,
      source	STRING,
      medium	STRING,
      campaign	STRING,
      page_location	STRING,
      page_type	STRING,
      page_referrer	STRING
    )
    # or maybe month? each partition should be 1GB https://medium.com/dataseries/costs-and-performance-lessons-after-using-bigquery-with-terabytes-of-data-54a5809ac912
    PARTITION BY TIMESTAMP_TRUNC(event_timestamp, DAY)
    OPTIONS (description = 'Version 4.2'); # queryVersion
  END IF;

  ALTER TABLE `tag_rocket.missing_pages`
  SET OPTIONS (partition_expiration_days = 65); # ExpirationDays

  SET datetogather = (SELECT TIMESTAMP_TRUNC(TIMESTAMP_ADD(MAX(PARSE_TIMESTAMP("%Y%m%d",event_date)), INTERVAL -lookbackDays DAY), DAY) FROM `tag_rocket.missing_pages`);

  IF datetogather IS NOT NULL THEN
    DELETE FROM `tag_rocket.missing_pages` WHERE PARSE_TIMESTAMP("%Y%m%d",event_date) >= datetogather;
  ELSE
    IF maxDaysToLookBackOnInitialQuery IS NOT NULL THEN
      SET datetogather = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL maxDaysToLookBackOnInitialQuery DAY);
    END IF;
  END IF;

  INSERT `tag_rocket.missing_pages` 
  (
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
  SELECT 
    CURRENT_TIMESTAMP(),
    user_pseudo_id,
    SAFE.TIMESTAMP_MICROS((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_timestamp')) AS call_timestamp,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_sequence') AS call_sequence,
    SAFE.TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    event_date,
    # traffic_source.medium AS traffic_medium, # user level
    # traffic_source.name AS traffic_name, # user level
    # traffic_source.source AS traffic_source, # user level
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS campaign,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    (SELECT COALESCE(value.string_value, CAST(value.int_value AS STRING)) FROM UNNEST(event_params) WHERE key = 'page_type') AS page_type,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') AS page_referrer
  FROM `DatasetID.events_*` 
  WHERE event_name = 'page_view'
  AND (datetogather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',datetogather) AND FORMAT_DATE('%Y%m%d',CURRENT_DATE()));
 
  # Billed Queries Log

  IF NOT EXISTS(
    SELECT
      1
    FROM
      `tag_rocket.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'query_logs'
      AND option_name = 'description'
      AND option_value LIKE "%Version 4.2%" # queryVersion
  ) 
  THEN
    CREATE OR REPLACE TABLE `tag_rocket.query_logs` (
      day_timestamp TIMESTAMP,
      principal_email	STRING,
      gb_billed INT64,
    #  gb_processed INT64,
    #  query_count INT64,
      billed_query_count	INT64,
    error_count	INT64
    )
    PARTITION BY DATE(day_timestamp)
    OPTIONS (description = 'Version 4.2'); # queryVersion
  END IF;

  SET datetogather = (SELECT TIMESTAMP_TRUNC(TIMESTAMP_ADD(MAX(day_timestamp), INTERVAL -1 DAY), DAY) FROM `tag_rocket.query_logs`);

  IF datetogather IS NOT NULL THEN
    DELETE FROM `tag_rocket.query_logs` WHERE day_timestamp >= datetogather;
  ELSE
    IF maxDaysToLookBackOnInitialQuery IS NOT NULL THEN
      SET datetogather = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL maxDaysToLookBackOnInitialQuery DAY);
    END IF;
  END IF;

  IF EXISTS(
    SELECT
      1
    FROM
      `bq_logs.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name LIKE "cloudaudit_googleapis_com_data_access_%"
  ) 
  THEN
    INSERT `tag_rocket.query_logs` 
    (
      day_timestamp,
      principal_email,
      gb_billed,
      billed_query_count,
      error_count
    )
    SELECT
      TIMESTAMP_TRUNC(timestamp, DAY) AS day_timestamp,
      protopayload_auditlog.authenticationInfo.principalEmail AS principal_email,
      SUM(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes/(1024*1024*1024)) AS gb_billed, 
    #  SUM(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes/(1024*1024*1024)) AS gb_processed,
      COUNT(1) AS billed_query_count,
    #  COUNTIF(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes > 0) AS billed_query_count,
    COUNTIF(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.message IS NOT NULL) AS error_count
    FROM
      `bq_logs.cloudaudit_googleapis_com_data_access_*`
    WHERE datetogather IS NULL OR TIMESTAMP_TRUNC(timestamp, DAY) >= datetogather
    GROUP BY 1, 2
    ORDER BY day_timestamp DESC, principal_email;
  END IF;

END;