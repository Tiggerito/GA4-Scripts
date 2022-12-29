# Tag Rocket Report Data Incremental 2 v5.0
# https://github.com/Tiggerito/GA4-Scripts/blob/main/bigquery-tag-rocket-report-data-incremental2.sql

# Replace all occurances of ${DatasetID} with your Dataset ID for the GA4 export. Something like analytics_1234567890
# Replace all occurances of ${ProjectID} with your Project ID for the GA4 export.

# make sure you run this using the same location as your analytics dataset

# this query will delete an recreate tables if they are for an older verios. Which means historical data may be lost.

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

  DECLARE timestampToGather DEFAULT CURRENT_TIMESTAMP(); # dummy value. gets updated before every use
  DECLARE dateToGather DEFAULT CURRENT_DATE(); # dummy value. gets updated before every use

  CREATE SCHEMA IF NOT EXISTS `${ProjectID}.tag_rocket`
  OPTIONS (
    default_partition_expiration_days = 65, # ExpirationDays
    description = 'Data for the Tag Rocket Report'
  );

  CREATE SCHEMA IF NOT EXISTS `${ProjectID}.bq_logs`
  OPTIONS (
    default_table_expiration_days = 65, # ExpirationDays
    description = 'Destination for the Log Sink of billed queries'
  );


  # meta data
  
  CREATE OR REPLACE TABLE `${ProjectID}.tag_rocket.meta_data` (
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
      last_exported	STRING,			     
      partition_expiration STRING,	
      bigquery_project_id STRING,	
      ga4_account_id STRING,	
      ga4_property_id STRING,		  
      last_report_version	STRING,	
      query_version	STRING,				
      last_run_timestamp	TIMESTAMP
    )
  OPTIONS (description = 'Version 5.1') # queryVersion
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
    '', # last_exported: set when using Tag Rocket to run the query. format '[yyyy-MM-dd]'
    '', # partition_expiration
    '', # bigquery_project_id
    '', # ga4_account_id
    '', # ga4_property_id
    '', # last_report_version
    '5.1', # query_version queryVersion
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
      `${ProjectID}.tag_rocket.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'web_vitals'
      AND option_name = 'description'
      AND option_value LIKE "%Version 5.0%" # queryVersion
  ) 
  THEN
    DROP TABLE IF EXISTS `${ProjectID}.tag_rocket.web_vitals`;
    CREATE TABLE `${ProjectID}.tag_rocket.web_vitals` (
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
      event_date	DATE,			
      metric_status	STRING
    )
    PARTITION BY event_date
    CLUSTER BY metric_name
    OPTIONS (description = 'Version 5.0');  # queryVersion
  END IF;

  ALTER TABLE `${ProjectID}.tag_rocket.web_vitals`
  SET OPTIONS (partition_expiration_days = 65); # ExpirationDays

  # 10MB min per query makes this look expensive for small tables.
  SET dateToGather = (SELECT DATE_SUB(MAX(event_date), INTERVAL lookbackDays DAY) FROM `${ProjectID}.tag_rocket.web_vitals`);

  IF dateToGather IS NOT NULL THEN
    DELETE FROM `${ProjectID}.tag_rocket.web_vitals` WHERE event_date >= dateToGather;
  ELSE
    IF maxDaysToLookBackOnInitialQuery IS NOT NULL THEN
      SET dateToGather = DATE_SUB(CURRENT_DATE(), INTERVAL maxDaysToLookBackOnInitialQuery DAY);
    END IF;
  END IF;

  INSERT `${ProjectID}.tag_rocket.web_vitals` 
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
              debug_target,
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
                (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id, # can be null in consent mode making grouping bad ?
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'metric_id') AS metric_id,

                SAFE.TIMESTAMP_MILLIS(ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_timestamp'))) AS call_timestamp,
                ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_sequence')) AS call_sequence,
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
                
                ARRAY_TO_STRING([ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'debug_target')), ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'debug_target2'))], '') AS debug_target,
                ANY_VALUE(user_pseudo_id) AS user_pseudo_id,
                ANY_VALUE(geo.country) AS country,
                ANY_VALUE(event_name) AS event_name,
                SUM(ecommerce.purchase_revenue) AS session_revenue, # TODO: if we use revenue need to also pull in the currency and the USD value. see the purchases query
                MAX(
                  (
                    SELECT
                      COALESCE(
                        value.double_value, value.int_value, CAST(value.string_value AS NUMERIC))
                    FROM UNNEST(event_params)
                    WHERE key = 'session_engaged'
                  )) AS session_engaged,
                SAFE.TIMESTAMP_MICROS(MAX(event_timestamp)) AS event_timestamp,
                MAX(PARSE_DATE('%Y%m%d', event_date)) AS event_date,
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
                `${ProjectID}.${DatasetID}.events_*`
              WHERE
                event_name IN ('LCP', 'FID', 'CLS', 'TTFB', 'FCP', 'INP', 'first_visit', 'purchase')
                AND (dateToGather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(dateToGather, INTERVAL 1 DAY)) AND FORMAT_DATE('%Y%m%d',CURRENT_DATE()))                  
              GROUP BY
                1, 2
            )
        )
      WHERE
        ga_session_id IS NOT NULL
      GROUP BY ga_session_id # can be null in consent mode making grouping bad ?
    )
  CROSS JOIN UNNEST(events) AS evt
  WHERE evt.event_name NOT IN ('first_visit', 'purchase')
  AND (dateToGather IS NULL OR event_date >= dateToGather);
 

  # Purchases 
  # If schema is changed then update both references to the version so the table gets re-built
  IF NOT EXISTS(
    SELECT
      1
    FROM
      `${ProjectID}.tag_rocket.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'purchases'
      AND option_name = 'description'
      AND option_value LIKE "%Version 5.1%" # queryVersion
  ) 
  THEN
    DROP TABLE IF EXISTS `${ProjectID}.tag_rocket.purchases`;  
    CREATE TABLE `${ProjectID}.tag_rocket.purchases` (  
      last_updated TIMESTAMP,
      user_pseudo_id	STRING,
      user_id	STRING,
      transaction_id	STRING,			
      event_timestamp	TIMESTAMP,			
      event_date DATE,			## NEW WAY: DATE 
      purchase_event_timestamp	TIMESTAMP,			
      purchase_revenue	FLOAT64,	
      purchase_currency	STRING,
      purchase_shipping_value FLOAT64,		
      purchase_tax_value FLOAT64,		
      purchase_refund_value FLOAT64,	
      purchase_revenue_in_usd FLOAT64,
      purchase_shipping_value_in_usd FLOAT64,
      purchase_tax_value_in_usd FLOAT64,
      purchase_refund_value_in_usd FLOAT64,	
      purchase_events	INT64,			
      server_purchase_event_timestamp	TIMESTAMP,			
      server_purchase_revenue	FLOAT64,
      server_purchase_currency	STRING,   
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
    PARTITION BY event_date
    OPTIONS (description = 'Version 5.1');  # queryVersion  
  END IF;

  ALTER TABLE `${ProjectID}.tag_rocket.purchases`
  SET OPTIONS (partition_expiration_days = 65); # ExpirationDays

  SET dateToGather = (SELECT DATE_SUB(MAX(event_date), INTERVAL lookbackDays DAY) FROM `${ProjectID}.tag_rocket.purchases`); 

  IF dateToGather IS NOT NULL THEN
    DELETE FROM `${ProjectID}.tag_rocket.purchases` WHERE event_date >= dateToGather;
  ELSE
    IF maxDaysToLookBackOnInitialQuery IS NOT NULL THEN
      SET dateToGather = DATE_SUB(CURRENT_DATE(), INTERVAL maxDaysToLookBackOnInitialQuery DAY);
    END IF;
  END IF;

 INSERT `${ProjectID}.tag_rocket.purchases`  
  (
      last_updated,
      user_pseudo_id,
      user_id,
      transaction_id,
      event_timestamp,
      event_date,
      purchase_event_timestamp,
      purchase_revenue,
      purchase_currency,
      purchase_shipping_value,
      purchase_tax_value,
      purchase_refund_value,
      purchase_revenue_in_usd,
      purchase_shipping_value_in_usd,
      purchase_tax_value_in_usd,
      purchase_refund_value_in_usd,
      purchase_events,
      server_purchase_event_timestamp,
      server_purchase_revenue,
      server_purchase_currency,
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
    user_id,
    IFNULL(purchase_transaction_id, server_purchase_transaction_id) AS transaction_id,
    IFNULL(purchase_event_timestamp, server_purchase_event_timestamp) AS event_timestamp,
    IFNULL(purchase_event_date, server_purchase_event_date) AS event_date, ## NEW WAY: DATE
    purchase_event_timestamp,
    purchase_revenue,
    purchase_currency,
    purchase_shipping_value,
    purchase_tax_value,
    purchase_refund_value,
    purchase_revenue_in_usd,
    purchase_shipping_value_in_usd,
    purchase_tax_value_in_usd,
    purchase_refund_value_in_usd,
    purchase_events,
    server_purchase_event_timestamp,
    server_purchase_revenue,
    server_purchase_currency,
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
      ANY_VALUE(user_id) AS user_id,
      SAFE.TIMESTAMP_MICROS(ANY_VALUE(event_timestamp)) AS purchase_event_timestamp,
      MAX(PARSE_DATE('%Y%m%d', event_date)) AS purchase_event_date,
      ecommerce.transaction_id AS purchase_transaction_id,
      ANY_VALUE(ecommerce.purchase_revenue) AS purchase_revenue,
      ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'currency')) AS purchase_currency,
      ANY_VALUE(ecommerce.shipping_value) AS purchase_shipping_value,
      ANY_VALUE(ecommerce.tax_value) AS purchase_tax_value,
      ANY_VALUE(ecommerce.refund_value) AS purchase_refund_value,
      ANY_VALUE(ecommerce.purchase_revenue_in_usd) AS purchase_revenue_in_usd,
      ANY_VALUE(ecommerce.shipping_value_in_usd) AS purchase_shipping_value_in_usd,
      ANY_VALUE(ecommerce.tax_value_in_usd) AS purchase_tax_value_in_usd,
      ANY_VALUE(ecommerce.refund_value_in_usd) AS purchase_refund_value_in_usd,
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
    FROM `${ProjectID}.${DatasetID}.events_*` 
    WHERE event_name = 'purchase'
    AND (dateToGather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(dateToGather, INTERVAL 1 DAY)) AND FORMAT_DATE('%Y%m%d',CURRENT_DATE())) # one more day than dateToGather so we get cross midnight joins working. 
    GROUP BY purchase_transaction_id
    )
  FULL OUTER JOIN 
    (SELECT 
      SAFE.TIMESTAMP_MICROS(ANY_VALUE(event_timestamp)) AS server_purchase_event_timestamp,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transaction_id') AS server_purchase_transaction_id,
      ANY_VALUE((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'value')) AS server_purchase_revenue,
      ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'currency')) AS server_purchase_currency,
      ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'method')) AS server_purchase_method,
      COUNT(*) AS server_purchase_events,
      MAX(PARSE_DATE('%Y%m%d', event_date)) AS server_purchase_event_date, ## NEW WAY: DATE
    FROM `${ProjectID}.${DatasetID}.events_*` 
    WHERE event_name = 'server_purchase'
    AND (dateToGather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(dateToGather, INTERVAL 1 DAY)) AND FORMAT_DATE('%Y%m%d',CURRENT_DATE())) # one more day than dateToGather so we get cross midnight joins working. 
    GROUP BY server_purchase_transaction_id
    )
  ON purchase_transaction_id = server_purchase_transaction_id
  WHERE (dateToGather IS NULL OR IFNULL(purchase_event_date, server_purchase_event_date) >= dateToGather); 

  # Website Errors 

  # If schema is changed then update both references to the version so the table gets re-built
  IF NOT EXISTS(
    SELECT
      1
    FROM
      `${ProjectID}.tag_rocket.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'website_errors'
      AND option_name = 'description'
      AND option_value LIKE "%Version 5.0%" # queryVersion
  ) 
  THEN
    DROP TABLE IF EXISTS `${ProjectID}.tag_rocket.website_errors`;
    CREATE TABLE `${ProjectID}.tag_rocket.website_errors` (
      last_updated TIMESTAMP,
      user_pseudo_id	STRING,
      call_timestamp TIMESTAMP,
      call_sequence INT64,
      event_timestamp TIMESTAMP,
      event_date	DATE,
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
    PARTITION BY event_date
    OPTIONS (description = 'Version 5.0');  # queryVersion
  END IF;

  ALTER TABLE `${ProjectID}.tag_rocket.website_errors`
  SET OPTIONS (partition_expiration_days = 65); # ExpirationDays

  SET dateToGather = (SELECT DATE_SUB(MAX(event_date), INTERVAL lookbackDays DAY) FROM `${ProjectID}.tag_rocket.website_errors`);

  IF dateToGather IS NOT NULL THEN
    DELETE FROM `${ProjectID}.tag_rocket.website_errors` WHERE event_date >= dateToGather;
  ELSE
    IF maxDaysToLookBackOnInitialQuery IS NOT NULL THEN
       SET dateToGather = DATE_SUB(CURRENT_DATE(), INTERVAL maxDaysToLookBackOnInitialQuery DAY);
   END IF;
  END IF;

  INSERT `${ProjectID}.tag_rocket.website_errors` 
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
    SAFE.TIMESTAMP_MILLIS((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_timestamp')) AS call_timestamp,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_sequence') AS call_sequence,
    SAFE.TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    PARSE_DATE('%Y%m%d', event_date),
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
  FROM `${ProjectID}.${DatasetID}.events_*` 
  WHERE event_name = 'exception'
  AND (dateToGather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',dateToGather) AND FORMAT_DATE('%Y%m%d',CURRENT_DATE()));

  # Missing Pages

  # If schema is changed then update both references to the version so the table gets re-built
  IF NOT EXISTS(
    SELECT
      1
    FROM
      `${ProjectID}.tag_rocket.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'missing_pages'
      AND option_name = 'description'
      AND option_value LIKE "%Version 5.0%" # queryVersion
  ) 
  THEN
    DROP TABLE IF EXISTS `${ProjectID}.tag_rocket.missing_pages`;
    CREATE TABLE `${ProjectID}.tag_rocket.missing_pages` (
      last_updated TIMESTAMP,
      user_pseudo_id	STRING,
      call_timestamp TIMESTAMP,
      call_sequence INT64,
      event_timestamp TIMESTAMP,
      event_date	DATE,
      source	STRING,
      medium	STRING,
      campaign	STRING,
      page_location	STRING,
      page_type	STRING,
      page_referrer	STRING
    )
    PARTITION BY event_date
    OPTIONS (description = 'Version 5.0'); # queryVersion
  END IF;

  ALTER TABLE `${ProjectID}.tag_rocket.missing_pages`
  SET OPTIONS (partition_expiration_days = 65); # ExpirationDays

  SET dateToGather = (SELECT DATE_SUB(MAX(event_date), INTERVAL lookbackDays DAY) FROM `${ProjectID}.tag_rocket.missing_pages`);

  IF dateToGather IS NOT NULL THEN
    DELETE FROM `${ProjectID}.tag_rocket.missing_pages` WHERE event_date >= dateToGather;
  ELSE
    IF maxDaysToLookBackOnInitialQuery IS NOT NULL THEN
       SET dateToGather = DATE_SUB(CURRENT_DATE(), INTERVAL maxDaysToLookBackOnInitialQuery DAY);
    END IF;
  END IF;

  INSERT `${ProjectID}.tag_rocket.missing_pages` 
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
    SAFE.TIMESTAMP_MILLIS((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_timestamp')) AS call_timestamp,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'call_sequence') AS call_sequence,
    SAFE.TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    PARSE_DATE('%Y%m%d', event_date),
    # traffic_source.medium AS traffic_medium, # user level
    # traffic_source.name AS traffic_name, # user level
    # traffic_source.source AS traffic_source, # user level
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS campaign,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    (SELECT COALESCE(value.string_value, CAST(value.int_value AS STRING)) FROM UNNEST(event_params) WHERE key = 'page_type') AS page_type,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') AS page_referrer
  FROM `${ProjectID}.${DatasetID}.events_*` 
  WHERE event_name = 'page_view'
  AND (dateToGather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',dateToGather) AND FORMAT_DATE('%Y%m%d',CURRENT_DATE()));

  # User Sessions

  # If schema is changed then update both references to the version so the table gets re-built
  IF NOT EXISTS(
    SELECT
      1
    FROM
      `${ProjectID}.tag_rocket.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'user_sessions'
      AND option_name = 'description'
      AND option_value LIKE "%Version 5.1%" # queryVersion
  ) 
  THEN
    DROP TABLE IF EXISTS `${ProjectID}.tag_rocket.user_sessions`;
    CREATE TABLE `${ProjectID}.tag_rocket.user_sessions` (
      user_pseudo_id STRING,
      ga_session_id INT64,
      last_updated TIMESTAMP,
      session_date DATE,
      session_start_timestamp TIMESTAMP,
      session_end_timestamp TIMESTAMP,
      user_id STRING,
      session_first_visit INT64,
      #session_first_purchase INT64,
      session_page_view_count INT64,
      session_view_item_list_count INT64,
      session_view_item_count INT64,
      session_select_item_count INT64,
      session_add_to_cart_count INT64,
      session_view_cart_count INT64,
      session_begin_checkout_count INT64,
      session_add_customer_info_count INT64,
      session_add_shipping_info_count INT64,
      session_add_billing_info_count INT64,
      session_purchase_count INT64,

      session_purchase_revenue FLOAT64,
      session_purchase_currency STRING,
      session_purchase_revenue_in_usd FLOAT64,

      session_device_category	STRING,
      session_device_browser	STRING,
      session_device_operating_system	STRING,
      session_country	STRING,
      session_landing_page	STRING,
      session_landing_page_type	STRING,
      session_referrer	STRING,
      session_campaign	STRING,
      session_source	STRING,
      session_medium	STRING,
      session_term	STRING,
      session_gclid	STRING,
      customer_group_name	STRING,

      user_ltv_revenue	FLOAT64,
      user_ltv_currency	STRING,
      user_campaign	STRING,
      user_medium	STRING,
      user_source	STRING,
    )
    PARTITION BY session_date
    OPTIONS (description = 'Version 5.1'); # queryVersion
  END IF;

  ALTER TABLE `${ProjectID}.tag_rocket.user_sessions`
  SET OPTIONS (partition_expiration_days = NULL); # ExpirationDays

  SET dateToGather = (SELECT DATE_SUB(MAX(session_date), INTERVAL lookbackDays DAY) FROM `${ProjectID}.tag_rocket.user_sessions`);

  IF dateToGather IS NOT NULL THEN
    DELETE FROM `${ProjectID}.tag_rocket.user_sessions` WHERE session_date >= dateToGather;
  ELSE
    IF maxDaysToLookBackOnInitialQuery IS NOT NULL THEN
       SET dateToGather = DATE_SUB(CURRENT_DATE(), INTERVAL maxDaysToLookBackOnInitialQuery DAY);
    END IF;
  END IF;

  INSERT `${ProjectID}.tag_rocket.user_sessions` 
  (  
      user_pseudo_id,
      ga_session_id,
      last_updated,
      session_date,
      session_start_timestamp,
      session_end_timestamp,
      user_id,
      session_first_visit,
      #session_first_purchase,
      session_page_view_count,
      session_view_item_list_count,
      session_view_item_count,
      session_select_item_count,
      session_add_to_cart_count,
      session_view_cart_count,
      session_begin_checkout_count,
      session_add_customer_info_count,
      session_add_shipping_info_count,
      session_add_billing_info_count,
      session_purchase_count,

      session_purchase_revenue,
      session_purchase_currency,
      session_purchase_revenue_in_usd,

      session_device_category,
      session_device_browser,
      session_device_operating_system,
      session_country,
      session_landing_page,
      session_landing_page_type,
      session_referrer,
      session_campaign,
      session_source,
      session_medium,
      session_term,
      session_gclid,
      customer_group_name,

      user_ltv_revenue,
      user_ltv_currency,
      user_campaign,
      user_medium,
      user_source
  )
  SELECT 
    
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id, # not unique, but is per user_pseudo_id
    CURRENT_TIMESTAMP(),
    MIN(PARSE_DATE('%Y%m%d', event_date)) AS session_date,  
    SAFE.TIMESTAMP_MICROS(MIN(event_timestamp)) AS session_start_timestamp, 
    SAFE.TIMESTAMP_MICROS(MAX(event_timestamp)) AS session_end_timestamp, 
    # session length
    ANY_VALUE(user_id) AS user_id,

    MAX(IF(event_name = 'first_visit',1,0)) AS session_first_visit,
    #MAX(IF(event_name = 'first_purchase',1,0)) AS session_first_purchase,

    SUM(IF(event_name = 'page_view',1,0)) AS session_page_view_count,
    SUM(IF(event_name = 'view_item_list',1,0)) AS session_view_item_list_count,
    SUM(IF(event_name = 'view_item',1,0)) AS session_view_item_count,
    SUM(IF(event_name = 'select_item',1,0)) AS session_select_item_count,
    SUM(IF(event_name = 'add_to_cart',1,0)) AS session_add_to_cart_count,
    SUM(IF(event_name = 'view_cart',1,0)) AS session_view_cart_count,
    SUM(IF(event_name = 'begin_checkout',1,0)) AS session_begin_checkout_count,
    SUM(IF(event_name = 'add_customer_info',1,0)) AS session_add_customer_info_count,
    SUM(IF(event_name = 'add_shipping_info',1,0)) AS session_add_shipping_info_count, 
    SUM(IF(event_name = 'add_billing_info',1,0)) AS session_add_billing_info_count,
    #SUM(IF(event_name = 'add_payment_info',1,0)) AS session_add_payment_info_count, # same as purchase
    SUM(IF(event_name = 'purchase',1,0)) AS session_purchase_count,
    #SUM(IF(event_name = 'search',1,0)) AS session_search_count,

    SUM(ecommerce.purchase_revenue) AS session_purchase_revenue, # is this only present on the purchase event?
    ANY_VALUE(IF(event_name = 'purchase',(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'currency'), NULL)) AS session_purchase_currency,
    SUM(ecommerce.purchase_revenue_in_usd) AS session_purchase_revenue_in_usd, # is this only present on the purchase event?



    ANY_VALUE(device.category) AS session_device_category,
    ANY_VALUE(device.web_info.browser) AS session_device_browser,
    ANY_VALUE(device.operating_system) AS session_device_operating_system,

    ANY_VALUE(geo.country) AS session_country,

    # channel grouping (user/session) ????

    ANY_VALUE(IF(event_name = 'session_start',(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), NULL)) AS session_landing_page,
    ARRAY_AGG(IF(event_name = 'page_view',(SELECT COALESCE(value.string_value, CAST(value.int_value AS STRING)) FROM UNNEST(event_params) WHERE key = 'page_type'), NULL) IGNORE NULLS ORDER BY event_timestamp LIMIT 1 )[OFFSET(0)] AS session_landing_page_type,
    ANY_VALUE(IF(event_name = 'session_start',(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer'), NULL)) AS session_referrer,

    ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign')) AS session_campaign,
    ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source')) AS session_source,
    ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium')) AS session_medium,
    ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'term')) AS session_term,
    ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'gclid')) AS session_gclid,


    ANY_VALUE((SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'customer_group_name')) AS customer_group_name,
    MAX(user_ltv.revenue) AS	user_ltv_revenue,
    MAX(user_ltv.currency) AS user_ltv_currency,

    ANY_VALUE(traffic_source.name)	AS user_campaign,
    ANY_VALUE(traffic_source.medium)	AS user_medium,
    ANY_VALUE(traffic_source.source)	AS user_source
  FROM `${ProjectID}.${DatasetID}.events_*` 
  WHERE event_name IN ('session_start', 'page_view', 'purchase', 'add_to_cart', 'begin_checkout', 'view_cart', 'view_item', 'view_item_list', 'first_visit', 'select_item', 'add_customer_info', 'add_shipping_info', 'add_billing_info')
  AND (dateToGather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',dateToGather) AND FORMAT_DATE('%Y%m%d',CURRENT_DATE()))
  AND user_pseudo_id IS NOT NULL
  GROUP BY 1, 2
  HAVING session_page_view_count > 0; 

  # Users

   # If schema is changed then update both references to the version so the table gets re-built
  IF NOT EXISTS(
    SELECT
      1
    FROM
      `${ProjectID}.tag_rocket.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'users'
      AND option_name = 'description'
      AND option_value LIKE "%Version 5.0%" # queryVersion
  ) 
  THEN
    DROP TABLE IF EXISTS `${ProjectID}.tag_rocket.users`;
    CREATE TABLE `${ProjectID}.tag_rocket.users` (
      user_pseudo_id STRING,
      last_updated TIMESTAMP,
      first_visit_timestamp TIMESTAMP,
      first_visit_day DATE,
      first_landing_page STRING,
      first_landing_page_type	STRING,
      first_referrer	STRING,
      user_campaign	STRING,
      user_medium	STRING,
      user_source	STRING,
      customer BOOL,
      last_active TIMESTAMP,
      first_purchase_ga_session_id INT64,
      user_ltv_revenue FLOAT64,
      user_ltv_currency STRING
    )
    PARTITION BY first_visit_day
    OPTIONS (description = 'Version 5.0'); # queryVersion
  END IF;

# keep users forever
  ALTER TABLE `${ProjectID}.tag_rocket.users`
  SET OPTIONS (partition_expiration_days = NULL); # ExpirationDays

  SET dateToGather = (SELECT DATE_SUB(MAX(first_visit_day), INTERVAL lookbackDays DAY) FROM `${ProjectID}.tag_rocket.users`);

  IF dateToGather IS NOT NULL THEN
    DELETE FROM `${ProjectID}.tag_rocket.users` WHERE first_visit_day >= dateToGather;
  #ELSE
  #  IF maxDaysToLookBackOnInitialQuery IS NOT NULL THEN
  #    SET dateToGather = DATE_SUB(CURRENT_DATE(), INTERVAL maxDaysToLookBackOnInitialQuery DAY);
  #  END IF;
  END IF;

  MERGE INTO `${ProjectID}.tag_rocket.users` A 
  USING (
    SELECT    
    user_pseudo_id,
    CURRENT_TIMESTAMP(),
    MIN(session_start_timestamp) AS first_visit_timestamp, # only set on creation
    MAX(session_end_timestamp) AS last_active, 
    MIN(session_date) AS first_visit_day,  
    ARRAY_AGG(session_landing_page IGNORE NULLS ORDER BY session_start_timestamp LIMIT 1 )[OFFSET(0)] AS first_landing_page, # only set on creation
    ARRAY_AGG(session_landing_page_type IGNORE NULLS ORDER BY session_start_timestamp LIMIT 1 )[OFFSET(0)] AS first_landing_page_type, # only set on creation
    ARRAY_AGG(session_referrer IGNORE NULLS ORDER BY session_start_timestamp LIMIT 1 )[OFFSET(0)] AS first_referrer, # only set on creation
    ANY_VALUE(user_campaign) AS user_campaign,# only set on creation
    ANY_VALUE(user_medium) AS user_medium,# only set on creation
    ANY_VALUE(user_source) AS user_source,# only set on creation
    SUM(session_purchase_count) AS purchase_count,
    MIN(IF(session_purchase_count > 0, ga_session_id, NULL)) AS first_purchase_ga_session_id,
    ARRAY_AGG(user_ltv_revenue IGNORE NULLS ORDER BY session_start_timestamp DESC LIMIT 1 )[OFFSET(0)] AS user_ltv_revenue,
    ARRAY_AGG(user_ltv_currency IGNORE NULLS ORDER BY session_start_timestamp DESC LIMIT 1 )[OFFSET(0)] AS user_ltv_currency
  FROM `${ProjectID}.tag_rocket.user_sessions` 
 
  GROUP BY 1
   HAVING dateToGather IS NULL OR first_visit_day >= dateToGather

  ) B
  ON A.user_pseudo_id = B.user_pseudo_id
  WHEN MATCHED THEN UPDATE SET 
    A.last_updated = CURRENT_TIMESTAMP(),
    A.customer = A.customer OR B.purchase_count > 0,
    A.last_active = B.last_active,
    A.user_ltv_revenue = B.user_ltv_revenue,
    A.user_ltv_currency = B.user_ltv_currency,
    A.first_purchase_ga_session_id = IFNULL(A.first_purchase_ga_session_id, B.first_purchase_ga_session_id)
  WHEN NOT MATCHED THEN INSERT (
      user_pseudo_id,
      last_updated,
      first_visit_timestamp,
      first_visit_day,
      first_landing_page,
      first_landing_page_type,
      first_referrer,
      user_campaign,
      user_medium,
      user_source,
      customer,
      last_active,
      first_purchase_ga_session_id,
      user_ltv_revenue,
      user_ltv_currency
  )
  VALUES (
    user_pseudo_id,
    CURRENT_TIMESTAMP(),
    first_visit_timestamp, # only set on creation
    first_visit_day,  
    first_landing_page, # only set on creation
    first_landing_page_type, # only set on creation
    first_referrer, # only set on creation
    user_campaign,# only set on creation
    user_medium,# only set on creation
    user_source,# only set on creation
    purchase_count > 0,
    last_active,
    first_purchase_ga_session_id,
    user_ltv_revenue,
    user_ltv_currency
  );

  # Billed Queries Log

  IF NOT EXISTS(
    SELECT
      1
    FROM
      `${ProjectID}.tag_rocket.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'query_logs'
      AND option_name = 'description'
      AND option_value LIKE "%Version 5.0%" # queryVersion
  ) 
  THEN
    DROP TABLE IF EXISTS `${ProjectID}.tag_rocket.query_logs`;
    CREATE TABLE `${ProjectID}.tag_rocket.query_logs` (
      day_timestamp TIMESTAMP,
      principal_email	STRING,
      billed_bytes INT64,
    #  processed_bytes INT64,
    #  query_count INT64,
      billed_query_count	INT64,
      error_count	INT64,
      budget_trendline_bytes INT64,
      rolling_total_bytes INT64,
      month_to_date_bytes INT64
    )
    PARTITION BY DATE(day_timestamp)
    OPTIONS (description = 'Version 5.0'); # queryVersion

    INSERT `${ProjectID}.tag_rocket.query_logs` (day_timestamp, principal_email, billed_bytes, billed_query_count, error_count, budget_trendline_bytes, rolling_total_bytes, month_to_date_bytes)
  VALUES(current_timestamp(),'',0,0,0,0,0,0);
  END IF;

  SET timestampToGather = (SELECT TIMESTAMP_TRUNC(TIMESTAMP_ADD(MAX(day_timestamp), INTERVAL -1 DAY), DAY) FROM `${ProjectID}.tag_rocket.query_logs`);

  IF timestampToGather IS NOT NULL THEN
    DELETE FROM `${ProjectID}.tag_rocket.query_logs` WHERE day_timestamp >= timestampToGather;
  ELSE
    IF maxDaysToLookBackOnInitialQuery IS NOT NULL THEN
      SET timestampToGather = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL maxDaysToLookBackOnInitialQuery DAY);
    END IF;
  END IF;

  IF EXISTS(
    SELECT
      1
    FROM
      `${ProjectID}.bq_logs.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name LIKE "cloudaudit_googleapis_com_data_access_%"
  ) 
  THEN
    INSERT `${ProjectID}.tag_rocket.query_logs` 
    (
      day_timestamp,
      principal_email,
      billed_bytes,
      billed_query_count,
      error_count,
      budget_trendline_bytes
    )
    SELECT
      TIMESTAMP_TRUNC(timestamp, DAY) AS day_timestamp,
      protopayload_auditlog.authenticationInfo.principalEmail AS principal_email,
      SUM(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes) AS billed_bytes,  
      COUNT(1) AS billed_query_count,
      COUNTIF(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.message IS NOT NULL) AS error_count,
      CAST((EXTRACT(DAY FROM CURRENT_DATE()) * 1000 * 1000 * 1000 * 1000) / EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE())) AS INT64) AS budget_trendline_bytes
    FROM
      `${ProjectID}.bq_logs.cloudaudit_googleapis_com_data_access_*`
    WHERE timestampToGather IS NULL OR TIMESTAMP_TRUNC(timestamp, DAY) >= timestampToGather
    GROUP BY 1, 2
    ORDER BY day_timestamp DESC, principal_email;
  END IF;

  # rolling 31 day total
  UPDATE `${ProjectID}.tag_rocket.query_logs` AS MAIN
  SET rolling_total_bytes = (SELECT 
          SUM(billed_bytes) 
          FROM `${ProjectID}.tag_rocket.query_logs` AS SUB
          WHERE SUB.day_timestamp <= MAIN.day_timestamp AND SUB.day_timestamp > DATE_SUB(MAIN.day_timestamp,INTERVAL 31 DAY) 
        )
  WHERE rolling_total_bytes IS NULL;

  UPDATE `${ProjectID}.tag_rocket.query_logs` AS MAIN
  SET month_to_date_bytes = (SELECT 
        SUM(billed_bytes) 
        FROM `${ProjectID}.tag_rocket.query_logs` AS SUB
        WHERE SUB.day_timestamp <= MAIN.day_timestamp
        AND 
        EXTRACT(MONTH FROM SUB.day_timestamp) = EXTRACT(MONTH FROM MAIN.day_timestamp)
        AND 
        EXTRACT(YEAR FROM SUB.day_timestamp) = EXTRACT(YEAR FROM MAIN.day_timestamp)
      )
  WHERE month_to_date_bytes IS NULL;

  # UPDATE `${ProjectID}.tag_rocket.query_logs` AS MAIN
  # SET budget_trendline_bytes = CAST((EXTRACT(DAY FROM day_timestamp) * 1000) / EXTRACT(DAY FROM LAST_DAY(EXTRACT(DATETIME FROM day_timestamp))) AS INT64)
  # WHERE budget_trendline_bytes IS NULL;

   # creating dummy data
   # INSERT `${ProjectID}.tag_rocket.query_logs` (day_timestamp, gb_billed)
   # VALUES(current_timestamp(), 10),
   #     (DATE_SUB(current_timestamp(), INTERVAL 1 DAY), 20),
   #     (DATE_SUB(current_timestamp(), INTERVAL 2 DAY), 30),
   #     (DATE_SUB(current_timestamp(), INTERVAL 3 DAY), 15),
   #     (DATE_SUB(current_timestamp(), INTERVAL 4 DAY), 25),
   #     (DATE_SUB(current_timestamp(), INTERVAL 5 DAY), 20),
   #     (DATE_SUB(current_timestamp(), INTERVAL 6 DAY), 40);

#CONCATENATE("DATE_SUB(current_timestamp(), INTERVAL ", A2, " DAY), ", B2, "),")
END;