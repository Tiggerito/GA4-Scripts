# Tag Rocket GA4 Sessions table v5.7
# https://github.com/Tiggerito/GA4-Scripts/blob/main/bigquery-tag-rocket-ga4-sessions.sql

# Replace all occurances of ${ProjectID} with your Project ID for the GA4 export.
# Replace all occurances of ${DatasetID} with your Dataset ID for the GA4 export. Something like analytics_1234567890

# make sure you run this using the same location as your analytics dataset

# this query will delete an recreate tables if they are for an older versions. Which means historical data may be lost.

BEGIN
  # The first run with gather all data. After that it will gather new data and merge the last 3 days of data (lookbackDays+1) 
  DECLARE lookbackDays DEFAULT 2; 
  # GA4 states they add events that happen 72 hours after the fact, and then re-export the relevant days table. Hence always replacing the last three days of data

  # NULL means process all the current GA4 data
  DECLARE maxDaysToLookBackOnInitialQuery DEFAULT NULL; 

  # declair some variables for use by the queries
  DECLARE timestampToGather DEFAULT CURRENT_TIMESTAMP(); 
  DECLARE dateToGather DEFAULT CURRENT_DATE(); 

  # create our tag_rocket dataset if it does not exist yet
  CREATE SCHEMA IF NOT EXISTS `${ProjectID}.tag_rocket`
  OPTIONS (
    default_partition_expiration_days = NULL, # ExpirationDays
    description = 'Data for the Tag Rocket Report'
  );

  # User Sessions

  # Create the user_sessions table if it is missing or out of date
  # If schema is changed then update both references to the version so the table gets re-built
  IF NOT EXISTS(
    SELECT
      1
    FROM
      `${ProjectID}.tag_rocket.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE
      table_name = 'user_sessions'
      AND option_name = 'description'
      AND option_value LIKE "%Version 5.7%" # queryVersion
  ) 
  THEN
    DROP TABLE IF EXISTS `${ProjectID}.tag_rocket.user_sessions`;
    CREATE TABLE `${ProjectID}.tag_rocket.user_sessions` (
      user_pseudo_id STRING,
      ga_session_id INT64,
      session_date_pt DATE,
      last_updated TIMESTAMP,
      session_date DATE,  
      session_start_timestamp TIMESTAMP,
      session_end_timestamp TIMESTAMP,
      user_id STRING,
      session_first_visit INT64,
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
      session_engaged	BOOL,
      customer_group_name	STRING,
      user_ltv_revenue	FLOAT64,
      user_ltv_currency	STRING,
      user_campaign	STRING,
      user_medium	STRING,
      user_source	STRING,
    )
    PARTITION BY session_date_pt
    OPTIONS (description = 'Version 5.7'); # queryVersion
  END IF;

  ALTER TABLE `${ProjectID}.tag_rocket.user_sessions`
  SET OPTIONS (partition_expiration_days = NULL); # ExpirationDays

  # work out the last day we have data for
  SET dateToGather = (SELECT DATE_SUB(MAX(session_date), INTERVAL lookbackDays DAY) FROM `${ProjectID}.tag_rocket.user_sessions`);

  # remove the last few days data so we can replace it
  IF dateToGather IS NOT NULL THEN
    DELETE FROM `${ProjectID}.tag_rocket.user_sessions` WHERE session_date >= dateToGather;
  ELSE
    IF maxDaysToLookBackOnInitialQuery IS NOT NULL THEN
       SET dateToGather = DATE_SUB(CURRENT_DATE(), INTERVAL maxDaysToLookBackOnInitialQuery DAY);
    END IF;
  END IF;

  # add the new data for the last few days
  INSERT `${ProjectID}.tag_rocket.user_sessions` 
  (  
      user_pseudo_id,
      ga_session_id,
      session_date_pt,
      last_updated,
      session_date, 
      session_start_timestamp,
      session_end_timestamp,
      user_id,
      session_first_visit,
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
      session_engaged,
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
    EXTRACT(DATE FROM SAFE.TIMESTAMP_MICROS(MIN(event_timestamp)) AT TIME ZONE 'US/Pacific') AS session_date_pt, 
    CURRENT_TIMESTAMP(),
    MIN(PARSE_DATE('%Y%m%d', event_date)) AS session_date,  
    SAFE.TIMESTAMP_MICROS(MIN(event_timestamp)) AS session_start_timestamp, 
    SAFE.TIMESTAMP_MICROS(MAX(event_timestamp)) AS session_end_timestamp, 
    ANY_VALUE(user_id) AS user_id,
    MAX(IF(event_name = 'first_visit',1,0)) AS session_first_visit,
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
    SUM(IF(event_name = 'purchase',1,0)) AS session_purchase_count,
    SUM(ecommerce.purchase_revenue) AS session_purchase_revenue, # is this only present on the purchase event?
    ANY_VALUE(IF(event_name = 'purchase',(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'currency'), NULL)) AS session_purchase_currency,
    SUM(ecommerce.purchase_revenue_in_usd) AS session_purchase_revenue_in_usd, # is this only present on the purchase event?
    ANY_VALUE(device.category) AS session_device_category,
    ANY_VALUE(device.web_info.browser) AS session_device_browser,
    ANY_VALUE(device.operating_system) AS session_device_operating_system,
    ANY_VALUE(geo.country) AS session_country,
    ANY_VALUE(IF(event_name = 'session_start',(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), NULL)) AS session_landing_page,
    ARRAY_AGG(IF(event_name = 'page_view',(SELECT COALESCE(value.string_value, CAST(value.int_value AS STRING)) FROM UNNEST(event_params) WHERE key = 'page_type'), NULL) IGNORE NULLS ORDER BY event_timestamp LIMIT 1 )[OFFSET(0)] AS session_landing_page_type,
    ANY_VALUE(IF(event_name = 'session_start',(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer'), NULL)) AS session_referrer,
    ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign')) AS session_campaign,
    ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source')) AS session_source,
    ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium')) AS session_medium,
    ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'term')) AS session_term,
    ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'gclid')) AS session_gclid,
    IF(MAX(
      (
        SELECT
          COALESCE(
            value.double_value, value.int_value, CAST(value.string_value AS NUMERIC))
        FROM UNNEST(event_params)
        WHERE key = 'session_engaged'
      )) > 0, true, false) AS session_engaged,
    ANY_VALUE((SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'customer_group_name')) AS customer_group_name,
    MAX(user_ltv.revenue) AS	user_ltv_revenue,
    MAX(user_ltv.currency) AS user_ltv_currency,
    ANY_VALUE(traffic_source.name)	AS user_campaign,
    ANY_VALUE(traffic_source.medium)	AS user_medium,
    ANY_VALUE(traffic_source.source)	AS user_source
  FROM `${ProjectID}.${DatasetID}.events_*`  # this could pick up the intraday table if it exists?
  WHERE event_name IN ('session_start', 'page_view', 'purchase', 'add_to_cart', 'begin_checkout', 'view_cart', 'view_item', 'view_item_list', 'first_visit', 'select_item', 'add_customer_info', 'add_shipping_info', 'add_billing_info')
  AND (dateToGather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',dateToGather) AND FORMAT_DATE('%Y%m%d',CURRENT_DATE()))
  AND user_pseudo_id IS NOT NULL
  GROUP BY 1, 2
  HAVING session_page_view_count > 0; 
  
END;