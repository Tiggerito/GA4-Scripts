# Tag Rocket Report Demo Dataset v2.0
# https://github.com/Tiggerito/GA4-Scripts/blob/main/bigquery-tag-rocket-report-demo-dataset.sql

# can only pull from datasets using the US multi region location

BEGIN

DECLARE timestampToGather DEFAULT TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 35 DAY);
DECLARE dateToGather DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY);

# meta data
DROP TABLE IF EXISTS `web-site-advantage-ga4.tag_rocket_demo.meta_data`;
CREATE TABLE `web-site-advantage-ga4.tag_rocket_demo.meta_data` 
  OPTIONS (description = 'Version 5.0') # queryVersion
  AS 
SELECT
      'daily' AS schedule_frequency ,
      'Tag Rocket' AS scheduled_by	,
      'demo.com' AS store_front_name ,
      'https://demo.com/' AS store_front_url ,
      'Welcome' AS notification1_title ,
      'The Tag Rocket report is available to subscribers of the Tag Rocket app for BigCommerce websites.' AS notification1_content ,
      'normal' AS notification1_type , 
      'Demo' AS notification2_title ,
      'This version of the report uses demo data.' AS notification2_content ,
      'normal' AS notification2_type ,
      '' AS notification3_title ,
      '' AS notification3_content ,
      'normal' AS notification3_type ,
      last_exported,			     
      partition_expiration ,	
      'web-site-advantage-ga4' AS bigquery_project_id ,	
      '' AS ga4_account_id ,	
      '' AS ga4_property_id ,		
      query_version	,			
      last_run_timestamp	
FROM `macs4u-tag-rocket.tag_rocket_313969512.meta_data`;

# Web Vitals
DROP TABLE IF EXISTS `web-site-advantage-ga4.tag_rocket_demo.web_vitals`;
CREATE TABLE `web-site-advantage-ga4.tag_rocket_demo.web_vitals` 
  PARTITION BY event_date
  CLUSTER BY metric_name
  OPTIONS (description = 'Version 5.0')  # queryVersion
AS
SELECT
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
      IF(STARTS_WITH(traffic_name,'('), traffic_name, CONCAT('Campaign ', MOD(ga_session_id, 9)+1)) AS traffic_name,
      CASE traffic_source
        WHEN 'google' THEN 'google'
        WHEN 'bing' THEN 'bing'
        WHEN '(direct)' THEN '(direct)'
        WHEN 'yahoo' THEN 'yahoo'
        ELSE CONCAT('Source ', MOD(ga_session_id, 9)+1)
      END AS traffic_source,
      CASE traffic_medium
        WHEN 'organic' THEN 'organic'
        WHEN 'cpc' THEN 'cpc'
        WHEN 'referral' THEN 'referral'
        ELSE CONCAT('Medium ', MOD(ga_session_id, 9)+1)
      END AS traffic_medium,				
    REPLACE(CONCAT('https://demo.com/',REPLACE(LEFT(LOWER(TO_BASE64(SHA256(page_path))), 30), '/', 'b')), '+', 'a') AS page_path,			
    REGEXP_REPLACE(REGEXP_REPLACE(debug_target, r'\.[^>.]*', '.class'), r'#[^>.]*', '#id') AS debug_target,
    event_timestamp,	
    event_date,			
    metric_id,			
    metric_value,	
    user_pseudo_id,					
    session_revenue,			
    metric_rating,	
    REPLACE(CONCAT('https://demo.com/',REPLACE(LEFT(LOWER(TO_BASE64(SHA256(page_location))), 30), '/', 'b')), '+', 'a') AS page_location,							
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
FROM `macs4u-tag-rocket.tag_rocket_313969512.web_vitals`
WHERE event_date > dateToGather;

# Purchases
DROP TABLE IF EXISTS `web-site-advantage-ga4.tag_rocket_demo.purchases`;
CREATE TABLE `web-site-advantage-ga4.tag_rocket_demo.purchases` 
  PARTITION BY event_date
  OPTIONS (description = 'Version 5.1')  # queryVersion
AS 
SELECT
      last_updated,
      user_pseudo_id,
      IF(user_id IS NULL, NULL, LEFT(LOWER(TO_BASE64(SHA256(user_id))), 10)) AS user_id,
      CONCAT('12', transaction_id) AS transaction_id,
      event_timestamp,
      event_date,
      purchase_event_timestamp,
      purchase_revenue * 2.34 AS purchase_revenue,
      'AUD' AS purchase_currency,
      purchase_shipping_value * 2.34 AS purchase_shipping_value,
      purchase_tax_value * 2.34 AS purchase_tax_value,
      purchase_refund_value * 2.34 AS purchase_refund_value,
      purchase_revenue_in_usd * 5 AS purchase_revenue_in_usd,
      purchase_shipping_value_in_usd * 5 AS purchase_shipping_value_in_usd,
      purchase_tax_value_in_usd * 5 AS purchase_tax_value_in_usd,
      purchase_refund_value_in_usd * 5 AS purchase_refund_value_in_usd,
      purchase_events,
      server_purchase_event_timestamp,
      server_purchase_revenue * 2.34 AS server_purchase_revenue,
      'AUD' AS server_purchase_currency,
      server_purchase_method,
      server_purchase_events,
      device_browser,
      device_browser_version,
      device_category,
      device_operating_system,
      device_operating_system_version,
      IF(STARTS_WITH(traffic_name,'('), traffic_name, CONCAT('Campaign ', MOD(CAST(transaction_id AS INT64), 9)+1)) AS traffic_name,
      CASE traffic_source
        WHEN 'google' THEN 'google'
        WHEN 'bing' THEN 'bing'
        WHEN '(direct)' THEN '(direct)'
        WHEN 'yahoo' THEN 'yahoo'
        ELSE CONCAT('Source ', MOD(CAST(transaction_id AS INT64), 9)+1)
      END AS traffic_source,
      CASE traffic_medium
        WHEN 'organic' THEN 'organic'
        WHEN 'cpc' THEN 'cpc'
        WHEN 'referral' THEN 'referral'
        ELSE CONCAT('Medium ', MOD(CAST(transaction_id AS INT64), 9)+1)
      END AS traffic_medium,
      user_ltv_revenue * 5 AS user_ltv_revenue,
      user_ltv_currency 
FROM `macs4u-tag-rocket.tag_rocket_313969512.purchases`
WHERE event_date > dateToGather;

# Website Errors
DROP TABLE IF EXISTS `web-site-advantage-ga4.tag_rocket_demo.website_errors`;
CREATE TABLE `web-site-advantage-ga4.tag_rocket_demo.website_errors` 
    PARTITION BY event_date
    OPTIONS (description = 'Version 5.0')  # queryVersion
AS 
SELECT
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
    REPLACE(CONCAT('https://demo.com/',REPLACE(LEFT(LOWER(TO_BASE64(SHA256(page_location))), 30), '/', 'b')), '+', 'a') AS page_location,		
    page_type,
    error_message,
    error_type,
    REPLACE(CONCAT('https://demo.com/',REPLACE(LEFT(LOWER(TO_BASE64(SHA256(error_filename))), 30), '/', 'b')), '+', 'a') AS error_filename,
    error_lineno,
    error_colno,
    error_object_type
FROM `macs4u-tag-rocket.tag_rocket_313969512.website_errors`
WHERE event_date > dateToGather;

# Missing Pages
DROP TABLE IF EXISTS `web-site-advantage-ga4.tag_rocket_demo.missing_pages`;
CREATE TABLE `web-site-advantage-ga4.tag_rocket_demo.missing_pages`
    PARTITION BY event_date
    OPTIONS (description = 'Version 5.0') # queryVersion
AS 
SELECT
    last_updated,
    user_pseudo_id,
    call_timestamp,
    call_sequence,
    event_timestamp,
    event_date,
    IF (source IS NULL, NULL, CONCAT('source ', LENGTH(source)))  AS source,
    IF (medium IS NULL, NULL, CONCAT('medium ', LENGTH(medium)))  AS medium,
    IF (campaign IS NULL, NULL, CONCAT('campaign ', LENGTH(campaign)))  AS campaign,
    REPLACE(CONCAT('https://demo.com/',REPLACE(LEFT(LOWER(TO_BASE64(SHA256(page_location))), 30), '/', 'b')), '+', 'a') AS page_location,	
    page_type,
    REPLACE(CONCAT('https://referrer.com/',REPLACE(LEFT(LOWER(TO_BASE64(SHA256(page_referrer))), 30), '/', 'b')), '+', 'a') AS page_referrer
FROM `macs4u-tag-rocket.tag_rocket_313969512.missing_pages`
WHERE event_date > dateToGather;

# User Sessions
DROP TABLE IF EXISTS `web-site-advantage-ga4.tag_rocket_demo.user_sessions`;
CREATE TABLE `web-site-advantage-ga4.tag_rocket_demo.user_sessions`
    PARTITION BY session_date
    OPTIONS (description = 'Version 5.1') # queryVersion
AS 
SELECT
      user_pseudo_id,
      ga_session_id,
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

      session_purchase_revenue * 2.34 AS session_purchase_revenue,
      session_purchase_currency,
      session_purchase_revenue_in_usd * 5 AS session_purchase_revenue_in_usd,
      session_device_category,
      session_device_browser,
      session_device_operating_system,
      session_country,
      REPLACE(CONCAT('https://demo.com/',REPLACE(LEFT(LOWER(TO_BASE64(SHA256(session_landing_page))), 30), '/', 'b')), '+', 'a') AS session_landing_page,	
      session_landing_page_type,
      REPLACE(CONCAT('https://',REPLACE(LEFT(LOWER(TO_BASE64(SHA256(session_referrer))), 30), '/', 'b'),'.com'), '+', 'a') AS session_referrer,

      IF(STARTS_WITH(session_campaign,'('), session_campaign, CONCAT('Campaign ', MOD(ga_session_id, 9)+1)) AS session_campaign,
      CASE session_source
        WHEN 'google' THEN 'google'
        WHEN 'bing' THEN 'bing'
        WHEN '(direct)' THEN '(direct)'
        WHEN 'yahoo' THEN 'yahoo'
        ELSE CONCAT('Source ', MOD(ga_session_id, 9)+1)
      END AS session_source,
      CASE session_medium
        WHEN 'organic' THEN 'organic'
        WHEN 'cpc' THEN 'cpc'
        WHEN 'referral' THEN 'referral'
        ELSE CONCAT('Medium ', MOD(ga_session_id, 9)+1)
      END AS session_medium,
      IF(session_term IS NULL, NULL, 'xxxxxx') AS session_term,
      IF(session_gclid IS NULL, NULL, 'xxxxxx') AS session_gclid,
      IF(customer_group_name IS NULL, NULL, 'xxxxxx') AS customer_group_name,
      user_ltv_revenue * 5 AS user_ltv_revenue,
      user_ltv_currency,
      IF(STARTS_WITH(user_campaign,'('), user_campaign, CONCAT('Campaign ', MOD(ga_session_id, 9)+1)) AS user_campaign,
      CASE user_source
        WHEN 'google' THEN 'google'
        WHEN 'bing' THEN 'bing'
        WHEN '(direct)' THEN '(direct)'
        WHEN 'yahoo' THEN 'yahoo'
        ELSE CONCAT('Source ', MOD(ga_session_id, 9)+1)
      END AS user_source,
      CASE user_medium
        WHEN 'organic' THEN 'organic'
        WHEN 'cpc' THEN 'cpc'
        WHEN 'referral' THEN 'referral'
        ELSE CONCAT('Medium ', MOD(ga_session_id, 9)+1) 
      END AS user_medium
FROM `macs4u-tag-rocket.tag_rocket_313969512.user_sessions`
WHERE session_date > dateToGather;

# Users
DROP TABLE IF EXISTS `web-site-advantage-ga4.tag_rocket_demo.users`;
CREATE TABLE `web-site-advantage-ga4.tag_rocket_demo.users`
    PARTITION BY first_visit_day
    OPTIONS (description = 'Version 5.0') # queryVersion
AS 
SELECT
      user_pseudo_id,
      last_updated,
      first_visit_timestamp,
      first_visit_day,
      REPLACE(CONCAT('https://demo.com/',REPLACE(LEFT(LOWER(TO_BASE64(SHA256(first_landing_page))), 30), '/', 'b')), '+', 'a') AS first_landing_page,	
      first_landing_page_type,
      REPLACE(CONCAT('https://',REPLACE(LEFT(LOWER(TO_BASE64(SHA256(first_referrer))), 30), '/', 'b'),'.com'), '+', 'a') AS first_referrer,
      customer,
      last_active,
      first_purchase_ga_session_id,
      user_ltv_revenue * 5 AS user_ltv_revenue,
      user_ltv_currency,
      IF(STARTS_WITH(user_campaign,'('), user_campaign, CONCAT('Campaign ', MOD(UNIX_SECONDS(first_visit_timestamp), 9)+1)) AS user_campaign,
      CASE user_source
        WHEN 'google' THEN 'google'
        WHEN 'bing' THEN 'bing'
        WHEN '(direct)' THEN '(direct)'
        WHEN 'yahoo' THEN 'yahoo'
        ELSE CONCAT('Source ', MOD(UNIX_SECONDS(first_visit_timestamp), 9)+1)
      END AS user_source,
      CASE user_medium
        WHEN 'organic' THEN 'organic'
        WHEN 'cpc' THEN 'cpc'
        WHEN 'referral' THEN 'referral'
        ELSE CONCAT('Medium ', MOD(UNIX_SECONDS(first_visit_timestamp), 9)+1)
      END AS user_medium

FROM `macs4u-tag-rocket.tag_rocket_313969512.users`;
#WHERE session_start_timestamp > datetogather;

# Billed Queries Log
DROP TABLE IF EXISTS `web-site-advantage-ga4.tag_rocket_demo.query_logs`;
CREATE TABLE `web-site-advantage-ga4.tag_rocket_demo.query_logs` 
    PARTITION BY DATE(day_timestamp)
    OPTIONS (description = 'Version 5.0') # queryVersion
AS 
SELECT
      day_timestamp,
      'tag-rocket@web-site-advantage.iam.gserviceaccount.com' AS principal_email,
      billed_bytes,
      billed_query_count,
      error_count,
      budget_trendline_bytes,
      rolling_total_bytes,
      month_to_date_bytes
FROM `macs4u-tag-rocket.tag_rocket_313969512.query_logs`
WHERE day_timestamp > timestampToGather;

DROP TABLE IF EXISTS `web-site-advantage-ga4.tag_rocket_demo.project_size`;
CREATE TABLE `web-site-advantage-ga4.tag_rocket_demo.project_size` 
OPTIONS (description = 'Version 5.0') # queryVersion
AS 
SELECT
      date,
      REGEXP_REPLACE(REGEXP_REPLACE(dataset, r'^[^:]*', 'demo-tag-rocket'), r'[0-9]+', '1234567890') AS dataset,
      size_bytes
FROM `macs4u-tag-rocket.tag_rocket_313969512.project_size`;

END;