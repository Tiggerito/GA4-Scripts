# Website Errors Materialised Table Incremental v1.0
# https://github.com/Tiggerito/GA4-Scripts/blob/main/error-tracking/bigquery-materialize-table-website-errors.sql

# Replace all occurances of DatasetID with your Dataset ID

BEGIN
  # The first run with gather all data. After that it will gather new data and merge the last 2 (or 3?) days of data

  DECLARE datetogather DEFAULT CURRENT_TIMESTAMP();

  # Website Errors 

  IF NOT EXISTS(SELECT 1 FROM `DatasetID.__TABLES_SUMMARY__`
    WHERE table_id = 'website_errors_incremental') THEN
    CREATE TABLE `DatasetID.website_errors_incremental` (
      last_updated TIMESTAMP,
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
    PARTITION BY TIMESTAMP_TRUNC(event_timestamp, DAY); 
  END IF;

  # 60 days
  # first run <12GB
  # second run 680MB - yay
  # original <12GB

  # 10MB min per query makes this look expensive for small tables.
  SET datetogather = (SELECT TIMESTAMP_TRUNC(TIMESTAMP_ADD(MAX(event_date), INTERVAL -2 DAY), DAY) FROM `DatasetID.website_errors_incremental`);

  # and this seems to be more expensive than replace
  MERGE INTO `DatasetID.website_errors_incremental` A 
  USING (
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
    FROM `DatasetID.events_*` 
    WHERE event_name = 'exception'
    #AND (datetogather IS NULL OR TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), DAY) > datetogather)
    AND (datetogather IS NULL OR _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',datetogather) AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
  ) B
  ON 
    A.event_timestamp = B.event_timestamp
    AND A.error_message = B.error_message
    AND A.page_location = B.page_location
    AND (datetogather IS NULL OR TIMESTAMP_TRUNC(B.event_timestamp, DAY) > datetogather)
  #WHEN MATCHED THEN UPDATE SET  # what we gather should never change. 
  #  A.last_updated = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (
    last_updated,
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



END;