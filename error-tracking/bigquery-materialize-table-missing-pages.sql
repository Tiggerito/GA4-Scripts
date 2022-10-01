# Missing Page Materialised Table v3.1
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
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_type') AS page_type,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') AS page_referrer,
  FROM `DatasetID.events_*` # Replace DatasetID with your Dataset ID
  WHERE event_name = 'page_view'
