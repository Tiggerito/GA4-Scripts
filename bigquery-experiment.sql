# user form user_session

  INSERT `${ProjectID}.tag_rocket.users` 
  (  
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
  )
  SELECT    
    user_pseudo_id,
    CURRENT_TIMESTAMP(),
    MIN(session_start_timestamp) AS first_visit_timestamp, # only set on creation
    FORMAT_TIMESTAMP("%Y%m%d",SAFE.TIMESTAMP_MICROS(MIN(session_start_timestamp))) AS first_visit_day,  
    ARRAY_AGG(session_landing_page IGNORE NULLS ORDER BY session_start_timestamp LIMIT 1 )[OFFSET(0)] AS first_landing_page, # only set on creation
    ARRAY_AGG(session_landing_page_type IGNORE NULLS ORDER BY session_start_timestamp LIMIT 1 )[OFFSET(0)] AS first_landing_page_type, # only set on creation
    ARRAY_AGG(session_referrer IGNORE NULLS ORDER BY session_start_timestamp LIMIT 1 )[OFFSET(0)] AS first_referrer, # only set on creation
    ANY_VALUE(user_campaign) AS user_campaign,# only set on creation
    ANY_VALUE(user_medium) AS user_medium,# only set on creation
    ANY_VALUE(user_source) AS user_source,# only set on creation
  FROM `${ProjectID}.tag_rocket.users_sessions` 
  WHERE (datetogather IS NULL OR first_visit_day BETWEEN FORMAT_DATE('%Y%m%d',datetogather) AND FORMAT_DATE('%Y%m%d',CURRENT_DATE()))
  GROUP BY 1; 

SELECT 
user_pseudo_id AS user_pseudo_id, # only set on creation
MIN(session_start_timestamp) AS first_visit_timestamp, # only set on creation
ARRAY_AGG(session_landing_page IGNORE NULLS ORDER BY session_start_timestamp LIMIT 1 )[OFFSET(0)] AS first_landing_page, # only set on creation
ARRAY_AGG(session_landing_page_type IGNORE NULLS ORDER BY session_start_timestamp LIMIT 1 )[OFFSET(0)] AS first_landing_page_type, # only set on creation
ARRAY_AGG(session_referrer IGNORE NULLS ORDER BY session_start_timestamp LIMIT 1 )[OFFSET(0)] AS first_referrer, # only set on creation
ANY_VALUE(user_campaign) AS user_campaign,# only set on creation
ANY_VALUE(user_medium) AS user_medium,# only set on creation
ANY_VALUE(user_source) AS user_source,# only set on creation
FROM `web-site-advantage-ga4.tag_rocket_demo.user_sessions`
GROUP BY 1
ORDER BY user_pseudo_id #, session_start_timestamp

# user_pseudo_id

SELECT 
user_pseudo_id,
user_id,
FIRST_VALUE(session_landing_page) OVER (PARTITION BY user_pseudo_id ORDER BY session_start_timestamp) AS first_landing_page,
user_campaign AS user_campaign,
FIRST_VALUE(session_campaign) OVER (PARTITION BY user_pseudo_id ORDER BY session_start_timestamp) AS first_campaign,
FROM `mylinen.tag_rocket.user_sessions`
GROUP BY 1

SELECT 
user_pseudo_id,

event_timestamp AS first_visit_timestamp,

user_first_touch_timestamp AS user_first_touch_timestamp,

(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS first_landing_page,
# user_landing_page_type ???? from first page_view
(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') AS first_referrer,

traffic_source.name AS traffic_source_name,
traffic_source.medium AS traffic_source_medium,
traffic_source.source AS traffic_source_source,

# prob 0 for first_visit
user_ltv.revenue AS	user_ltv_revenue,
user_ltv.currency AS	currency,

FROM `web-site-advantage-ga4.analytics_327863596.events_*` # Replace DatasetID with your Dataset ID
WHERE event_name IN ('first_visit')
#AND _table_suffix BETWEEN '20221201' AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND user_pseudo_id IS NOT NULL
#GROUP BY 1
ORDER BY first_visit_timestamp;

# update users
#ANY_VALUE(user_ltv.revenue) AS	user_ltv_revenue,
#ANY_VALUE(user_ltv.currency) AS user_ltv_currency,

# sessions 2.0
SELECT 
user_pseudo_id,
(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id, # not unique, but is per user_pseudo_id
FORMAT_TIMESTAMP("%Y%m%d",TIMESTAMP_TRUNC(MIN(event_timestamp), DAY)) AS session_date, 
MIN(event_timestamp) AS session_start_timestamp, 
MAX(event_timestamp) AS session_end_timestamp, 
# session length
ANY_VALUE(user_id) AS user_id,

# first time purchase if session_first_visit = 1 and session_purchase_count > 0
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
SUM(IF(event_name = 'add_billing_info',1,0)) AS session_add_payment_info_count,
#SUM(IF(event_name = 'add_payment_info',1,0)) AS session_add_payment_info_count, # same as purchase
SUM(IF(event_name = 'purchase',1,0)) AS session_purchase_count,
#SUM(IF(event_name = 'search',1,0)) AS session_search_count,

SUM(ecommerce.purchase_revenue) AS session_purchase_revenue,

ANY_VALUE(IF(event_name = 'purchase',(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'currency'), NULL)) AS session_purchase_currency,

ANY_VALUE(device.category) AS session_device_category,
ANY_VALUE(device.web_info.browser) AS session_device_browser,
ANY_VALUE(device.operating_system) AS session_device_operating_system,

ANY_VALUE(geo.country) AS session_country,

# channel grouping (user/session) ????

ANY_VALUE(IF(event_name = 'session_start',(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), NULL)) AS session_landing_page,
ANY_VALUE(IF(event_name = 'session_start',(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer'), NULL)) AS session_referrer,

ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign')) AS session_campaign,
ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source')) AS session_source,
ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium')) AS session_medium,
ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'term')) AS session_term,
ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'gclid')) AS session_gclid,


ANY_VALUE((SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'customer_group_name')) AS customer_group_name,


FROM `web-site-advantage-ga4.analytics_327863596.events_*` # Replace DatasetID with your Dataset ID
WHERE event_name IN ('session_start', 'page_view', 'purchase', 'add_to_cart', 'begin_checkout', 'view_cart', 'view_item', 'view_item_list', 'first_visit', 'select_item', 'add_customer_info', 'add_shipping_info', 'add_billing_info')
AND _table_suffix BETWEEN '20221201' AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND user_pseudo_id IS NOT NULL
GROUP BY 1, 2
ORDER BY session_purchase_count DESC, user_pseudo_id, ga_session_id;

# user_pseudo_id <-> user_id

SELECT 
user_pseudo_id,

user_id,

FROM `web-site-advantage-ga4.analytics_327863596.events_*` # Replace DatasetID with your Dataset ID
WHERE event_name IN ('page_view')
#AND _table_suffix BETWEEN '20221201' AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND user_id IS NOT NULL
AND user_pseudo_id IS NOT NULL
GROUP BY 1, 2;


# stuff based on sessions
SELECT 
user_pseudo_id,
(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id, # not unique, but is per user_pseudo_id
FORMAT_TIMESTAMP("%Y%m%d",TIMESTAMP_TRUNC(MIN(event_timestamp), DAY)) AS session_date, 
MIN(event_timestamp) AS session_start_timestamp, 
MAX(event_timestamp) AS session_end_timestamp, 
# session length
ANY_VALUE(user_id) AS user_id,

# first time purchase if session_first_visit = 1 and session_purchase_count > 0
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
SUM(IF(event_name = 'add_billing_info',1,0)) AS session_add_payment_info_count,
#SUM(IF(event_name = 'add_payment_info',1,0)) AS session_add_payment_info_count, # same as purchase
SUM(IF(event_name = 'purchase',1,0)) AS session_purchase_count,
#SUM(IF(event_name = 'search',1,0)) AS session_search_count,

SUM(ecommerce.purchase_revenue) AS session_purchase_revenue,

ANY_VALUE(IF(event_name = 'purchase',(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'currency'), NULL)) AS session_purchase_currency,

ANY_VALUE(device.category) AS session_device_category,
ANY_VALUE(device.web_info.browser) AS session_device_browser,
ANY_VALUE(device.operating_system) AS session_device_operating_system,

ANY_VALUE(geo.country) AS session_country,

# channel grouping (user/session) ????

ANY_VALUE(IF(event_name = 'session_start',(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), NULL)) AS session_landing_page,
ANY_VALUE(IF(event_name = 'session_start',(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer'), NULL)) AS session_referrer,

ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign')) AS session_campaign,
ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source')) AS session_source,
ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium')) AS session_medium,
ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'term')) AS session_term,
ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'gclid')) AS session_gclid,


ANY_VALUE((SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'customer_group_name')) AS customer_group_name,
ANY_VALUE(user_ltv.revenue) AS	user_ltv_revenue,
ANY_VALUE(user_ltv.currency) AS user_ltv_currency,

ANY_VALUE(traffic_source.name)	AS user_campaign,
ANY_VALUE(traffic_source.medium)	AS user_medium,
ANY_VALUE(traffic_source.source)	AS user_source,

ANY_VALUE((SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'referrer')) AS user_referrer,
ANY_VALUE((SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'landing_page')) AS user_landing_page,
ANY_VALUE((SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'landing_page_type')) AS user_landing_page_type,
SAFE.TIMESTAMP(ANY_VALUE((SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'first_datetime'))) AS user_first_timestamp,

FROM `mylinen.analytics_262072120.events_*` # Replace DatasetID with your Dataset ID
WHERE event_name IN ('session_start', 'page_view', 'purchase', 'add_to_cart', 'begin_checkout', 'view_cart', 'view_item', 'view_item_list', 'first_visit', 'select_item', 'add_customer_info', 'add_shipping_info', 'add_billing_info')
AND _table_suffix BETWEEN '20221201' AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND user_pseudo_id IS NOT NULL
GROUP BY 1, 2
ORDER BY session_purchase_count DESC, user_pseudo_id, ga_session_id;

# purchase attribution
SELECT 
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), DAY) AS event_date,

    user_id,
    user_pseudo_id, # can be null in consent mode
    user_first_touch_timestamp,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'customer_group_name') AS customer_group_name,
    user_ltv.revenue AS	user_ltv_revenue,
    user_ltv.currency AS user_ltv_currency,
    traffic_source.name	AS traffic_source_name,

    # users first attribution
    traffic_source.medium	AS traffic_source_medium,
    traffic_source.source	AS traffic_source_source,

    ecommerce.total_item_quantity AS ecommerce_total_item_quantity,	
    ecommerce.transaction_id AS ecommerce_transaction_id,	

    (SELECT COALESCE(value.double_value, CAST(value.int_value AS DOUBLE)) WHERE key = 'value') AS value,
    (SELECT COALESCE(value.double_value, CAST(value.int_value AS DOUBLE)) FROM UNNEST(event_params) WHERE key = 'shipping') AS shipping,
    (SELECT COALESCE(value.double_value, CAST(value.int_value AS DOUBLE)) FROM UNNEST(event_params) WHERE key = 'tax') AS tax,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'shipping_tier') AS shipping_tier,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transaction_id') AS transaction_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'currency') AS currency,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'payment_type') AS payment_type

    # device.web_info.browser AS device_browser,
    # device.web_info.browser_version AS device_browser_version,
    # device.category AS device_category,
    # device.mobile_marketing_name AS device_mobile_marketing_name,
    # device.mobile_brand_name AS device_mobile_brand_name,
    # device.mobile_model_name AS device_mobile_model_name,
    # device.operating_system AS device_operating_system,
    # device.operating_system_version AS device_operating_system_version,
FROM `mylinen.analytics_262072120.events_*` # Replace DatasetID with your Dataset ID
WHERE event_name = 'purchase'
AND _table_suffix BETWEEN '20221001' AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

# field explanations... https://support.google.com/firebase/answer/7029846

    # event_timestamp, event_name, event_bundle, user_id (null), user_pseudo_id, user_first_touch_timestamp, user_properties..., user_ltv.., device..., geo..., traffic_source..., ecommerce..., items...
    # https://ken-williams.com/guide/reporting-analysis/understanding-the-user-id-in-google-analytics-4/
    # when user id is set it is applied to all previous user_pseudo_ids in that session, but not future ones.
    # if possible group user_pseudo_ids by user_id.
    # consent restrictions can mean user_pseudo_id can be null 
    # ga_session_number = 1 is new user
    #   session_start has page_location, page_referrer, page_title, ga_session_id, ga_session_number
    #   page_view has gclid, ga_session_id, source, medium, ga_session_number
    #   purchase???


# lots of examples in https://www.ga4bigquery.com/user-dimensions-metrics-ga4/
    select
    -- user_id (dimension | the user id set via the setUserId api)
    user_id,
    -- user_pseudo_id (dimension | the pseudonymous id (e.g., app instance id) for the user)
    user_pseudo_id,
    -- user_first_touch_timestamp (dimension | the time (in microseconds) at which the user first opened the app/website)
    timestamp_micros(user_first_touch_timestamp) as user_first_touch_timestamp,
    -- user_properties.key (dimension | the name of the user property | replace <insert key> with a parameter key or delete where clause to select all)
    (select key from unnest(user_properties) where key = '<insert key>') as user_properties_key,
    -- user_properties.value.string_value (dimension | the string value of the user property | replace <insert key> with a parameter key or delete where clause to select all)
    (select value.string_value from unnest(user_properties) where key = '<insert key>') as user_string_value,
    -- user_properties.value.int_value (metric | the integer value of the user property | replace <insert key> with a parameter key or delete where clause to select all)
    (select value.int_value from unnest(user_properties) where key = '<insert key>') as user_int_value,
    -- user_properties.value.float_value (metric | the float value of the user property | replace <insert key> with a parameter key or delete where clause to select all)
    (select value.float_value from unnest(user_properties) where key = '<insert key>') as user_float_value,
    -- user_properties.value.double_value (metric | the double value of the user property | replace <insert key> with a parameter key or delete where clause to select all)
    (select value.double_value from unnest(user_properties) where key = '<insert key>') as user_double_value,
    -- user_properties.value.set_timestamp_micros (dimension | the time (in microseconds) at which the user property was last set | replace <insert key> with a parameter key or delete where clause to select all)
    timestamp_micros((select value.set_timestamp_micros from unnest(user_properties) where key = '<insert key>')) as user_set_timestamp_micros,
    -- user_ltv.revenue (metric | the lifetime value (revenue) of the user)
    user_ltv.revenue as user_ltv_revenue,
    -- user_ltv.currency (dimension | the lifetime value (currency) of the user)
    user_ltv.currency as user_ltv_currency
from
    -- change this to your google analytics 4 export location in bigquery
    `mylinen.analytics_262072120.events_*`
where
    -- define static and/or dynamic start and end date
    _table_suffix between '20221001'
    and format_date('%Y%m%d',date_sub(current_date(), interval 1 day));