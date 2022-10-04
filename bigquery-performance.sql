#  Replace target table name
SELECT
    event_name,

    COUNT(0) as events,

    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'effective_connection_type') AS effective_connection_type,

    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'save_data') AS save_data,

    #(SELECT COALESCE(value.string_value, CAST(value.int_value AS STRING)) FROM UNNEST(event_params) WHERE key = 'page_type') AS page_type,

    (APPROX_QUANTILES((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'fetchStart'), 100)[offset(75)]) AS fetchStart_p75,
    (APPROX_QUANTILES((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'requestStart'), 100)[offset(75)]) AS requestStart_p75,
    (APPROX_QUANTILES((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'responseStart'), 100)[offset(75)]) AS responseStart_p75,
    (APPROX_QUANTILES((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'responseEnd'), 100)[offset(75)]) AS responseEnd_p75,
   
    (APPROX_QUANTILES((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'fp'), 100)[offset(75)]) AS fp_p75,

    (APPROX_QUANTILES((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'fcp'), 100)[offset(75)]) AS fcp_p75,  
    (APPROX_QUANTILES((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'lcp'), 100)[offset(75)]) AS lcp_p75,

    (APPROX_QUANTILES((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'domContentLoadedEventStart'), 100)[offset(75)]) AS domContentLoadedEventStart_p75,
    (APPROX_QUANTILES((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'domContentLoadedEventEnd'), 100)[offset(75)]) AS domContentLoadedEventEnd_p75,
    (APPROX_QUANTILES((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'loadEventStart'), 100)[offset(75)]) AS loadEventStart_p75,
    (APPROX_QUANTILES((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'loadEventEnd'), 100)[offset(75)]) AS loadEventEnd_p75,

    (APPROX_QUANTILES((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'cls'), 100)[offset(75)]) AS cls_p75,
    (APPROX_QUANTILES((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'fid'), 100)[offset(75)]) AS fid_p75,

    (APPROX_QUANTILES((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'transferSize'), 100)[offset(75)]) AS transferSize_p75,
    (MAX((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'transferSize'))) AS transferSize_max,
    #(SELECT string_value FROM UNNEST(event_params) WHERE key = 'type'), 100)[offset(75)]) AS type,
    (APPROX_QUANTILES((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'redirectCount'), 100)[offset(75)]) AS redirectCount_p75,
    (MAX((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'redirectCount'))) AS redirectCount_max,

    (APPROX_QUANTILES((SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(event_params) WHERE key = 'eventTime'), 100)[offset(75)]) AS eventTime_p75,
FROM
    # Replace source table name
    `mylinen.analytics_262072120.events_*`
WHERE
    event_name IN ('performance_metrics_hidden', 'performance_metrics_pagehide')
GROUP BY event_name, effective_connection_type, save_data
ORDER BY event_name, effective_connection_type, save_data