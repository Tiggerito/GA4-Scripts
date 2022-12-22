#  Replace target table name
SELECT bucket*100 as TTFB_from_ms, (bucket+1)*100 as TTFB_to_ms, COUNT(event_name) as page_views
FROM (
    SELECT
        event_name,
        (SELECT TRUNC(value.double_value/100) FROM UNNEST(event_params) WHERE key = 'metric_value') AS bucket,
    FROM
        # Replace source table name
        `your_project.analytics_123456789.events_*`
    WHERE
        event_name IN ('TTFB')
         
)
WHERE bucket IS NOT NULL
GROUP BY bucket
ORDER BY bucket