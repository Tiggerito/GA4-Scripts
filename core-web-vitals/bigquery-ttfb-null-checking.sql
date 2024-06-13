SELECT browser, category, operating_system_version, COUNT(event_name) as page_views
FROM (
    SELECT
        event_name,
        (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'metric_value') AS event_value,
        device.browser AS browser,
        device.category AS category,
        device.operating_system_version AS operating_system_version,
    FROM
        # Replace source table name
        `${ProjectID}.${DatasetID}.events_*`
    WHERE
        event_name IN ('TTFB')
                
)
WHERE event_value IS NULL
GROUP BY browser, category, operating_system_version
ORDER BY page_views DESC