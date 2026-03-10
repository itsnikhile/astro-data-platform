-- models/marts/fct_daily_event_summary.sql
-- Aggregates event counts per user per day.
-- Consumed by BI tools and reporting layers.

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

daily_summary AS (
    SELECT
        event_date,
        user_id,
        COUNT(*)                                                        AS total_events,
        COUNT(*) FILTER (WHERE event_type = 'page_view')               AS page_views,
        COUNT(*) FILTER (WHERE event_type = 'click')                   AS clicks,
        COUNT(*) FILTER (WHERE event_type = 'purchase')                AS purchases,
        COUNT(*) FILTER (WHERE event_type = 'signup')                  AS signups,
        SUM(CASE WHEN event_type = 'purchase'
                THEN (properties->>'amount')::NUMERIC ELSE 0 END)      AS total_revenue,
        MIN(event_ts)                                                   AS first_event_ts,
        MAX(event_ts)                                                   AS last_event_ts
    FROM events
    GROUP BY 1, 2
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['event_date', 'user_id']) }}  AS summary_id,
    event_date,
    user_id,
    total_events,
    page_views,
    clicks,
    purchases,
    signups,
    total_revenue,
    first_event_ts,
    last_event_ts,
    NOW()                                                               AS dbt_updated_at
FROM daily_summary
