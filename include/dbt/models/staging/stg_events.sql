-- models/staging/stg_events.sql
-- Cleans and standardizes raw event data from the ingestion layer.
-- Materialized as a view (no storage cost, always fresh).

WITH source AS (
    SELECT
        event_id,
        user_id,
        LOWER(TRIM(event_type))         AS event_type,
        event_ts::TIMESTAMP             AS event_ts,
        properties,
        _loaded_at
    FROM {{ source('raw', 'events') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY _loaded_at DESC
        ) AS row_num
    FROM source
)

SELECT
    event_id,
    user_id,
    event_type,
    event_ts,
    properties,
    _loaded_at,
    DATE_TRUNC('day', event_ts)         AS event_date
FROM deduplicated
WHERE row_num = 1
  AND event_id IS NOT NULL
  AND user_id  IS NOT NULL
  AND event_ts IS NOT NULL
