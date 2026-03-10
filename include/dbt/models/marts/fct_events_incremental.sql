-- models/marts/fct_events_incremental.sql
-- Incremental model that appends/merges only new or updated events.
-- Avoids full table scans on large event datasets.
--
-- Strategy: merge on event_id (upsert pattern)
-- On schema change: append new columns without breaking existing runs

{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        tags=['daily', 'incremental']
    )
}}

WITH staged AS (
    SELECT * FROM {{ ref('stg_events') }}
),

enriched AS (
    SELECT
        event_id,
        user_id,
        event_type,
        event_ts,
        event_date,
        properties,
        _loaded_at,

        -- Derived fields
        CASE
            WHEN event_type = 'purchase'
                THEN (properties->>'amount')::NUMERIC
            ELSE NULL
        END                                     AS purchase_amount,

        CASE
            WHEN event_type IN ('page_view', 'click') THEN 'engagement'
            WHEN event_type = 'purchase'              THEN 'conversion'
            WHEN event_type = 'signup'                THEN 'acquisition'
            ELSE 'other'
        END                                     AS event_category,

        NOW()                                   AS dbt_updated_at
    FROM staged
)

SELECT * FROM enriched

-- Incremental filter: only process new records since last run
{% if is_incremental() %}
    WHERE event_ts > (
        SELECT COALESCE(MAX(event_ts), '1900-01-01'::TIMESTAMP)
        FROM {{ this }}
    )
{% endif %}
