-- tests/assert_no_negative_revenue.sql
-- Fails if any purchase event has a negative amount.
-- This is a data quality guardrail on the incremental model.

SELECT
    event_id,
    purchase_amount
FROM {{ ref('fct_events_incremental') }}
WHERE event_type = 'purchase'
  AND purchase_amount < 0
