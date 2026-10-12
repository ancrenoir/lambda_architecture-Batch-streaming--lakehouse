WITH agg_silver_transaction_bank AS (
    SELECT
        CAST(s.event_time AS DATE) AS event_date,
        s.country_code,
        s.entity_type,
        s.transaction_type,
        COUNT(DISTINCT s.transaction_id) AS transaction_volume,
        SUM(s.streaming_amount_eur) AS total_amount_eur
    FROM kafka.default."silver-bank-transactions" AS s
    WHERE s.transaction_status = 'SUCCESS'
      AND CAST(s.event_time AS DATE) = CURRENT_DATE
    GROUP BY
        CAST(s.event_time AS DATE),
        s.country_code,
        s.entity_type,
        s.transaction_type
),

gold_daily_transaction_volume AS (
    SELECT
        b.event_date,
        b.country_code,
        b.entity_type,
        b.transaction_type,
        b.transaction_volume,
        b.total_amount_eur,
        b.reporting_currency
    FROM data.gold.daily_transaction_volume AS b
    WHERE b.event_date < CURRENT_DATE
),

lambda_unification_detaillee AS (
    SELECT
        COALESCE(
            gold_batch.event_date,
            silver_streaming.event_date
        ) AS event_date,
        COALESCE(
            gold_batch.country_code,
            silver_streaming.country_code
        ) AS country_code,
        COALESCE(
            gold_batch.entity_type,
            silver_streaming.entity_type
        ) AS entity_type,
        COALESCE(
            gold_batch.transaction_type,
            silver_streaming.transaction_type
        ) AS transaction_type,
        COALESCE(gold_batch.transaction_volume, 0)
            + COALESCE(silver_streaming.transaction_volume, 0)
            AS transaction_volume,
        COALESCE(gold_batch.total_amount_eur, 0)
            + COALESCE(silver_streaming.total_amount_eur, 0)
            AS total_amount_eur,
        COALESCE(
            gold_batch.reporting_currency,
            'EUR'
        ) AS reporting_currency
    FROM gold_daily_transaction_volume AS gold_batch
    FULL OUTER JOIN agg_silver_transaction_bank AS silver_streaming
        ON gold_batch.event_date = silver_streaming.event_date
       AND gold_batch.country_code = silver_streaming.country_code
       AND gold_batch.entity_type = silver_streaming.entity_type
       AND gold_batch.transaction_type = silver_streaming.transaction_type
),

lambda_vue_globale AS (
    SELECT
        event_date,
        country_code,
        SUM(transaction_volume) AS transaction_volume,
        SUM(total_amount_eur) AS total_amount_eur,
        'EUR' AS reporting_currency
    FROM lambda_unification_detaillee
    GROUP BY
        event_date,
        country_code
)

SELECT *
FROM lambda_vue_globale
ORDER BY
    event_date,
    country_code;
