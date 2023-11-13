WITH source_data AS(
    SELECT
    *,
    ROW_NUMBER() OVER(
        PARTITION BY sample_id
    ) AS row_number
    FROM {{ ref("04_einstein_fill_results") }}
)
SELECT
    *
FROM source_data
WHERE row_number = 1
