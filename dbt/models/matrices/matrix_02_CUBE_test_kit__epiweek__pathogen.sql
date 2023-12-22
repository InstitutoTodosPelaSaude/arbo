
WITH source_data AS (
    SELECT
        *
    FROM {{ ref("matrix_01_pivoted") }}
)
SELECT
    test_kit,
    epiweek_enddate,
    pathogen,
    
    -- # Key indicators
    -- Total Number of Pos
    -- Total Number of Neg
    -- Total Number of NT
    -- Total Number of lines
    -- Positivity Rate Pos/Pos+Neg

    SUM(CASE WHEN result = 'Pos' THEN 1 ELSE 0 END) AS "Pos",
    SUM(CASE WHEN result = 'Neg' THEN 1 ELSE 0 END) AS "Neg",
    SUM(CASE WHEN result IN ('Pos', 'Neg') THEN 1 ELSE 0 END) AS "PosNeg",
    SUM(CASE WHEN result = 'NT'  THEN 1 ELSE 0 END) AS "NT",
    COUNT(*) AS total_lines,
    CASE
        -- Avoid division by zero
        WHEN SUM(CASE WHEN result IN ('Pos', 'Neg') THEN 1 ELSE 0 END) > 0 THEN
            SUM(CASE WHEN result = 'Pos' THEN 1 ELSE 0 END)::decimal / SUM(CASE WHEN result IN ('Pos', 'Neg') THEN 1 ELSE 0 END)
        ELSE NULL
    END AS positivity_rate

FROM
    source_data
GROUP BY
    CUBE(test_kit, epiweek_enddate, pathogen)