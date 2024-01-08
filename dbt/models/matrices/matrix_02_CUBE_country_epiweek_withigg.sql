
WITH source_data AS (
    SELECT
        test_kit,
        epiweek_enddate,
        result,
        pathogen
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE -- FILTER USEFUL TEST KITS FOR EACH PATHOGEN
        CASE 
            WHEN "DENV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('arbo_pcr_3', 'ns1_antigen', 'igg_serum')
            ELSE TRUE
        END
)
SELECT
    test_kit,
    epiweek_enddate,
    pathogen,
    
    -- # Key indicators
    -- Total Number of Pos
    -- Total Number of Neg
    -- Total Number of Tests (Pos + Neg)
    -- Total Number of NT
    -- Positivity Rate (Pos/Pos+Neg)

    SUM(CASE WHEN result = 'Pos' THEN 1 ELSE 0 END) AS "Pos",
    SUM(CASE WHEN result = 'Neg' THEN 1 ELSE 0 END) AS "Neg",
    SUM(CASE WHEN result IN ('Pos', 'Neg') THEN 1 ELSE 0 END) AS "totaltests",
    SUM(CASE WHEN result = 'NT'  THEN 1 ELSE 0 END) AS "NT",
    CASE
        -- Avoid division by zero
        WHEN SUM(CASE WHEN result IN ('Pos', 'Neg') THEN 1 ELSE 0 END) > 0 THEN
            SUM(CASE WHEN result = 'Pos' THEN 1 ELSE 0 END)::decimal / SUM(CASE WHEN result IN ('Pos', 'Neg') THEN 1 ELSE 0 END)
        ELSE NULL
    END AS "posrate"

FROM
    source_data
GROUP BY
    CUBE(test_kit, epiweek_enddate, pathogen)