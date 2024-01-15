WITH source_data AS (
    SELECT * FROM
    {{ ref("einstein_03_pivot_results") }}
)
SELECT
    1 AS _
FROM source_data
WHERE
    1=1
	AND denv_test_result=-1
    AND zikv_test_result=-1
    AND chikv_test_result=-1
    AND yfv_test_result=-1
    AND mayv_test_result=-1
    AND orov_test_result=-1
    AND wnv_test_result=-1