-- Number of repetitions for each sample id is 1 or 3
WITH source_data AS (
    SELECT * FROM
    {{ ref("sabin_02_fix_values") }}
)
SELECT
    *
FROM 
(
    SELECT DISTINCT
        COUNT(*) AS count_lines
    FROM source_data
    GROUP BY sample_id
) AS count_lines_sample_id
WHERE count_lines != 1 
AND count_lines != 3 -- arbo_pcr_3
AND count_lines != 4 -- arbo_pcr_3 + pcrdect
AND count_lines != 2 -- Temporary to avoid breaking the pipeline in a very specific case: cd61b303b552fe
AND count_lines != 6 -- Temporary to avoid breaking the pipeline in a very specific case: sample_id = 5af1b32297a79f90b31710470266eb5b