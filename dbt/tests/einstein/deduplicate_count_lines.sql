WITH source_data AS (
    SELECT * FROM
    {{ref("einstein_04_fill_results")}}
),
source_deduplicated AS (
    SELECT * FROM 
    {{ref("einstein_05_deduplicate")}}
),
diff_qty_lines_result AS (
    SELECT
        SUM(qty_original_lines) - (SELECT COUNT(*) FROM source_data) AS diff_qty_lines
    FROM source_deduplicated 
)
SELECT 
    * 
FROM diff_qty_lines_result
WHERE diff_qty_lines > 0