{{ config(materialized='table') }}

SELECT
	lab_id,
	test_kit,
	pathogen, 
	"result",
	count(*)
FROM {{ ref("matrix_01_pivoted") }} 
WHERE "result" <> 'NT'
GROUP BY lab_id, test_kit, pathogen, "result"
ORDER BY lab_id, test_kit, pathogen, "result"