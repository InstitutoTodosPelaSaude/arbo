{{ config(materialized='table') }}
WITH source_data AS (
    SELECT * FROM
    {{ ref("hpardini_02_fix_values") }}
)
SELECT 
    *,
    {{ 
        pivot_pathogen_results(
            [
                'DENV'
            ], 
            'pathogen', 
            'result', 
            'DENV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'pathogen', 
            'result', 
            'ZIKV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'CHIKV'
            ], 
            'pathogen', 
            'result', 
            'CHIKV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'pathogen', 
            'result', 
            'YFV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'pathogen', 
            'result', 
            'MAYV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'pathogen', 
            'result', 
            'OROV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'pathogen', 
            'result', 
            'WNV_test_result'
        )
    }}

FROM source_data