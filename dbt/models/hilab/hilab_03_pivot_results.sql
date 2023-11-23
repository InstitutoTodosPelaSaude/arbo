

{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ ref('hilab_02_fix_values') }}

)
SELECT
    *,
    {{ 
        pivot_pathogen_results(
            [
                'Dengue IgM',
                'Dengue IgG',
                'Dengue NS1'
            ], 
            'exame', 
            'result', 
            'DENV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'Zika IgG',
                'Zika IgM'
            ], 
            'exame', 
            'result', 
            'ZIKV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'exame', 
            'result', 
            'CHIKV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'exame', 
            'result', 
            'YFV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'exame', 
            'result', 
            'MAYV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'exame', 
            'result', 
            'OROV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'exame', 
            'result', 
            'WNV_test_result'
        )
    }}
FROM source_data