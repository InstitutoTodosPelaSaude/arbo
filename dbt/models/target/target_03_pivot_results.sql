{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ ref("target_02_fix_values") }}

)
SELECT
    *,
    {{ 
        pivot_pathogen_results(
            [
                'RES1', 
                'DENV'
            ], 
            'detalhe_exame', 
            'result', 
            'DENV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'ZIKV'
            ], 
            'detalhe_exame', 
            'result', 
            'ZIKV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'CHIKV'
            ], 
            'detalhe_exame', 
            'result', 
            'CHIKV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'detalhe_exame', 
            'result', 
            'YFV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'detalhe_exame', 
            'result', 
            'MAYV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'detalhe_exame', 
            'result', 
            'OROV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'detalhe_exame', 
            'result', 
            'WNV_test_result'
        )
    }}
FROM source_data