{{ config(materialized='table') }}




WITH source_data AS (

    SELECT 
    *,
    {{ 
        pivot_pathogen_results(
            [
                'VÍRUS DENGUE:',
                'DENGUE IGG',
                'DENGUE IGM',
                'DENGUE IGG, TESTE RÁPIDO',
                'DENGUE IGM, TESTE RÁPIDO',
                'DENGUE NS1, TESTE RÁPIDO',
                'ANTÍGENO NS1 DO VIRUS DA DENGUE',
                'NS1 EUROIMMUN'
            ], 
            'detalhe_exame', 
            'result', 
            'DENV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'VÍRUS ZIKA:'
            ], 
            'detalhe_exame', 
            'result', 
            'ZIKV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'VÍRUS CHIKUNGUNYA:'
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
    FROM
    {{ ref("einstein_02_fix_values") }}

)
SELECT * FROM source_data