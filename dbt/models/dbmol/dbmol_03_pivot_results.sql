{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ ref("dbmol_02_fix_values") }}

)
SELECT
    *,
    {{ 
        pivot_pathogen_results(
            [
                'DENGG',
                'DENGM',
                'DENGUE',
                'DNS1',
                'TDENGE'
            ], 
            'detalhe_exame', 
            'result', 
            'DENV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'ZICAG',
                'ZICAM',
                'ZIKA',
                'ZIKAP',
                'ZIKAV'
            ], 
            'detalhe_exame', 
            'result', 
            'ZIKV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'AACHIG',
                'AACHIM',
                'CHIKUN',
                'CHIKV',
                'CHIKV_IGG',
                'CHIKV_IGM',
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
                'MAYV_IGG',
                'MAYV_IGM',
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

