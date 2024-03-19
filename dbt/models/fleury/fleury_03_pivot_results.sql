{{ config(materialized='table') }}

WITH source_data AS (
    SELECT * FROM
    {{ ref("fleury_02_fix_values") }}
)
SELECT
    *,                
    {{ 
        pivot_pathogen_results(
            [
                'DENGUE, ANTIGENO NS1, TESTE RAPIDO',
                'DENGUE, IGG',
                'DENGUE, IGG, TESTE RAPIDO',
                'DENGUE, IGM',
                'DENGUE, IGM, TESTE RAPIDO',
                'DENGUE, NS1'
            ], 
            'pathogen', 
            'result', 
            'DENV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'ZIKA VIRUS, DETECCAO NO RNA',
                'ZIKA VIRUS - IGG',
                'ZIKA VIRUS - IGM'
            ], 
            'pathogen', 
            'result', 
            'ZIKV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'CHIKUNGUNYA, ANTICORPOS, IGG',
                'CHIKUNGUNYA, ANTICORPOS, IGM',
                'CHIKUNGUNYA, PCR'
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