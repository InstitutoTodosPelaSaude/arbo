{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT
        *
    FROM {{ ref('sabin_03_deduplicate_denguegi') }}
)
SELECT
    *,
    {{ 
        pivot_pathogen_results(
            [
                'PCRDE',
                'DENGIGG', 'DENGUEGI',
                'DENGUEMELISA',
                'DENGUEMIC',
                'DENGIGM',
                'DNS1',
                'DENGUEMI',
                'NS1ELISA',
                'NS1IMUNOCRO',
                'PCRDECT',
                'PCRD'
            ], 
            'detalhe_exame', 
            'result', 
            'DENV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'VIRUSZICA',
                'ZIKAGINDICE',
                'ZIKAM1',
                'ZIKAM2',
                'ZIKAIGG2',
                'ZIKAPCRBIO'
            ], 
            'detalhe_exame', 
            'result', 
            'ZIKV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'PCRCHIK',
                'RCHIKUNGMELISAIGG',
                'RCHIKUNGMELISAIGM',
                'CHIKVPCR-BIOMOL',
                'PCRCHIKCT'
            ], 
            'detalhe_exame', 
            'result', 
            'CHIKV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'YFIC'
            ], 
            'detalhe_exame', 
            'result', 
            'YFV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'MAYROVIGG',
                'MAYVIGM',
                'RESMAYARO'
            ], 
            'detalhe_exame', 
            'result', 
            'MAYV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'DEVORO'
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