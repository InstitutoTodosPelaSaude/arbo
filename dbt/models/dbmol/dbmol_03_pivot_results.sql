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
                -- já existentes
                'DENGG', 'DENGM', 'DENGUE', 'TDENGE',
                -- IgG
                'DENGIGG', 'DENGUEGI',
                -- IgM
                'DENGUEMELISA', 'DENGUEMIC', 'DENGIGM', 'DENGUEMI',
                -- NS1
                'DNS1', 'NS1IMUNOCRO', 'NS1ELISA',
                -- PCR
                'PCRDE', 'PCRD', 'DENV', 'DENV1', 'DENV2', 'DENV3', 'DENV4'
            ],
            'detalhe_exame',
            'result',
            'DENV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                -- já existentes
                'ZICAG', 'ZICAM', 'ZIKA', 'ZIKAP', 'ZIKAV',
                -- IgG
                'ZIKAGINDICE', 'ZIKAIGG2', 'ZIKAG',
                -- IgM
                'ZIKAM1', 'ZIKAM2', 'ZIKAMINDICE', 'ZIKAM',
                -- PCR
                'ZIKAPCRBIO', 'ZIKA1', 'ZIKA2', 'ZIKAPCR'
            ],
            'detalhe_exame',
            'result',
            'ZIKV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                -- já existentes
                'AACHIG', 'AACHIM', 'CHIKUN', 'CHIKV', 'CHIKV_IGG', 'CHIKV_IGM',
                -- IgG
                'RCHIKUNGMELISAIGG', 'IGGCHIKUN', 'CHIKUNG1', 'CHIKUNG2',
                -- IgM
                'RCHIKUNGMELISAIGM', 'IGMCHIKUNELISA', 'IGMCHIKUN', 'CHIKUNM1', 'CHIKUNM2',
                -- PCR
                'CHIKVPCR-BIOMOL'
            ],
            'detalhe_exame',
            'result',
            'CHIKV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                -- já existentes
                'FEAMA', 'YFV_IGG', 'YFV_IGM',
                -- sabin
                'FEBREGLC', 'FEBREMLC', 'YF'
            ],
            'detalhe_exame',
            'result',
            'YFV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                -- já existentes
                'MAYV_IGG', 'MAYV_IGM',
                -- PCR
                'MAYARO', 'RESMAYARO'
            ],
            'detalhe_exame',
            'result',
            'MAYV_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'OROG', 'OROM',        -- IgG / IgM
                'DEVORO', 'OROVPCR'    -- PCR
            ],
            'detalhe_exame',
            'result',
            'OROV_test_result'
        )
    }}
FROM source_data

