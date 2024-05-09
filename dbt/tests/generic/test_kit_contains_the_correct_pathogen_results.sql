{% test test_kit_contains_the_correct_pathogen_results(model) %}


    SELECT
        *
    FROM {{ model }}
    WHERE 0=1
    OR (
        test_kit = 'arbo_pcr_3' 
        AND NOT ( 
            DENV_test_result      IN ('Pos','Neg') 
            AND ZIKV_test_result  IN ('Pos','Neg') 
            AND CHIKV_test_result IN ('Pos','Neg')
            AND MAYV_test_result = 'NT'
            AND WNV_test_result  = 'NT'
            AND OROV_test_result = 'NT'
            AND YFV_test_result  = 'NT'
        ) 
    )
    OR ( 
        test_kit in ('igg_serum', 'igm_serum') 
        AND NOT (
            DENV_test_result     IN ('Pos','Neg') 
            OR ZIKV_test_result  IN ('Pos','Neg') 
            OR CHIKV_test_result IN ('Pos','Neg')
            OR MAYV_test_result  IN ('Pos','Neg')
            OR OROV_test_result  IN ('Pos','Neg')
            OR YFV_test_result   IN ('Pos','Neg')
            OR WNV_test_result   IN ('Pos','Neg')
        ) 
    )
    OR ( test_kit = 'ns1_antigen' AND NOT (DENV_test_result  IN ('Pos','Neg')) )
    OR ( test_kit = 'denv_pcr'    AND NOT (DENV_test_result  IN ('Pos','Neg')) )
    OR ( test_kit = 'zikv_pcr'    AND NOT (ZIKV_test_result  IN ('Pos','Neg')) )
    OR ( test_kit = 'chikv_pcr'   AND NOT (CHIKV_test_result IN ('Pos','Neg')) )
    OR ( test_kit = 'mayv_pcr'    AND NOT (MAYV_test_result  IN ('Pos','Neg')) )
    OR ( test_kit = 'orov_pcr'    AND NOT (OROV_test_result  IN ('Pos','Neg')) )
    OR ( test_kit = 'wnv_pcr'     AND NOT (WNV_test_result   IN ('Pos','Neg')) )
    OR ( test_kit = 'yfv_pcr'     AND NOT (YFV_test_result   IN ('Pos','Neg')) )

{% endtest %}