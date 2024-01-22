-- arbo_pcr_3 MUST HAVE denv, chikv, zika SIMULTANEOUSLY
-- igg_serum, igm_serum MUST HAVE den, chik, zika
-- ns1_antigen MUST HAVE den
-- denv_pcr MUST HAVE denv
-- chikv_pcr MUST HAVE chik
-- zika_pcr MUST HAVE zika

SELECT
    *
FROM {{ ref('sabin_05_fill_results') }}
WHERE 0=1
    -- OR ( test_kit = 'arbo_pcr_3' AND NOT ( DENV_test_result IN ('Pos','Neg') AND ZIKV_test_result IN ('Pos','Neg') AND CHIKV_test_result IN ('Pos','Neg') ) )
    OR ( test_kit in ('igg_serum', 'igm_serum') AND NOT (DENV_test_result IN ('Pos','Neg') OR ZIKV_test_result IN ('Pos','Neg') OR CHIKV_test_result IN ('Pos','Neg')) )
    OR ( test_kit = 'ns1_antigen' AND NOT (DENV_test_result IN ('Pos','Neg')) )
    OR ( test_kit = 'denv_pcr' AND NOT (DENV_test_result IN ('Pos','Neg')) )
    OR ( test_kit = 'chikv_pcr' AND NOT (CHIKV_test_result IN ('Pos','Neg')) )
    OR ( test_kit = 'zika_pcr' AND NOT (ZIKV_test_result IN ('Pos','Neg')) )