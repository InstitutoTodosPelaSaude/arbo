SELECT
    *
FROM {{ ref('sabin_05_fill_results') }}
WHERE 1=1 
    AND DENV_test_result = 'NT'
    AND ZIKV_test_result = 'NT'
    AND CHIKV_test_result = 'NT'
    AND YFV_test_result = 'NT'
    AND MAYV_test_result = 'NT'
    AND OROV_test_result = 'NT'
    AND WNV_test_result = 'NT' 