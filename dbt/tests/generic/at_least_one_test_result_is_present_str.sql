{% test at_least_one_test_result_is_present_str(model) %}

    SELECT
        *
    FROM {{ model }}
    WHERE
        "denv_test_result"  = 'NT' AND
        "zikv_test_result"  = 'NT' AND
        "chikv_test_result" = 'NT' AND
        "yfv_test_result"   = 'NT' AND
        "mayv_test_result"  = 'NT' AND
        "orov_test_result"  = 'NT' AND
        "wnv_test_result"   = 'NT'
{% endtest %}