{% test only_one_test_result_is_present(model) %}

    SELECT
        *
    FROM {{ model }}
    WHERE
        NOT (
            "denv_test_result"+
            "zikv_test_result"+
            "chikv_test_result"+
            "yfv_test_result"+
            "mayv_test_result"+
            "orov_test_result"+
            "wnv_test_result"
        ) IN (-6, -5)
        -- Because the test results are -1 for not tested, 0 for negative and 1 for positive
        -- the only way to have a sum of -6 or -5 is if there is only one test result present (0 or 1)

{% endtest %}