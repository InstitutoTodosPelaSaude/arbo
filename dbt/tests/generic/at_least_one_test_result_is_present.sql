{% test at_least_one_test_result_is_present(model) %}

    SELECT
        *
    FROM {{ model }}
    WHERE
        "denv_test_result" = -1 AND
        "zikv_test_result" = -1 AND
        "chikv_test_result" = -1 AND
        "yfv_test_result" = -1 AND
        "mayv_test_result" = -1 AND
        "orov_test_result" = -1 AND
        "wnv_test_result" = -1

{% endtest %}