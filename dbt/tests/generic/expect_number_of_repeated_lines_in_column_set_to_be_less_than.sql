{% test expect_number_of_repeated_lines_in_column_set_to_be_less_than(model, columns, max_value) %}

    SELECT
        COUNT(*)
    FROM
    (
        -- Filter every line that is repeated more than once
        -- Based on the columns that are passed
        SELECT COUNT(*), {{ columns }}
        FROM {{ model }}
        GROUP BY {{ columns }}
        HAVING COUNT(*) > 1
    ) _
    HAVING COUNT(*) > {{ max_value }} 
    -- Test Fail if the number of repeated lines is greater than the max_value

{% endtest %}