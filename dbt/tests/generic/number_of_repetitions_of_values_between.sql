{% test number_of_repetitions_of_values_between(model, column_name, min_value, max_value) %}

    SELECT
        COUNT(*), {{ column_name }}
    FROM {{ model }}
    GROUP BY {{ column_name }}
    HAVING NOT COUNT(*) BETWEEN {{ min_value }} AND {{ max_value }}
    -- Checks if the number of repetitions of each value in the column is between min_value and max_value
    -- Useful to detect unwanted duplicates in a column

{% endtest %}