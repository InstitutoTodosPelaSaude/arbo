{% macro tables_have_the_same_number_lines(table1, table2) %}
    SELECT *
    FROM (
        SELECT
            (SELECT COUNT(*) FROM {{ ref(table1) }}) = (SELECT COUNT(*) FROM {{ ref(table2) }}) AS tables_have_the_same_number_lines
    ) AS subquery
    WHERE tables_have_the_same_number_lines = FALSE

{% endmacro %}