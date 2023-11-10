{% macro pivot_pathogen_results(pathogens, pathogen_source_column, result_column, pathogen_column_target) %}
    CASE
        {% for pathogen in pathogens %}
            WHEN {{ pathogen_source_column }} = '{{ pathogen }}' THEN {{ result_column }}
        {% endfor %}
        ELSE 'NT'
    END AS {{ pathogen_column_target }}
{% endmacro %}