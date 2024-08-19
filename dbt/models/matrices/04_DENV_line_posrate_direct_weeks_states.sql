{{ config(materialized='table') }}

{# 
    This model is a filtered version of the matrix_03_denv_posrate_by_epiweek_state.sql model.
    It filters the states that have a fill rate of at least 50%.
#}
{% set state_fill_threshold = 0.5 %} 

-- Consulta SQL com Jinja que corresponde ao source_data
{% set source_data_sql_script %}
    SELECT
            epiweek_enddate,
            state_code,
            pathogen,
            {{ matrices_metrics('result') }}
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE "DENV_test_result" IN ('Pos', 'Neg') AND test_kit IN ('arbo_pcr_3', 'ns1_antigen', 'denv_pcr')
    GROUP BY epiweek_enddate, state_code, pathogen
    ORDER BY epiweek_enddate, state_code, pathogen
{% endset %}

{% set get_state_codes %}
    SELECT distinct state_code
        FROM (
            SELECT
                state_code,
                MAX(total) as total,
                COUNT(CASE WHEN "posrate" IS NOT NULL AND "posrate" > 0 THEN 1 ELSE NULL END) AS filled
            FROM 
                ({{ source_data_sql_script }}), 
                (SELECT COUNT(DISTINCT epiweek_enddate) as total FROM ({{ source_data_sql_script }}))
            WHERE pathogen = 'DENV'
            GROUP BY state_code
        ) AS state_fill_rates
    WHERE filled::decimal / total >= {{ state_fill_threshold }}
{% endset %}
{% if execute %}
 {% set results_list = run_query(get_state_codes).columns[0].values() %}
{% endif %}

WITH source_data AS (
    {{ source_data_sql_script }}
),

transformation AS (
    SELECT
        epiweek_enddate,
        state_code,
        MAX(CASE WHEN pathogen = 'DENV' THEN "posrate" * 100 ELSE NULL END) AS "DENV"
    FROM source_data
    WHERE state_code != 'NOT REPORTED'
    GROUP BY epiweek_enddate, state_code
)

SELECT
    epiweek_enddate as "Semanas epidemiológicas"
    {% for state in results_list %}
        {% if loop.first %},{% endif %}
        MAX(CASE WHEN state_code = '{{ state }}' THEN "DENV" ELSE NULL END) AS "{{ state }}"
        {% if not loop.last %},{% endif %}
    {% endfor %}
FROM transformation
GROUP BY epiweek_enddate
ORDER BY epiweek_enddate
    