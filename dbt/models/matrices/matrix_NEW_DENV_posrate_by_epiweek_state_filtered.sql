{{ config(materialized='table') }}

{# 
    This model is a filtered version of the matrix_03_denv_posrate_by_epiweek_state.sql model.
    It filters the states that have a fill rate of at least 70%.
#}
{% set state_fill_threshold = 0.7 %} 

{% set get_state_codes %}
    SELECT distinct state_code
        FROM (
            SELECT
                state_code,
                MAX(total) as total,
                COUNT(CASE WHEN "posrate" IS NOT NULL AND "posrate" > 0 THEN 1 ELSE NULL END) AS filled
            FROM 
                {{ ref("matrix_02_epiweek_state") }}, 
                (SELECT COUNT(DISTINCT epiweek_enddate) as total FROM {{ ref("matrix_02_epiweek_state") }})
            WHERE pathogen = 'DENV'
            GROUP BY state_code
        ) AS state_fill_rates
    WHERE filled::decimal / total >= {{ state_fill_threshold }}
{% endset %}
{% if execute %}
 {% set results_list = run_query(get_state_codes).columns[0].values() %}
{% endif %}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        state_code,
        MAX(CASE WHEN pathogen = 'DENV' THEN "posrate" ELSE NULL END) AS "DENV"
    FROM {{ ref("matrix_02_epiweek_state") }}
    GROUP BY epiweek_enddate, state_code
)
SELECT
    epiweek_enddate as "Semanas epidemiológicas"
    {% for state in results_list %}
        {% if loop.first %},{% endif %}
        MAX(CASE WHEN state_code = '{{ state }}' THEN "DENV" ELSE NULL END) AS "{{ state }}"
        {% if not loop.last %},{% endif %}
    {% endfor %}
FROM source_data
GROUP BY epiweek_enddate
ORDER BY epiweek_enddate
    