{{ config(materialized='table') }}

{% set get_state_codes %}
  select distinct state_code from {{ ref("matrix_02_epiweek_state") }}
{% endset %}
{% if execute %}
 {% set results_list = run_query(get_state_codes).columns[0].values() %}
{% endif %}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        state_code,
        MAX(CASE WHEN pathogen = 'DENV' THEN "Pos" ELSE 0 END) AS "DENV"
    FROM {{ ref("matrix_02_epiweek_state") }}
    GROUP BY epiweek_enddate, state_code
)
SELECT
    epiweek_enddate as "Semanas epidemiológicas",
    {% for state in results_list %}
        MAX(CASE WHEN state_code = '{{ state }}' THEN "DENV" ELSE NULL END) AS "{{ state }}"{% if not loop.last %},{% endif %}
    {% endfor %}
FROM source_data
GROUP BY epiweek_enddate
ORDER BY epiweek_enddate
    