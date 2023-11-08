
{{
    config(
        materialized='incremental',
    )
}}

SELECT 
    *
FROM {{ ref('my_first_dbt_model') }}

{% if is_incremental() %}

WHERE id is not null

{% endif %}