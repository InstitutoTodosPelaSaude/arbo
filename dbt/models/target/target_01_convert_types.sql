WITH source_data AS (
    SELECT * FROM 
    {{ source("dagster", "target_raw") }}
    
)
SELECT 
    *
    -- Correções aqui dentro
FROM source_data