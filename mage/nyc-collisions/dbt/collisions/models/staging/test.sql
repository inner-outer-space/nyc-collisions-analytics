SELECT *
FROM 
    {{ source('staging','crash_external_data') }}

LIMIT 10