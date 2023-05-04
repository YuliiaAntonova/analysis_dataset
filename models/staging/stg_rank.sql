{{ config(materialized='view') }}

SELECT *, 
       RANK() OVER(PARTITION BY t.year ORDER BY t.rank_order ASC) j
FROM {{ source('staging','rank') }} as t
ORDER BY t.year, j

{% if var('is_test_run', default=false) %}
    limit 100
{% endif %}