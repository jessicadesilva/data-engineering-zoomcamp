{{
    config(
        materialized='view'
    )
}}

select
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    safe_cast(PUlocationID as integer) as pickup_locationid,
    safe_cast(DOlocationID as integer) as dropoff_locationid,
    SR_Flag,
    Affiliated_base_number
from {{ source('staging', 'fhv_tripdata') }}

WHERE EXTRACT(YEAR FROM pickup_datetime)=2019

 -- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
 {% if var('is_test_run', default=true) %}

    limit 100

{% endif %}