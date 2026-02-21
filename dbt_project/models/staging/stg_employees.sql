with source as (
    select * from {{ source('raw', 'employees') }}
)

select
    {{ clean_string('employee_id') }} as employee_id,
    {{ clean_string('first_name') }} as first_name,
    {{ clean_string('last_name') }} as last_name,
    {{ mask_pii('email') }} as email,
    cast(hire_date as date) as hire_date,
    lower({{ clean_string('status') }}) as status,
    lower({{ clean_string('status') }}) = 'terminated' as is_terminated
from source
