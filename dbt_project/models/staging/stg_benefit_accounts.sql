with source as (
    select * from {{ source('raw', 'benefit_accounts') }}
)

select
    {{ clean_string('benefit_account_id') }} as benefit_account_id,
    {{ clean_string('employee_id') }} as employee_id,
    lower({{ clean_string('benefit_type') }}) as benefit_type,
    {{ clean_string('program_name') }} as program_name,
    {{ safe_cast_numeric('monthly_allowance') }} as monthly_allowance,
    {{ safe_cast_numeric('current_balance') }} as current_balance
from source
