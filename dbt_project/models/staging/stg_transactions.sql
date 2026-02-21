with source as (
    select * from {{ source('raw', 'transactions') }}
)

select
    {{ clean_string('transaction_id') }} as transaction_id,
    {{ clean_string('employee_id') }} as employee_id,
    {{ clean_string('benefit_account_id') }} as benefit_account_id,
    {{ clean_string('merchant') }} as merchant,
    {{ safe_cast_numeric('amount') }} as amount,
    {{ normalize_timestamp('timestamp') }} as transaction_timestamp,
    lower({{ clean_string('category') }}) as category
from source
