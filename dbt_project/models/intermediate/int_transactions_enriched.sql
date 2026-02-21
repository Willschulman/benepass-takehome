with transactions as (
    select * from {{ ref('stg_transactions') }}
),

benefit_accounts as (
    select * from {{ ref('stg_benefit_accounts') }}
),

employees as (
    select * from {{ ref('stg_employees') }}
)

select
    t.transaction_id,
    t.employee_id,
    t.benefit_account_id,
    t.merchant,
    t.amount,
    t.transaction_timestamp,
    t.category,
    ba.benefit_type,
    ba.program_name,
    ba.employee_id as account_employee_id,
    e.status as employee_status,
    e.is_terminated as is_employee_terminated,
    e.first_name as employee_first_name,
    e.last_name as employee_last_name
from transactions t
left join benefit_accounts ba on t.benefit_account_id = ba.benefit_account_id
left join employees e on t.employee_id = e.employee_id
