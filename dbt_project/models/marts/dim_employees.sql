with employees as (
    select * from {{ ref('stg_employees') }}
)

select
    employee_id,
    first_name,
    last_name,
    email,
    hire_date,
    status,
    is_terminated,
    (current_date - hire_date)::int as tenure_days
from employees
