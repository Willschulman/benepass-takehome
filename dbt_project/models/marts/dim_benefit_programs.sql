with benefit_accounts as (
    select * from {{ ref('stg_benefit_accounts') }}
)

select distinct
    benefit_type,
    program_name,
    monthly_allowance
from benefit_accounts