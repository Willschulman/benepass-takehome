with staged as (
    select * from {{ ref('int_transactions_enriched') }}
),

quarantined_ids as (
    select transaction_id from {{ ref('quarantine_transactions') }}
),

valid_categories as (
    select * from {{ ref('category_benefit_mappings') }}
),

clean_transactions as (
    select
        s.*,
        vc.valid_category is not null as is_valid_category
    from staged s
    left join valid_categories vc
        on s.benefit_type = vc.benefit_type
        and s.category = vc.valid_category
    where s.transaction_id not in (select transaction_id from quarantined_ids)
)

select
    transaction_id,
    employee_id,
    benefit_account_id,
    merchant,
    amount,
    transaction_timestamp,
    category,
    benefit_type,
    program_name,
    account_employee_id,
    employee_status,
    employee_first_name,
    employee_last_name,
    {{ build_dq_flags([
        {'name': 'category_mismatch', 'condition': 'not is_valid_category'}
    ]) }} as dq_flags
from clean_transactions
