with staged as (
    select * from {{ ref('int_transactions_enriched') }}
),

quarantined as (
    select
        *,
        case
            when amount < 0
                then 'negative_amount'
            when is_employee_terminated
                then 'terminated_employee'
            when employee_status is null
                then 'invalid_employee_id'
            when benefit_type is null
                then 'invalid_benefit_account_id'
            when employee_id != account_employee_id
                then 'cross_employee_account_usage'
        end as rejection_reason
    from staged
)

select * from quarantined
where rejection_reason is not null
