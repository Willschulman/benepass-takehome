with transactions as (
    select * from {{ ref('int_transactions_enriched') }}
),

merchant_categories as (
    select
        merchant,
        category,
        count(*) as category_count
    from transactions
    group by merchant, category
),

ranked as (
    select
        merchant,
        category as primary_category,
        category_count as transaction_count,
        row_number() over (partition by merchant order by category_count desc) as rn
    from merchant_categories
)

select
    merchant,
    primary_category,
    transaction_count
from ranked
where rn = 1