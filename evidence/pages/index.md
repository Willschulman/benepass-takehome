---
title: Benefits Overview
queries: hide
---

```sql total_transactions
select count(*) as total_transactions from benepass.fact_transactions
```

```sql total_spend
select sum(amount) as total_spend from benepass.fact_transactions
```

```sql active_employees
select count(*) as active_employees from benepass.dim_employees where status = 'active'
```

```sql active_programs
select count(*) as active_programs from benepass.dim_benefit_programs
```

<BigValue data={total_transactions} value=total_transactions title="Total Transactions" />
<BigValue data={total_spend} value=total_spend title="Total Spend" fmt=usd />
<BigValue data={active_employees} value=active_employees title="Active Employees" />
<BigValue data={active_programs} value=active_programs title="Benefit Programs" />

## Spending by Benefit Type

```sql spending_by_type
select benefit_type, sum(amount) as total_spend, count(*) as transaction_count
from benepass.fact_transactions
group by benefit_type
order by total_spend desc
```

<BarChart data={spending_by_type} x=benefit_type y=total_spend title="Spending by Benefit Type" fmt=usd />

## Spending by Merchant

```sql spending_by_merchant
select merchant, sum(amount) as total_spend, count(*) as transaction_count
from benepass.fact_transactions
group by merchant
order by total_spend desc
```

<BarChart data={spending_by_merchant} x=merchant y=total_spend title="Spending by Merchant" fmt=usd />

## Program Utilization

```sql program_utilization
select
    bp.program_name,
    bp.benefit_type,
    bp.monthly_allowance,
    coalesce(sum(ft.amount), 0) as total_spend
from benepass.dim_benefit_programs bp
left join benepass.fact_transactions ft on bp.benefit_type = ft.benefit_type
group by bp.program_name, bp.benefit_type, bp.monthly_allowance
order by bp.program_name
```

<BarChart data={program_utilization} x=program_name y={["monthly_allowance", "total_spend"]} title="Monthly Allowance vs. Actual Spend" fmt=usd type=grouped />
