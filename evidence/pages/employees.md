---
title: Employees
---

```sql active_count
select count(*) as active_count from benepass.dim_employees where status = 'active'
```

```sql terminated_count
select count(*) as terminated_count from benepass.dim_employees where status = 'terminated'
```

```sql avg_tenure
select round(avg(tenure_days), 0) as avg_tenure from benepass.dim_employees
```

<BigValue data={active_count} value=active_count title="Active Employees" />
<BigValue data={terminated_count} value=terminated_count title="Terminated Employees" />
<BigValue data={avg_tenure} value=avg_tenure title="Avg Tenure (Days)" />

## Tenure Distribution

```sql tenure
select
    employee_id,
    first_name || ' ' || last_name as employee_name,
    status,
    tenure_days
from benepass.dim_employees
order by tenure_days desc
```

<BarChart data={tenure} x=employee_name y=tenure_days title="Employee Tenure (Days)" />

## Spending per Employee

```sql employee_spending
select
    e.employee_id,
    e.first_name || ' ' || e.last_name as employee_name,
    e.status,
    e.tenure_days,
    coalesce(sum(ft.amount), 0) as total_spend,
    count(ft.transaction_id) as transaction_count
from benepass.dim_employees e
left join benepass.fact_transactions ft on e.employee_id = ft.employee_id
group by e.employee_id, e.first_name, e.last_name, e.status, e.tenure_days
order by total_spend desc
```

<BarChart data={employee_spending} x=employee_name y=total_spend title="Total Spend by Employee" fmt=usd />

<DataTable data={employee_spending} />
