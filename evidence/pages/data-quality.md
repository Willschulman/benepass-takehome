---
title: Data Quality
queries: hide
---

```sql clean_count
select count(*) as clean_count from benepass.fact_transactions
```

```sql quarantine_count
select count(*) as quarantine_count from benepass.quarantine_transactions
```

```sql clean_rate
select
    round(
        (select count(*) from benepass.fact_transactions)::float /
        ((select count(*) from benepass.fact_transactions) + (select count(*) from benepass.quarantine_transactions))::float * 100,
    1) as clean_rate
```

<BigValue data={clean_count} value=clean_count title="Clean Records" />
<BigValue data={quarantine_count} value=quarantine_count title="Quarantined Records" />
<BigValue data={clean_rate} value=clean_rate title="Clean Rate %" fmt=num1 />

## Quarantined by Rejection Reason

```sql quarantine_by_reason
select rejection_reason, count(*) as record_count
from benepass.quarantine_transactions
group by rejection_reason
order by record_count desc
```

<BarChart data={quarantine_by_reason} x=rejection_reason y=record_count title="Quarantined Records by Reason" />

## DQ Flags on Clean Records

```sql flagged_count
select count(*) as flagged_records
from benepass.fact_transactions
where len(dq_flags) > 0
```

<BigValue data={flagged_count} value=flagged_records title="Records with DQ Flags" />

## Quarantined Records Detail

```sql quarantine_detail
select
    transaction_id,
    employee_id,
    merchant,
    amount,
    rejection_reason
from benepass.quarantine_transactions
order by transaction_id
```

<DataTable data={quarantine_detail} />
