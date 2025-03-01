---
displayed_sidebar: docs
---

# SQL Digest

This topic introduces the SQL Digest feature of StarRocks. This feature is supported from v3.3.6 onwards.

## Overview

SQL Digest is a fingerprint generated by historical SQL statements with parameters removed. It helps cluster SQL statements with the same structure but different parameters.

Common use cases of SQL Digest include:

- Finding other SQL statements with the same structure but different parameters in query history
- Tracking execution frequency, cumulative time, and other statistics of SQL with the same structure
- Identifying the most time-consuming SQL patterns in the system

In StarRocks, SQL Digests are mainly recorded through audit logs **fe.audit.log**. For example, execute the following two SQL statements:

```SQL
SELECT count(*) FROM lineorder WHERE lo_orderdate > '19920101';
SELECT count(*) FROM lineorder WHERE lo_orderdate > '19920202';
```

Two same Digest will be generated in **fe.audit.log**:

```SQL
Digest=f58bb71850d112014f773717830e7f77
Digest=f58bb71850d112014f773717830e7f77
```

## Usage

### Prerequisites

To enable this feature, you must set the FE configuration item `enable_sql_digest` to `true`.

Execute the following statement to enable it dynamically:

```SQL
ADMIN SET FRONTEND CONFIG ('enable_sql_digest'='true');
```

To enable it permanently, you must add `enable_sql_digest = true` to the FE configuration file `fe.conf` and restart FE.

After enabling this feature, you can install the [AuditLoader](./management/audit_loader.md) plugin to perform statistical analysis on SQL statements.

### Find similar SQL

```SQL
SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__ 
WHERE digest = '<Digest>'
LIMIT 1;
```

### Track daily execution count and time of similar SQL

```SQL
SELECT 
    date_trunc('day', `timestamp`) query_date, 
    count(*), 
    sum(queryTime), 
    sum(scanRows), 
    sum(cpuCostNs), 
    sum(memCostBytes)
FROM starrocks_audit_db__.starrocks_audit_tbl__ 
WHERE digest = '<Digest>'
GROUP BY query_date
ORDER BY query_date 
DESC LIMIT 30;
```

### Calculate average execution time of similar SQL

```SQL
SELECT avg(queryTime), min(queryTime), max(queryTime), stddev(queryTime)
FROM starrocks_audit_db__.starrocks_audit_tbl__ 
WHERE digest = '<Digest>';
```

### Aggregate similar SQL to analyze the most time-consuming pattern

```SQL
WITH top_sql AS (
    SELECT digest, sum(queryTime)
    FROM starrocks_audit_db__.starrocks_audit_tbl__ 
    GROUP BY digest
    ORDER BY sum(queryTime) 
    DESC LIMIT 10 
)
SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__ 
WHERE digest IN (SELECT digest FROM top_sql);
```

## Parameter normalization rules

- Constant values in SQL will be normalized. For example, similar SQL statements with `WHERE a = 1` and `WHERE a = 2` will have the same Digest.
- IN predicates will be normalized. For example, similar SQL statements with `IN (1,2,3)` and `IN (1,2)` will have the same Digest.
- `LIMIT N` clauses will be normalized. For example, similar SQL statements with `LIMIT 10` and `LIMIT 30` will have the same Digest.

<!--
- For `INSERT VALUES`, multiple `VALUES` rows will be normalized.
-->
