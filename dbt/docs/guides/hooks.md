# dbt Hooks Guide

## Hooks la gi?

Hooks la **cac doan SQL duoc chay tu dong** truoc hoac sau khi dbt thuc hien mot hanh dong (build model, load seed, bat dau run, ...).

Tuong tu nhu trigger trong database, nhung o tang dbt:
- Database trigger: chay khi INSERT/UPDATE/DELETE tren bang
- dbt hook: chay khi dbt BUILD/SEED/RUN

## 4 cap do hooks

```
dbt run
│
├── CAP 1: on-run-start          (1 lan, dau tien)
│
├── CAP 2: project-level hooks   (moi model)
│   ├── +pre-hook   (truoc build)
│   ├── [BUILD MODEL]
│   └── +post-hook  (sau build)
│
├── CAP 3: model-level hooks     (1 model cu the)
│   ├── pre_hook    (truoc build)
│   ├── [BUILD MODEL]
│   └── post_hook   (sau build)
│
├── CAP 4: seed hooks            (khi load CSV)
│   └── +post-hook  (sau load)
│
└── CAP 1: on-run-end            (1 lan, cuoi cung)
```

## Cap 1: on-run-start / on-run-end

**Khai bao trong**: `dbt_project.yml`
**Chay khi**: bat dau / ket thuc **toan bo** lenh `dbt run`
**So lan chay**: **1 lan duy nhat** moi lenh

### Vi du trong project

```yaml
# dbt_project.yml
on-run-start:
  - "{{ create_audit_table() }}"    # Tao bang audit neu chua co

on-run-end:
  - "{{ audit_summary() }}"         # In tong ket
```

### Khi nao dung

| Use case | Hook |
|----------|------|
| Tao bang log/audit | on-run-start |
| Set session parameters (NLS_DATE_FORMAT) | on-run-start |
| Ghi tong ket vao bang | on-run-end |
| Gui notification (qua stored procedure) | on-run-end |
| Dong ket noi, cleanup | on-run-end |

### Flow thuc te

```
$ dbt run --vars "{business_date: '20260920'}"

10:00:00  on-run-start: create_audit_table()
          -> CREATE TABLE dbt_audit_log (...) -- neu chua co
          -> ORA-00955 table already exists -> skip (co EXCEPTION handler)

10:00:01  Building model: dbt_curated_transaction
10:00:03  Building model: dbt_insight_high_value_txn

10:00:05  on-run-end: audit_summary()
          -> SELECT COUNT(*) FROM dbt_audit_log WHERE run_started = '...'
          -> AUDIT SUMMARY: 2 models built
```

## Cap 2: Project-level hooks

**Khai bao trong**: `dbt_project.yml` duoi `models:`
**Chay khi**: truoc/sau **moi model** trong section do
**So lan chay**: 1 lan **moi model**

### Vi du trong project

```yaml
# dbt_project.yml
models:
  aml_transaction_pipeline:
    curated:
      +materialized: incremental
      +post-hook:
        - "{{ audit_log_insert() }}"       # Ghi audit log
        - "{{ gather_table_stats() }}"     # Cap nhat Oracle statistics
```

### Giai thich tung hook

**audit_log_insert()** — ghi 1 dong vao bang `dbt_audit_log`:
```sql
INSERT INTO dbt_audit_log (model_name, schema_name, event_type, row_count, ...)
VALUES ('dbt_curated_transaction', 'vpbcustom', 'MODEL_BUILT',
        (SELECT COUNT(*) FROM vpbcustom.dbt_curated_transaction), ...);
```

**gather_table_stats()** — cap nhat Oracle optimizer statistics:
```sql
DBMS_STATS.GATHER_TABLE_STATS(
    ownname => 'vpbcustom',
    tabname => 'DBT_CURATED_TRANSACTION',
    cascade => TRUE                -- cap nhat ca index stats
);
```
Tai sao can: sau khi INSERT du lieu moi, Oracle can biet phan bo du lieu
de chon execution plan tot nhat. Khong co stats -> query co the cham.

### Dac diem

- Ap dung cho **tat ca models** trong section (VD: tat ca models trong `curated/`)
- Neu co 3 models trong curated/ -> hook chay **3 lan**
- Thu tu: project-level hooks chay **SAU** model-level hooks
- Co the khai bao nhieu hooks bang list YAML

## Cap 3: Model-level hooks

**Khai bao trong**: `config()` cua tung file `.sql`
**Chay khi**: truoc/sau model **cu the do**
**So lan chay**: 1 lan cho model do

### Vi du trong project

```sql
-- models/curated/dbt_curated_transaction.sql
{{
    config(
        materialized='incremental',
        pre_hook="{{ log_model_event('START') }}",
        post_hook="{{ log_model_event('END') }}"
    )
}}
```

### log_model_event compile thanh

```sql
-- pre_hook (chay TRUOC khi INSERT du lieu)
BEGIN
    DBMS_OUTPUT.PUT_LINE(
        'START | vpbcustom.dbt_curated_transaction | run_started_at=2026-09-20 10:00:00 | ts=...'
    );
END;

-- [dbt BUILD MODEL: INSERT INTO ... SELECT ...]

-- post_hook (chay SAU khi INSERT xong)
BEGIN
    DBMS_OUTPUT.PUT_LINE(
        'END | vpbcustom.dbt_curated_transaction | run_started_at=2026-09-20 10:00:00 | ts=...'
    );
END;
```

### Bien dac biet trong hooks

| Bien | Gia tri | Giai thich |
|------|---------|------------|
| `{{ this }}` | `vpbcustom.dbt_curated_transaction` | Tham chieu day du den model |
| `{{ this.name }}` | `dbt_curated_transaction` | Ten model |
| `{{ this.schema }}` | `vpbcustom` | Schema cua model |
| `{{ this.database }}` | `amldb` | Database |
| `{{ run_started_at }}` | `2026-09-20 10:00:00+00:00` | Thoi diem dbt run bat dau |

## Cap 4: Seed hooks

**Khai bao trong**: `dbt_project.yml` duoi `seeds:`
**Chay khi**: sau khi `dbt seed` load CSV vao Oracle

### Vi du trong project

```yaml
# dbt_project.yml
seeds:
  aml_transaction_pipeline:
    +post-hook:
      - "{{ log_model_event('SEED_LOADED') }}"
```

### Khi chay `dbt seed`

```
$ dbt seed --full-refresh

Loading seed: account_types
  -> INSERT 4 rows from CSV
  -> post-hook: DBMS_OUTPUT.PUT_LINE('SEED_LOADED | vpbcustom.account_types | ...')

Loading seed: country_codes
  -> INSERT 20 rows from CSV
  -> post-hook: DBMS_OUTPUT.PUT_LINE('SEED_LOADED | vpbcustom.country_codes | ...')

... (6 seeds x 1 hook = 6 lan chay)
```

## Thu tu thuc hien hooks

Khi 1 model co CA model-level VA project-level hooks:

```
dbt_curated_transaction:
  1. pre_hook  (model-level):  log_model_event('START')
  2. [BUILD MODEL — INSERT INTO ... SELECT ...]
  3. post_hook (model-level):  log_model_event('END')
  4. +post-hook (project-level): audit_log_insert()
  5. +post-hook (project-level): gather_table_stats()
```

Quy tac:
- **pre-hook**: model-level chay TRUOC project-level
- **post-hook**: model-level chay TRUOC project-level
- Trong cung cap: chay theo **thu tu khai bao** (tren truoc, duoi sau)

## Ephemeral models KHONG co hooks

```
models/stag/dbt_stag_customer.sql    -> ephemeral -> KHONG co hooks
models/stag/dbt_stag_account.sql     -> ephemeral -> KHONG co hooks
models/stag/dbt_stag_transaction.sql -> ephemeral -> KHONG co hooks
```

Tai sao: ephemeral models **khong tao bang**. Chung duoc inline thanh CTE
trong model cha (curated). Vi khong co bang -> khong co gi de hook vao.

## Hooks chi chay voi `dbt run`

| Lenh | on-run-start/end | pre/post-hook | Giai thich |
|------|:---:|:---:|------------|
| `dbt run` | Co | Co | Build models -> chay hooks |
| `dbt test` | **Khong** | **Khong** | Test chi SELECT, khong build |
| `dbt compile` | **Khong** | **Khong** | Chi render Jinja, khong chay SQL |
| `dbt seed` | Co | Co (seed hooks) | Load CSV -> chay seed hooks |

## Cac hook macro trong project

### File: macros/hooks_audit_log.sql

| Macro | Cap | Muc dich |
|-------|-----|----------|
| `create_audit_table()` | on-run-start | Tao bang DBT_AUDIT_LOG |
| `audit_log_insert()` | post-hook (project) | Ghi 1 dong audit sau moi model |
| `audit_summary()` | on-run-end | In tong ket cuoi run |
| `grant_select(role)` | post-hook (manual) | Cap quyen SELECT cho role |
| `gather_table_stats()` | post-hook (project) | Chay DBMS_STATS cho optimizer |

### File: macros/log_model_event.sql

| Macro | Cap | Muc dich |
|-------|-----|----------|
| `log_model_event(type)` | pre/post-hook (model) | In START/END vao DBMS_OUTPUT |

## Flow day du khi chay `dbt run`

```
$ dbt run --vars "{business_date: '20260920'}"

[on-run-start]
  create_audit_table()
  -> CREATE TABLE dbt_audit_log (ok hoac skip neu da co)

[Model 1: dbt_stag_customer — ephemeral]
  -> Khong chay, khong co hook (inline thanh CTE)

[Model 2: dbt_stag_account — ephemeral]
  -> Khong chay, khong co hook

[Model 3: dbt_stag_transaction — ephemeral]
  -> Khong chay, khong co hook

[Model 4: dbt_curated_transaction — incremental]
  pre_hook (model):   log_model_event('START')    -> DBMS_OUTPUT
  BUILD:              DELETE + INSERT INTO ... SELECT (with CTEs from stag)
  post_hook (model):  log_model_event('END')      -> DBMS_OUTPUT
  +post-hook (project): audit_log_insert()        -> INSERT vao dbt_audit_log
  +post-hook (project): gather_table_stats()      -> DBMS_STATS

[Model 5: dbt_insight_high_value_txn — incremental]
  pre_hook (model):   log_model_event('START')
  BUILD:              DELETE + INSERT INTO ... SELECT FROM curated WHERE > 500M
  post_hook (model):  log_model_event('END')
  +post-hook (project): audit_log_insert()
  +post-hook (project): gather_table_stats()

[on-run-end]
  audit_summary()
  -> AUDIT SUMMARY: 2 models built
```

## Error handling trong hooks

Tat ca hook macros trong project deu co `EXCEPTION` handler:

```sql
BEGIN
    -- hook logic here
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('WARNING: ' || SQLERRM);
END;
```

Tai sao: neu hook fail (VD: khong co quyen GRANT, bang audit bi lock),
**khong muon model build bi fail theo**. Hook la phu, model la chinh.

Neu muon hook fail -> model fail: bo `EXCEPTION` handler di.

## Them hook moi

### Buoc 1: Tao macro trong macros/

```sql
-- macros/my_new_hook.sql
{% macro notify_slack() %}
BEGIN
    -- Goi stored procedure gui notification
    pkg_notification.send_slack(
        channel => '#data-pipeline',
        message => 'Model {{ this.name }} built successfully'
    );
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
{% endmacro %}
```

### Buoc 2: Khai bao trong dbt_project.yml

```yaml
# Ap dung cho 1 section
models:
  aml_transaction_pipeline:
    insight:
      +post-hook:
        - "{{ notify_slack() }}"

# Hoac ap dung cho toan bo run
on-run-end:
  - "{{ notify_slack() }}"
```

### Buoc 3: Hoac khai bao trong model config

```sql
{{ config(
    post_hook="{{ notify_slack() }}"
) }}
```

## Pre-hook use cases thuc te

| Use case | SQL |
|----------|-----|
| Set Oracle session format | `ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'` |
| Lock bang truoc khi write | `LOCK TABLE {{ this }} IN EXCLUSIVE MODE NOWAIT` |
| Xoa du lieu cu theo partition | `DELETE FROM {{ this }} WHERE business_date = TO_DATE('{{ var("business_date") }}', 'YYYYMMDD')` |
| Tat constraint tam thoi | `ALTER TABLE {{ this }} DISABLE CONSTRAINT ...` |

## Post-hook use cases thuc te

| Use case | SQL/Macro |
|----------|-----------|
| Ghi audit log | `audit_log_insert()` |
| Cap nhat stats | `gather_table_stats()` |
| Cap quyen | `grant_select('analyst_role')` |
| Bat constraint lai | `ALTER TABLE {{ this }} ENABLE CONSTRAINT ...` |
| Gui notification | Goi stored procedure |
| Tao index | `CREATE INDEX IF NOT EXISTS ...` |

## So sanh: Hooks vs Macros

| | Macros | Hooks |
|---|---|---|
| Chay khi nao | Khi duoc goi trong SQL | Tu dong truoc/sau model build |
| Khai bao o dau | Trong file .sql | Trong config() hoac dbt_project.yml |
| Output | SQL expression (inline) | PL/SQL block (chay rieng) |
| Vi du | `{{ risk_score_label('col') }}` | `pre_hook="{{ log_model_event('START') }}"` |
| Muc dich | Tao SQL | Side effect (log, grant, stats) |
