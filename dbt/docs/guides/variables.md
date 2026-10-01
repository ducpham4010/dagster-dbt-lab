# DBT Variables - Hướng dẫn chi tiết

## 1. Các cách truyền biến trong dbt

### A. Command Line Variables (--vars)

#### Cú pháp cơ bản:
```bash
dbt run --vars '{"variable_name": "value"}'
```

#### Ví dụ thực tế:
```bash
# 1. Truyền ngày tháng để filter data
dbt run --vars '{"start_date": "2024-01-01", "end_date": "2024-12-31"}'

# 2. Truyền environment để switch logic
dbt run --vars '{"env": "prod", "debug": false}'

# 3. Truyền table names động
dbt run --vars '{"source_table": "raw_transactions", "target_schema": "marts"}'

# 4. Truyền số lượng records để test
dbt test --vars '{"limit_records": 1000}'

# 5. Chạy incremental với custom logic
dbt run --select +my_incremental_model --vars '{"full_refresh": false, "lookback_days": 7}'
```

### B. Environment Variables

#### Setup:
```bash
# Windows
set DBT_ENV=production
set DBT_START_DATE=2024-01-01
set DBT_BATCH_SIZE=5000

# Linux/Mac
export DBT_ENV=production
export DBT_START_DATE=2024-01-01
export DBT_BATCH_SIZE=5000
```

#### Sử dụng trong dbt:
```bash
dbt run --vars '{
  "environment": "{{ env_var('DBT_ENV') }}", 
  "start_date": "{{ env_var('DBT_START_DATE') }}", 
  "batch_size": {{ env_var('DBT_BATCH_SIZE') }}
}'
```

### C. dbt_project.yml Configuration

```yaml
# dbt_project.yml
vars:
  # Default values
  start_date: '2024-01-01'
  end_date: '2024-12-31'
  environment: 'dev'
  batch_size: 1000
  debug_mode: true
  
  # Environment-specific vars
  prod:
    batch_size: 10000
    debug_mode: false
  
  dev:
    batch_size: 100
    debug_mode: true
```

## 2. Sử dụng Variables trong Models

### A. Trong SQL Models

```sql
-- models/marts/fact_transactions_filtered.sql
{{ config(materialized='table') }}

SELECT 
    transaction_id,
    customer_id,
    transaction_date,
    amount,
    status
FROM {{ ref('stg_transactions') }}
WHERE 1=1
    {% if var('start_date', none) %}
        AND transaction_date >= '{{ var('start_date') }}'
    {% endif %}
    {% if var('end_date', none) %}
        AND transaction_date <= '{{ var('end_date') }}'
    {% endif %}
    {% if var('environment') == 'dev' %}
        AND customer_id IN (1, 2, 3, 4, 5)  -- Limit data in dev
    {% endif %}
    {% if var('debug_mode', false) %}
        LIMIT {{ var('batch_size', 1000) }}
    {% endif %}
```

### B. Conditional Logic với Variables

```sql
-- models/marts/dim_customers_dynamic.sql
SELECT 
    customer_id,
    customer_name,
    email,
    phone,
    
    -- Dynamic column based on environment
    {% if var('environment') == 'prod' %}
        encrypted_ssn,
        encrypted_account_number
    {% else %}
        'MASKED' as encrypted_ssn,
        'MASKED' as encrypted_account_number
    {% endif %}
    
FROM {{ ref('stg_customers') }}

{% if var('environment') == 'dev' %}
    -- Only load subset in dev
    WHERE customer_id <= {{ var('dev_customer_limit', 100) }}
{% endif %}
```

### C. Dynamic Table Names

```sql
-- models/staging/stg_transactions_dynamic.sql
SELECT *
FROM {{ source('raw_data', var('source_table_name', 'transactions')) }}
WHERE created_date >= '{{ var('start_date', '2024-01-01') }}'
```

## 3. Variables trong Tests

### A. Custom Test với Variables

```sql
-- tests/assert_transaction_count.sql
SELECT COUNT(*) as record_count
FROM {{ ref('fact_transactions') }}
WHERE transaction_date BETWEEN '{{ var('start_date') }}' AND '{{ var('end_date') }}'
HAVING COUNT(*) < {{ var('min_expected_records', 1000) }}
```

### B. Schema Tests với Variables

```yaml
# models/schema.yml
version: 2

models:
  - name: fact_transactions
    tests:
      - dbt_utils.expression_is_true:
          expression: "amount > 0"
          condition: "transaction_date >= '{{ var('start_date', '2024-01-01') }}'"
```

## 4. Variables trong Macros

```sql
-- macros/get_date_filter.sql
{% macro get_date_filter(date_column='created_date') %}
    {% if var('start_date', none) and var('end_date', none) %}
        {{ date_column }} BETWEEN '{{ var('start_date') }}' AND '{{ var('end_date') }}'
    {% elif var('start_date', none) %}
        {{ date_column }} >= '{{ var('start_date') }}'
    {% elif var('end_date', none) %}
        {{ date_column }} <= '{{ var('end_date') }}'
    {% else %}
        1=1  -- No date filter
    {% endif %}
{% endmacro %}
```

Sử dụng macro:
```sql
SELECT * 
FROM {{ ref('stg_transactions') }}
WHERE {{ get_date_filter('transaction_date') }}
```

## 5. Best Practices

### A. Default Values
```sql
-- Luôn có default value
{{ var('batch_size', 1000) }}

-- Check if variable exists
{% if var('custom_filter', none) %}
    AND {{ var('custom_filter') }}
{% endif %}
```

### B. Type Conversion
```sql
-- Convert string to number
WHERE amount > {{ var('min_amount') | int }}

-- Convert to boolean
{% if var('include_deleted', 'false') | as_bool %}
    -- Include deleted records
{% endif %}
```

### C. Validation
```sql
-- Validate date format
{% if var('start_date', none) %}
    {% if not (var('start_date') | length == 10) %}
        {{ exceptions.raise_compiler_error("start_date must be in YYYY-MM-DD format") }}
    {% endif %}
{% endif %}
```

## 6. Ví dụ thực tế - Customer ETL với Variables

### Command để chạy:
```bash
# Development run
dbt run --select +dim_customers --vars '{
  "environment": "dev",
  "start_date": "2024-01-01",
  "end_date": "2024-01-31",
  "batch_size": 100,
  "debug_mode": true
}'

# Production run
dbt run --select +dim_customers --vars '{
  "environment": "prod",
  "start_date": "2024-01-01",
  "end_date": "2024-12-31",
  "batch_size": 10000,
  "debug_mode": false
}'

# Incremental run với lookback
dbt run --select +fact_transactions --vars '{
  "is_incremental": true,
  "lookback_days": 3,
  "environment": "prod"
}'
```

## 7. Variables trong dbt Cloud / Airflow

### dbt Cloud:
```yaml
# Job configuration
vars:
  start_date: "{{ ds }}"  # Airflow date
  end_date: "{{ next_ds }}"
  environment: "{{ var.value.env }}"
```

### Airflow DAG:
```python
from airflow.operators.bash import BashOperator

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='''
    dbt run --vars '{
        "start_date": "{{ ds }}",
        "end_date": "{{ next_ds }}",
        "environment": "prod",
        "batch_id": "{{ run_id }}"
    }'
    '''
)
```

## 8. Debugging Variables

### Check variable values:
```sql
-- models/debug/variable_check.sql
SELECT 
    '{{ var("start_date", "NOT_SET") }}' as start_date,
    '{{ var("end_date", "NOT_SET") }}' as end_date,
    '{{ var("environment", "NOT_SET") }}' as environment,
    {{ var("batch_size", 0) }} as batch_size,
    '{{ var("debug_mode", false) }}' as debug_mode
```

### Log variables:
```sql
{{ log("Running with start_date: " ~ var('start_date', 'NOT_SET'), info=true) }}
{{ log("Environment: " ~ var('environment', 'NOT_SET'), info=true) }}
```
