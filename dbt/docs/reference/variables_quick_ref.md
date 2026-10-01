# DBT Variables - Quick Reference & Examples

## 🚀 Tóm tắt nhanh

Variables trong dbt cho phép bạn truyền tham số động khi chạy job, giúp code linh hoạt và có thể tái sử dụng.

## 📋 Các cách truyền Variables

### 1. Command Line (Phổ biến nhất)
```bash
dbt run --vars '{"start_date": "2024-01-01", "environment": "prod"}'
```

### 2. Environment Variables
```bash
set DBT_ENV=production
dbt run --vars '{"environment": "{{ env_var('DBT_ENV') }}"}'
```

### 3. dbt_project.yml (Default values)
```yaml
vars:
  environment: 'dev'
  start_date: '2024-01-01'
```

## 🎯 Ví dụ thực tế trong project

### Test Variables
```bash
cd aml_transaction_pipeline
dbt run --select variable_debug --vars '{"environment": "dev", "start_date": "2024-01-01", "min_amount": 1000}'
```

### Chạy với Date Range
```bash
dbt run --select fact_transactions_dynamic --vars '{"start_date": "2024-01-01", "end_date": "2024-01-31"}'
```

### Environment-specific runs
```bash
# Development
dbt run --vars '{"environment": "dev", "debug_mode": true, "dev_limit": 100}'

# Production  
dbt run --vars '{"environment": "prod", "debug_mode": false, "batch_size": 10000}'
```

### Incremental với Variables
```bash
# Full refresh
dbt run --select dim_customers_incremental --vars '{"full_refresh": true}' --full-refresh

# Incremental với lookback
dbt run --select dim_customers_incremental --vars '{"lookback_days": 3}'
```

## 🔧 Sử dụng trong SQL

### Basic Usage
```sql
SELECT * FROM transactions 
WHERE transaction_date >= '{{ var("start_date") }}'
  AND amount >= {{ var("min_amount", 0) }}
```

### Conditional Logic
```sql
{% if var('environment') == 'dev' %}
    LIMIT {{ var('dev_limit', 1000) }}
{% endif %}
```

### Default Values
```sql
-- Luôn có default value
{{ var('batch_size', 1000) }}

-- Check if exists
{% if var('custom_filter', none) %}
    AND {{ var('custom_filter') }}
{% endif %}
```

## 📊 Variables được hỗ trợ trong project

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `environment` | string | 'dev' | Environment (dev/prod) |
| `start_date` | string | '2024-01-01' | Filter start date |
| `end_date` | string | '2024-12-31' | Filter end date |
| `min_amount` | number | none | Minimum amount filter |
| `max_amount` | number | none | Maximum amount filter |
| `debug_mode` | boolean | false | Enable debug mode |
| `batch_size` | number | 1000 | Batch processing size |
| `batch_id` | string | 'MANUAL' | Batch identifier |
| `full_refresh` | boolean | false | Force full refresh |
| `lookback_days` | number | none | Incremental lookback |
| `mask_pii` | boolean | true | Mask PII data |

## 🛠️ Tools & Scripts

### Interactive Demo Script
```bash
# Chạy script demo interactive
aml_transaction_pipeline/scripts/test_variables.bat
```

### Debug Variables
```bash
# Xem tất cả variable values hiện tại
dbt run --select variable_debug --vars '{"environment": "dev"}'
```

### Macros hỗ trợ
- `get_dynamic_filter()` - Tạo WHERE conditions động
- `log_current_variables()` - Log tất cả variables
- `validate_required_vars()` - Validate required variables
- `get_batch_metadata()` - Tạo batch metadata

## ⚡ Best Practices

### 1. Luôn có Default Values
```sql
{{ var('batch_size', 1000) }}  -- ✅ Good
{{ var('batch_size') }}        -- ❌ Bad - có thể error
```

### 2. Validate Input
```sql
{% if var('start_date', none) %}
    {% if not (var('start_date') | length == 10) %}
        {{ exceptions.raise_compiler_error("start_date must be YYYY-MM-DD") }}
    {% endif %}
{% endif %}
```

### 3. Environment-specific Logic
```sql
{% if var('environment') == 'prod' %}
    -- Production logic
{% else %}
    -- Development logic with limited data
    WHERE customer_id <= {{ var('dev_limit', 1000) }}
{% endif %}
```

### 4. Log Variables for Debugging
```sql
{{ log("Running with environment: " ~ var('environment'), info=true) }}
```

## 🚨 Common Issues & Solutions

### Issue: Variable not found
```bash
# Error: Required var 'start_date' not found
# Solution: Provide default or pass via --vars
{{ var('start_date', '2024-01-01') }}
```

### Issue: Boolean values in Oracle
```sql
-- Oracle stores booleans as strings
{% if var('debug_mode', 'false') == 'true' %}
    -- Debug logic
{% endif %}
```

### Issue: Date format
```sql
-- Use proper Oracle date format
WHERE transaction_date >= DATE '{{ var('start_date') }}'
```

## 📈 Advanced Examples

### Dynamic Table Names
```sql
FROM {{ source('raw_data', var('source_table', 'transactions')) }}
```

### Conditional Columns
```sql
SELECT 
    customer_id,
    {% if var('environment') == 'prod' %}
        phone_number,
        email
    {% else %}
        'MASKED' as phone_number,
        'MASKED' as email
    {% endif %}
FROM customers
```

### Variable-driven Tests
```sql
-- tests/assert_min_records.sql
SELECT COUNT(*) as record_count
FROM {{ ref('fact_transactions') }}
HAVING COUNT(*) < {{ var('min_expected_records', 1000) }}
```

## 🎯 Production Deployment

### Airflow Integration
```python
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

### dbt Cloud Jobs
```yaml
# Job configuration
vars:
  start_date: "{{ ds }}"
  environment: "prod"
  batch_size: 10000
```

---

**💡 Tip**: Sử dụng `scripts/test_variables.bat` để test interactively tất cả các scenarios!
