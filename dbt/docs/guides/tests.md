# dbt Tests - Hướng dẫn toàn diện về Data Quality Testing

## 📋 Mục lục

1. [Giới thiệu về dbt Tests](#giới-thiệu-về-dbt-tests)
2. [Các loại Tests](#các-loại-tests)
3. [Built-in Tests](#built-in-tests)
4. [Singular Tests](#singular-tests)
5. [Generic Tests](#generic-tests)
6. [Business Rules Tests](#business-rules-tests)
7. [Test Organization](#test-organization)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)
10. [Advanced Testing Strategies](#advanced-testing-strategies)

## 🎯 Giới thiệu về dbt Tests

### Tests là gì?

**dbt Tests** là các **SQL queries** được thiết kế để **validate data quality** và **business rules** trong dbt project. Tests giúp đảm bảo:

- ✅ **Data Integrity** - Dữ liệu không bị lỗi
- ✅ **Business Rules** - Tuân thủ quy tắc nghiệp vụ  
- ✅ **Data Quality** - Chất lượng dữ liệu cao
- ✅ **Pipeline Reliability** - ETL pipeline ổn định
- ✅ **Early Error Detection** - Phát hiện lỗi sớm

### Tại sao cần Tests?

```sql
-- ❌ KHÔNG CÓ TESTS - Rủi ro cao
-- Dữ liệu có thể có:
-- - Duplicate records
-- - NULL values ở primary key
-- - Invalid formats
-- - Business rule violations
-- - Data inconsistency

-- ✅ CÓ TESTS - An toàn và tin cậy
-- Tự động kiểm tra:
-- - Unique constraints
-- - Not null constraints  
-- - Data format validation
-- - Business logic compliance
-- - Cross-table consistency
```

## 📁 Cấu trúc Tests trong dbt

```
dbt_project/
├── tests/                          # Custom tests folder
│   ├── singular/                   # Singular tests (specific queries)
│   │   ├── test_no_duplicates.sql
│   │   ├── test_valid_formats.sql
│   │   └── test_data_consistency.sql
│   ├── generic/                    # Generic tests (reusable)
│   │   ├── test_value_in_range.sql
│   │   ├── test_string_pattern.sql
│   │   └── test_not_empty_string.sql
│   └── business_rules/             # Business logic tests
│       ├── test_customer_rules.sql
│       └── test_financial_rules.sql
├── models/
│   ├── staging/
│   │   └── schema.yml             # Built-in tests for staging
│   └── marts/
│       └── schema.yml             # Built-in tests for marts
└── dbt_project.yml
```

## 🔧 Các loại Tests

### 1. **Built-in Tests** (trong schema.yml)

Các tests có sẵn trong dbt:

```yaml
# models/schema.yml
version: 2

models:
  - name: dim_customers
    columns:
      - name: customer_id
        tests:
          - unique                    # Không trùng lặp
          - not_null                  # Không NULL
      - name: email
        tests:
          - unique
          - not_null
      - name: status
        tests:
          - accepted_values:          # Chỉ chấp nhận các giá trị này
              values: ['active', 'inactive', 'pending']
      - name: created_at
        tests:
          - not_null

sources:
  - name: raw_data
    tables:
      - name: customers
        columns:
          - name: id
            tests:
              - unique
              - not_null
```

### 2. **Singular Tests** (files .sql trong tests/)

Tests cụ thể cho từng trường hợp:

```sql
-- tests/singular/test_no_duplicate_customers.sql
-- Kiểm tra không có customer trùng lặp giữa source và target

SELECT 
    customer_id,
    COUNT(*) as duplicate_count
FROM (
    SELECT customer_id FROM {{ source('raw_data', 'source_customers') }}
    UNION ALL
    SELECT customer_id FROM {{ ref('dim_customers') }}
) combined_data
GROUP BY customer_id
HAVING COUNT(*) != 2  -- Mỗi customer phải xuất hiện đúng 2 lần
```

```sql
-- tests/singular/test_valid_phone_format.sql
-- Kiểm tra format số điện thoại

SELECT 
    customer_id,
    phone,
    'Invalid phone format' as error_message
FROM {{ ref('dim_customers') }}
WHERE phone IS NOT NULL
  AND (
    LENGTH(phone) < 10 
    OR LENGTH(phone) > 15
    OR REGEXP_LIKE(phone, '[^0-9]')  -- Chứa ký tự không phải số
  )
```

```sql
-- tests/singular/test_staging_mart_consistency.sql
-- Kiểm tra tính nhất quán giữa staging và mart

WITH staging_data AS (
    SELECT customer_id, full_name, phone, created_at
    FROM {{ ref('stg_customers') }}
),
mart_data AS (
    SELECT customer_id, full_name, phone, created_at
    FROM {{ ref('dim_customers') }}
),
differences AS (
    -- Tìm records chỉ có ở staging
    SELECT s.customer_id, 'staging_only' as difference_type
    FROM staging_data s
    LEFT JOIN mart_data m ON s.customer_id = m.customer_id
    WHERE m.customer_id IS NULL
    
    UNION ALL
    
    -- Tìm records chỉ có ở mart
    SELECT m.customer_id, 'mart_only' as difference_type
    FROM mart_data m
    LEFT JOIN staging_data s ON m.customer_id = s.customer_id
    WHERE s.customer_id IS NULL
    
    UNION ALL
    
    -- Tìm records có data khác nhau
    SELECT s.customer_id, 'data_mismatch' as difference_type
    FROM staging_data s
    JOIN mart_data m ON s.customer_id = m.customer_id
    WHERE s.full_name != m.full_name
       OR NVL(s.phone, 'NULL') != NVL(m.phone, 'NULL')
       OR s.created_at != m.created_at
)

SELECT * FROM differences
```

### 3. **Generic Tests** (Reusable tests)

Tests có thể tái sử dụng với tham số:

```sql
-- tests/generic/test_value_in_range.sql
-- Kiểm tra giá trị trong khoảng cho phép

{% test value_in_range(model, column_name, min_value, max_value) %}

    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL
      AND ({{ column_name }} < {{ min_value }} OR {{ column_name }} > {{ max_value }})

{% endtest %}
```

```sql
-- tests/generic/test_string_pattern.sql
-- Kiểm tra format string theo regex pattern

{% test string_pattern(model, column_name, pattern, pattern_name='') %}

    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL
      AND NOT REGEXP_LIKE({{ column_name }}, '{{ pattern }}')

{% endtest %}
```

```sql
-- tests/generic/test_not_empty_string.sql
-- Kiểm tra string không rỗng và không chỉ chứa khoảng trắng

{% test not_empty_string(model, column_name) %}

    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL
      AND (TRIM({{ column_name }}) = '' OR TRIM({{ column_name }}) IS NULL)

{% endtest %}
```

**Sử dụng Generic Tests:**

```yaml
# models/schema.yml
models:
  - name: dim_customers
    columns:
      - name: customer_id
        tests:
          - value_in_range:
              min_value: 1
              max_value: 999999
      - name: full_name
        tests:
          - not_empty_string
      - name: phone
        tests:
          - string_pattern:
              pattern: '^[0-9]{10,15}$'
              pattern_name: 'valid_phone_format'
```

### 4. **Business Rules Tests**

Tests kiểm tra quy tắc nghiệp vụ:

```sql
-- tests/business_rules/test_customer_name_no_numbers.sql
-- Business Rule: Tên khách hàng không được chứa số

SELECT 
    customer_id,
    full_name,
    'Customer name contains numbers' as violation_reason
FROM {{ ref('dim_customers') }}
WHERE REGEXP_LIKE(full_name, '[0-9]')
```

```sql
-- tests/business_rules/test_created_at_not_future.sql
-- Business Rule: Created_at không được là tương lai

SELECT 
    customer_id,
    created_at,
    'Created date is in the future' as violation_reason
FROM {{ ref('dim_customers') }}
WHERE created_at > CURRENT_TIMESTAMP
```

```sql
-- tests/business_rules/test_record_count_match.sql
-- Business Rule: Số lượng records phải khớp giữa source và target

WITH source_count AS (
    SELECT COUNT(*) as source_records
    FROM {{ source('raw_data', 'source_customers_test_dbt') }}
),
target_count AS (
    SELECT COUNT(*) as target_records  
    FROM {{ ref('dim_customers') }}
),
count_comparison AS (
    SELECT 
        source_records,
        target_records,
        ABS(source_records - target_records) as difference
    FROM source_count, target_count
)

SELECT 
    source_records,
    target_records,
    difference,
    'Record count mismatch between source and target' as violation_reason
FROM count_comparison
WHERE difference > 0
```

## 🚀 Chạy Tests

### Các lệnh cơ bản:

```bash
# Chạy tất cả tests
dbt test

# Chạy tests cho model cụ thể
dbt test --select dim_customers

# Chạy tests cho source
dbt test --select source:raw_data

# Chạy chỉ singular tests
dbt test --select test_type:singular

# Chạy chỉ generic tests  
dbt test --select test_type:generic

# Chạy tests với tag
dbt test --select tag:data_quality

# Chạy tests và show failures
dbt test --store-failures
```

### Test với tags:

```yaml
# models/schema.yml
models:
  - name: dim_customers
    tests:
      - unique:
          column_name: customer_id
          tags: ['data_quality', 'critical']
      - not_null:
          column_name: customer_id  
          tags: ['data_quality', 'critical']
```

```bash
# Chạy chỉ critical tests
dbt test --select tag:critical

# Chạy data quality tests
dbt test --select tag:data_quality
```

## 📊 Advanced Testing Strategies

### 1. **Data Freshness Tests**

```yaml
# models/schema.yml
sources:
  - name: raw_data
    tables:
      - name: customers
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}
        loaded_at_field: created_at
```

### 2. **Cross-Database Tests**

```sql
-- tests/singular/test_cross_database_consistency.sql
-- Kiểm tra tính nhất quán giữa các database

WITH prod_count AS (
    SELECT COUNT(*) as prod_records
    FROM prod_db.customers
),
staging_count AS (
    SELECT COUNT(*) as staging_records
    FROM staging_db.customers  
)

SELECT 
    prod_records,
    staging_records,
    ABS(prod_records - staging_records) as difference
FROM prod_count, staging_count
WHERE ABS(prod_records - staging_records) > 100  -- Threshold
```

### 3. **Performance Tests**

```sql
-- tests/singular/test_model_performance.sql
-- Kiểm tra performance của model

WITH model_stats AS (
    SELECT 
        COUNT(*) as record_count,
        COUNT(DISTINCT customer_id) as unique_customers,
        MAX(created_at) as latest_record
    FROM {{ ref('dim_customers') }}
)

SELECT *
FROM model_stats
WHERE record_count = 0  -- Model không được rỗng
   OR unique_customers != record_count  -- Không được có duplicate
   OR latest_record < CURRENT_DATE - 7  -- Data không được cũ quá 7 ngày
```

### 4. **Incremental Tests**

```sql
-- tests/singular/test_incremental_consistency.sql
-- Kiểm tra tính nhất quán của incremental model

{% if is_incremental() %}

WITH new_records AS (
    SELECT COUNT(*) as new_count
    FROM {{ this }}
    WHERE created_at >= (
        SELECT MAX(created_at) 
        FROM {{ this }} 
        WHERE created_at < CURRENT_DATE
    )
),
expected_records AS (
    SELECT COUNT(*) as expected_count
    FROM {{ source('raw_data', 'customers') }}
    WHERE created_at >= CURRENT_DATE - 1
)

SELECT 
    new_count,
    expected_count,
    ABS(new_count - expected_count) as difference
FROM new_records, expected_records
WHERE ABS(new_count - expected_count) > 10  -- Threshold

{% endif %}
```

## 🏆 Best Practices

### 1. **Test Organization**

```
tests/
├── critical/              # Critical tests (must pass)
│   ├── test_primary_keys.sql
│   └── test_data_integrity.sql
├── data_quality/          # Data quality tests
│   ├── test_formats.sql
│   └── test_completeness.sql
├── business_rules/        # Business logic tests
│   ├── test_customer_rules.sql
│   └── test_financial_rules.sql
└── performance/           # Performance tests
    ├── test_model_size.sql
    └── test_execution_time.sql
```

### 2. **Test Naming Convention**

```sql
-- ✅ GOOD - Descriptive names
test_no_duplicate_customer_ids.sql
test_valid_email_format.sql
test_customer_age_within_range.sql
test_order_total_matches_line_items.sql

-- ❌ BAD - Vague names
test1.sql
check_data.sql
validation.sql
test_stuff.sql
```

### 3. **Test Documentation**

```sql
-- tests/singular/test_customer_data_quality.sql
/*
Test: Customer Data Quality Validation

Purpose: 
  Ensures customer data meets quality standards before being used in downstream models

Checks:
  1. No duplicate customer IDs
  2. Valid email formats
  3. Phone numbers in correct format
  4. Names don't contain numbers
  5. Created dates are not in future

Frequency: Run on every dbt run
Criticality: High - Pipeline should fail if this test fails

Author: Data Team
Last Updated: 2024-01-01
*/

WITH quality_checks AS (
    SELECT 
        customer_id,
        CASE 
            WHEN email IS NOT NULL AND NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$') 
            THEN 'Invalid email format'
            WHEN phone IS NOT NULL AND NOT REGEXP_LIKE(phone, '^[0-9]{10,15}$')
            THEN 'Invalid phone format'  
            WHEN REGEXP_LIKE(full_name, '[0-9]')
            THEN 'Name contains numbers'
            WHEN created_at > CURRENT_TIMESTAMP
            THEN 'Future created date'
            ELSE NULL
        END as quality_issue
    FROM {{ ref('dim_customers') }}
)

SELECT 
    customer_id,
    quality_issue
FROM quality_checks
WHERE quality_issue IS NOT NULL
```

### 4. **Test Configuration**

```yaml
# dbt_project.yml
tests:
  aml_transaction_pipeline:
    +store_failures: true      # Lưu failed records
    +severity: error           # Default severity
    critical:
      +severity: error         # Critical tests must pass
    data_quality:
      +severity: warn          # Data quality tests can warn
    performance:
      +severity: warn
      +store_failures: false
```

### 5. **Test Macros for Reusability**

```sql
-- macros/test_helpers.sql

-- Macro để test data freshness
{% macro test_data_freshness(model, date_column, max_days_old=7) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ date_column }} < CURRENT_DATE - {{ max_days_old }}
{% endmacro %}

-- Macro để test record count trong khoảng
{% macro test_record_count_range(model, min_count=1, max_count=1000000) %}
    WITH record_count AS (
        SELECT COUNT(*) as total_records
        FROM {{ model }}
    )
    SELECT *
    FROM record_count
    WHERE total_records < {{ min_count }} 
       OR total_records > {{ max_count }}
{% endmacro %}

-- Macro để test referential integrity
{% macro test_referential_integrity(child_model, parent_model, foreign_key, primary_key) %}
    SELECT {{ foreign_key }}
    FROM {{ child_model }}
    WHERE {{ foreign_key }} IS NOT NULL
      AND {{ foreign_key }} NOT IN (
          SELECT {{ primary_key }}
          FROM {{ parent_model }}
          WHERE {{ primary_key }} IS NOT NULL
      )
{% endmacro %}
```

**Sử dụng test macros:**

```sql
-- tests/singular/test_customer_freshness.sql
{{ test_data_freshness(ref('dim_customers'), 'created_at', 30) }}

-- tests/singular/test_customer_count.sql  
{{ test_record_count_range(ref('dim_customers'), 100, 1000000) }}

-- tests/singular/test_order_customer_integrity.sql
{{ test_referential_integrity(ref('fct_orders'), ref('dim_customers'), 'customer_id', 'customer_id') }}
```

## 🚨 Troubleshooting

### Common Issues & Solutions

#### 1. **Test chạy chậm**

```sql
-- ❌ Slow test - Full table scan
SELECT * FROM large_table WHERE complex_condition

-- ✅ Fast test - Use sampling or indexing
SELECT * FROM (
    SELECT * FROM large_table 
    WHERE created_at >= CURRENT_DATE - 7  -- Filter first
) WHERE complex_condition
```

#### 2. **Test fails intermittently**

```sql
-- ❌ Time-dependent test
SELECT * FROM orders WHERE created_at = CURRENT_DATE

-- ✅ Stable test  
SELECT * FROM orders WHERE created_at >= CURRENT_DATE - 1
```

#### 3. **Too many test failures**

```yaml
# Use severity levels
tests:
  +severity: warn  # Don't fail pipeline, just warn

# Or use limits
tests:
  +limit: 100      # Only show first 100 failures
```

#### 4. **Tests not running**

```bash
# Check test compilation
dbt compile --select test_name

# Check test selection
dbt ls --select test_type:singular

# Run with debug
dbt test --debug
```

### Debug Tests

```sql
-- Macro để debug failed tests
{% macro debug_test_failures(test_name) %}
    {% set query %}
        SELECT * FROM {{ test_name }}
        WHERE 1=1  -- Your test condition
        FETCH FIRST 10 ROWS ONLY
    {% endset %}
    
    {% set results = run_query(query) %}
    {% if execute %}
        {{ log("=== DEBUG TEST FAILURES ===", info=True) }}
        {% for row in results %}
            {{ log("Failed record: " ~ row, info=True) }}
        {% endfor %}
    {% endif %}
{% endmacro %}
```

## 📈 Test Monitoring & Reporting

### 1. **Test Results Dashboard**

```sql
-- macros/test_reporting.sql
{% macro generate_test_report() %}
    {% set test_results_query %}
        SELECT 
            test_name,
            status,
            execution_time,
            failures,
            run_started_at
        FROM {{ target.schema }}.dbt_test_results
        WHERE run_started_at >= CURRENT_DATE - 7
        ORDER BY run_started_at DESC
    {% endset %}
    
    {% set results = run_query(test_results_query) %}
    {% if execute %}
        {{ log("=== TEST REPORT (Last 7 days) ===", info=True) }}
        {% for row in results %}
            {{ log(row[0] ~ " | " ~ row[1] ~ " | " ~ row[2] ~ "s | " ~ row[3] ~ " failures", info=True) }}
        {% endfor %}
    {% endif %}
{% endmacro %}
```

### 2. **Automated Test Alerts**

```yaml
# .github/workflows/dbt_tests.yml
name: dbt Tests
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run dbt tests
        run: |
          dbt test --store-failures
          if [ $? -ne 0 ]; then
            echo "Tests failed! Sending alert..."
            # Send Slack/email notification
          fi
```

## 🎯 Test Strategy by Environment

### Development
```bash
# Quick tests for development
dbt test --select tag:critical
dbt test --select models:staging
```

### Staging  
```bash
# Comprehensive testing
dbt test --store-failures
dbt test --select tag:data_quality
```

### Production
```bash
# Critical tests only
dbt test --select tag:critical,tag:business_rules
dbt test --store-failures --threads 4
```

## 📚 Tài liệu tham khảo

### Official Documentation
- [dbt Testing Guide](https://docs.getdbt.com/docs/build/tests)
- [Test Configurations](https://docs.getdbt.com/reference/test-configs)
- [Custom Generic Tests](https://docs.getdbt.com/guides/best-practices/writing-custom-generic-tests)

### Useful Packages
```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_expectations
    version: 0.10.1
  - package: dbt-labs/dbt_utils
    version: 1.1.1
  - package: calogica/dbt_expectations
    version: 0.10.1
```

### dbt_expectations Examples
```yaml
models:
  - name: dim_customers
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1000
          max_value: 100000
      - dbt_expectations.expect_column_values_to_match_regex:
          column_name: email
          regex: '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
```

---

## 🎯 Kết luận

**dbt Tests là foundation của Data Quality** trong modern data stack:

- ✅ **Prevent bad data** từ việc lan truyền downstream
- ✅ **Automate quality checks** thay vì manual validation  
- ✅ **Document business rules** trong code
- ✅ **Build confidence** trong data pipeline
- ✅ **Enable continuous integration** cho data

**Test Strategy:**
1. **Start simple** - Built-in tests (unique, not_null)
2. **Add business rules** - Singular tests cho logic cụ thể
3. **Create reusable tests** - Generic tests cho patterns chung
4. **Monitor and alert** - Automated testing trong CI/CD
5. **Iterate and improve** - Continuous improvement

**Remember:** Good tests = Reliable data = Better business decisions! 🎯

---

**Tác giả:** VPAURA  
**Ngày:** 2025-12-30  
**Slogan:** `Code chay như muối bỏ bể, nhờ VPAura thì ngon luôn` 🚀
