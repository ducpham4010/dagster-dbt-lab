# dbt Macros - Hướng dẫn sử dụng toàn diện

## 📋 Mục lục

1. [Giới thiệu về Macros](#giới-thiệu-về-macros)
2. [Cú pháp cơ bản](#cú-pháp-cơ-bản)
3. [Các loại Macros](#các-loại-macros)
4. [Ví dụ thực tế](#ví-dụ-thực-tế)
5. [Best Practices](#best-practices)
6. [Troubleshooting](#troubleshooting)
7. [Tài liệu tham khảo](#tài-liệu-tham-khảo)

## 🎯 Giới thiệu về Macros

### Macros là gì?

**Macros** trong dbt là các **functions tái sử dụng** được viết bằng **Jinja templating language**, cho phép bạn:

- 🔄 **Tái sử dụng code** - Tránh lặp lại logic
- 🤖 **Tự động hóa tasks** - Setup, backup, analyze
- 🎛️ **Tạo SQL động** - Logic có điều kiện
- 🏗️ **Mở rộng dbt** - Custom operations
- 📊 **Centralize business logic** - Dễ bảo trì

### Tại sao cần Macros?

```sql
-- ❌ TRƯỚC KHI CÓ MACROS - Lặp lại code
-- File 1
SELECT 
  TRIM(full_name) as full_name,
  CASE WHEN TRIM(phone) = '' THEN NULL ELSE TRIM(phone) END as phone
FROM customers_table1;

-- File 2  
SELECT 
  TRIM(full_name) as full_name,
  CASE WHEN TRIM(phone) = '' THEN NULL ELSE TRIM(phone) END as phone
FROM customers_table2;

-- ✅ SAU KHI CÓ MACROS - Tái sử dụng
-- File 1
SELECT {{ clean_name('full_name') }}, {{ clean_phone('phone') }} FROM customers_table1;

-- File 2
SELECT {{ clean_name('full_name') }}, {{ clean_phone('phone') }} FROM customers_table2;
```

## 🔧 Cú pháp cơ bản

### 1. Định nghĩa Macro

```sql
{% macro tên_macro(tham_số1, tham_số2='giá_trị_mặc_định') %}
  {# Comment: Mô tả macro #}
  
  -- SQL logic ở đây
  SELECT {{ tham_số1 }} FROM {{ tham_số2 }}
  WHERE created_at >= CURRENT_DATE - 30
  
{% endmacro %}
```

### 2. Sử dụng Macro

```sql
-- Trong model (.sql file)
{{ tên_macro('column_name', 'table_name') }}

-- Với tham số mặc định
{{ tên_macro('column_name') }}

-- Chạy operation macro
-- dbt run-operation tên_macro --args '{\"param1\": \"value1\"}'
```

### 3. Cấu trúc thư mục

```
dbt_project/
├── macros/
│   ├── data_cleaning.sql      # Macros làm sạch dữ liệu
│   ├── operations.sql         # Macros thực thi tác vụ
│   ├── utilities.sql          # Macros tiện ích
│   └── business_logic.sql     # Macros business logic
└── models/
    └── ...
```

## 📚 Các loại Macros

### 1. 🧹 Data Cleaning Macros

```sql
-- Làm sạch số điện thoại
{% macro clean_phone_number(phone_column) %}
  CASE 
    WHEN {{ phone_column }} IS NULL THEN NULL
    WHEN TRIM({{ phone_column }}) = '' THEN NULL
    WHEN LENGTH(TRIM({{ phone_column }})) < 10 THEN NULL
    ELSE REGEXP_REPLACE(TRIM({{ phone_column }}), '[^0-9]', '')
  END
{% endmacro %}

-- Làm sạch tên
{% macro clean_name(name_column) %}
  CASE 
    WHEN {{ name_column }} IS NULL THEN NULL
    WHEN TRIM({{ name_column }}) = '' THEN NULL
    ELSE INITCAP(TRIM({{ name_column }}))
  END
{% endmacro %}

-- Làm sạch email
{% macro clean_email(email_column) %}
  CASE 
    WHEN {{ email_column }} IS NULL THEN NULL
    WHEN TRIM(LOWER({{ email_column }})) = '' THEN NULL
    WHEN INSTR(TRIM(LOWER({{ email_column }})), '@') = 0 THEN NULL
    ELSE TRIM(LOWER({{ email_column }}))
  END
{% endmacro %}

-- Cách sử dụng
SELECT 
  customer_id,
  {{ clean_name('full_name') }} as full_name,
  {{ clean_phone_number('phone') }} as phone,
  {{ clean_email('email') }} as email
FROM raw_customers;
```

### 2. 📊 Business Logic Macros

```sql
-- Tính tuổi từ ngày sinh
{% macro calculate_age(birth_date_column) %}
  FLOOR(MONTHS_BETWEEN(CURRENT_DATE, {{ birth_date_column }}) / 12)
{% endmacro %}

-- Phân loại khách hàng theo tuổi
{% macro customer_age_segment(age_column) %}
  CASE 
    WHEN {{ age_column }} < 18 THEN 'Under 18'
    WHEN {{ age_column }} BETWEEN 18 AND 25 THEN 'Young Adult'
    WHEN {{ age_column }} BETWEEN 26 AND 40 THEN 'Adult'
    WHEN {{ age_column }} BETWEEN 41 AND 60 THEN 'Middle Age'
    WHEN {{ age_column }} > 60 THEN 'Senior'
    ELSE 'Unknown'
  END
{% endmacro %}

-- Tính revenue tier
{% macro revenue_tier(amount_column) %}
  CASE 
    WHEN {{ amount_column }} < 1000000 THEN 'Bronze'
    WHEN {{ amount_column }} < 5000000 THEN 'Silver'  
    WHEN {{ amount_column }} < 10000000 THEN 'Gold'
    ELSE 'Platinum'
  END
{% endmacro %}

-- Cách sử dụng
SELECT 
  customer_id,
  birth_date,
  {{ calculate_age('birth_date') }} as age,
  {{ customer_age_segment(calculate_age('birth_date')) }} as age_segment,
  total_revenue,
  {{ revenue_tier('total_revenue') }} as customer_tier
FROM customer_data;
```

### 3. 🔄 Utility Macros

```sql
-- Tạo audit columns
{% macro add_audit_columns() %}
  CURRENT_TIMESTAMP as created_at,
  USER as created_by,
  CURRENT_TIMESTAMP as updated_at,
  USER as updated_by
{% endmacro %}

-- Safe division (tránh chia cho 0)
{% macro safe_divide(numerator, denominator) %}
  CASE 
    WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL THEN NULL
    ELSE {{ numerator }} / {{ denominator }}
  END
{% endmacro %}

-- Generate date spine
{% macro generate_date_spine(start_date, end_date) %}
  WITH date_spine AS (
    SELECT 
      DATE '{{ start_date }}' + LEVEL - 1 as date_day
    FROM dual
    CONNECT BY LEVEL <= (DATE '{{ end_date }}' - DATE '{{ start_date }}' + 1)
  )
  SELECT * FROM date_spine
{% endmacro %}

-- Cách sử dụng
SELECT 
  sales_amount,
  quantity,
  {{ safe_divide('sales_amount', 'quantity') }} as unit_price,
  {{ add_audit_columns() }}
FROM sales_data;
```

### 4. 🛠️ Operation Macros

```sql
-- Backup table
{% macro backup_table(source_table, backup_suffix='_backup') %}
  {% set backup_table_name = source_table ~ backup_suffix ~ '_' ~ modules.datetime.datetime.now().strftime('%Y%m%d_%H%M%S') %}
  
  {% set backup_sql %}
    CREATE TABLE {{ backup_table_name }} AS 
    SELECT * FROM {{ source_table }}
  {% endset %}
  
  {% do run_query(backup_sql) %}
  {{ log(\"Created backup table: \" ~ backup_table_name, info=True) }}
  
  {% set count_sql %}
    SELECT COUNT(*) as record_count FROM {{ backup_table_name }}
  {% endset %}
  
  {% set results = run_query(count_sql) %}
  {% if execute %}
    {{ log(\"Backup contains \" ~ results.columns[0].values()[0] ~ \" records\", info=True) }}
  {% endif %}
{% endmacro %}

-- Drop table if exists
{% macro drop_table_if_exists(table_name) %}
  {% set drop_sql %}
    BEGIN
      EXECUTE IMMEDIATE 'DROP TABLE {{ table_name }}';
    EXCEPTION
      WHEN OTHERS THEN
        IF SQLCODE != -942 THEN -- Table doesn't exist
          RAISE;
        END IF;
    END;
  {% endset %}
  
  {% do run_query(drop_sql) %}
  {{ log(\"Dropped table \" ~ table_name ~ \" if it existed\", info=True) }}
{% endmacro %}

-- Analyze table statistics
{% macro analyze_table_stats(table_name) %}
  {% set stats_sql %}
    SELECT 
      '{{ table_name }}' as table_name,
      COUNT(*) as total_records,
      COUNT(DISTINCT customer_id) as unique_customers,
      MIN(created_at) as earliest_record,
      MAX(created_at) as latest_record
    FROM {{ table_name }}
  {% endset %}
  
  {% set results = run_query(stats_sql) %}
  {% if execute %}
    {% for row in results %}
      {{ log(\"=== TABLE STATISTICS ===\", info=True) }}
      {{ log(\"Table: \" ~ row[0], info=True) }}
      {{ log(\"Total Records: \" ~ row[1], info=True) }}
      {{ log(\"Unique Customers: \" ~ row[2], info=True) }}
      {{ log(\"Date Range: \" ~ row[3] ~ \" to \" ~ row[4], info=True) }}
    {% endfor %}
  {% endif %}
{% endmacro %}

-- Cách sử dụng
-- dbt run-operation backup_table --args '{\"source_table\": \"customers\"}'
-- dbt run-operation analyze_table_stats --args '{\"table_name\": \"customers\"}'
-- dbt run-operation drop_table_if_exists --args '{\"table_name\": \"old_table\"}'
```

### 5. 🎯 Advanced Macros

```sql
-- Dynamic pivot table
{% macro pivot_table(table_name, group_by_col, pivot_col, value_col, agg_func='SUM') %}
  {% set pivot_values_query %}
    SELECT DISTINCT {{ pivot_col }} 
    FROM {{ table_name }}
    WHERE {{ pivot_col }} IS NOT NULL
    ORDER BY {{ pivot_col }}
  {% endset %}
  
  {% set results = run_query(pivot_values_query) %}
  {% if execute %}
    {% set pivot_values = results.columns[0].values() %}
  {% else %}
    {% set pivot_values = [] %}
  {% endif %}
  
  SELECT 
    {{ group_by_col }},
    {% for value in pivot_values %}
    {{ agg_func }}(CASE WHEN {{ pivot_col }} = '{{ value }}' THEN {{ value_col }} END) as {{ value | replace(' ', '_') | replace('-', '_') | lower }}
    {%- if not loop.last -%},{%- endif %}
    {% endfor %}
  FROM {{ table_name }}
  GROUP BY {{ group_by_col }}
{% endmacro %}

-- Generate test data
{% macro generate_test_customers(num_records=10) %}
  {% for i in range(num_records) %}
    SELECT 
      {{ i + 1 }} as customer_id,
      'Customer ' || {{ i + 1 }} as full_name,
      '090' || LPAD({{ i + 1 }}, 7, '0') as phone,
      CURRENT_TIMESTAMP - {{ i }} as created_at
    FROM dual
    {% if not loop.last %}
    UNION ALL
    {% endif %}
  {% endfor %}
{% endmacro %}

-- Conditional materialization
{% macro smart_materialization(table_name, threshold=1000000) %}
  {% set count_sql %}
    SELECT COUNT(*) as record_count FROM {{ table_name }}
  {% endset %}
  
  {% set results = run_query(count_sql) %}
  {% if execute %}
    {% set record_count = results.columns[0].values()[0] %}
    {% if record_count > threshold %}
      {{ return('table') }}
    {% else %}
      {{ return('view') }}
    {% endif %}
  {% else %}
    {{ return('view') }}
  {% endif %}
{% endmacro %}

-- Cách sử dụng
-- {{ config(materialized=smart_materialization('source_table', 500000)) }}
```

## 🎯 Ví dụ thực tế

### Scenario: Customer Data Pipeline

```sql
-- macros/customer_processing.sql
{% macro process_customer_data(source_table) %}
  SELECT 
    customer_id,
    {{ clean_name('full_name') }} as full_name,
    {{ clean_phone_number('phone') }} as phone,
    {{ clean_email('email') }} as email,
    birth_date,
    {{ calculate_age('birth_date') }} as age,
    {{ customer_age_segment(calculate_age('birth_date')) }} as age_segment,
    total_revenue,
    {{ revenue_tier('total_revenue') }} as customer_tier,
    {{ add_audit_columns() }}
  FROM {{ source_table }}
  WHERE {{ clean_name('full_name') }} IS NOT NULL
{% endmacro %}

-- models/staging/stg_customers.sql
{{ config(materialized='view') }}

{{ process_customer_data(source('raw_data', 'customers')) }}

-- models/marts/dim_customers.sql
{{ config(materialized='table') }}

WITH processed_customers AS (
  {{ process_customer_data(ref('stg_customers')) }}
)

SELECT * FROM processed_customers
```

### Scenario: Data Quality Checks

```sql
-- macros/data_quality.sql
{% macro data_quality_check(table_name, primary_key) %}
  {% set quality_checks = [
    ('total_records', 'SELECT COUNT(*) FROM ' ~ table_name),
    ('null_primary_keys', 'SELECT COUNT(*) FROM ' ~ table_name ~ ' WHERE ' ~ primary_key ~ ' IS NULL'),
    ('duplicate_primary_keys', 'SELECT COUNT(*) - COUNT(DISTINCT ' ~ primary_key ~ ') FROM ' ~ table_name),
    ('empty_names', 'SELECT COUNT(*) FROM ' ~ table_name ~ ' WHERE TRIM(full_name) IS NULL OR TRIM(full_name) = \\'\\'')
  ] %}
  
  {{ log(\"=== DATA QUALITY REPORT FOR \" ~ table_name ~ \" ===\", info=True) }}
  
  {% for check_name, check_sql in quality_checks %}
    {% set results = run_query(check_sql) %}
    {% if execute %}
      {% set value = results.columns[0].values()[0] %}
      {{ log(check_name ~ \": \" ~ value, info=True) }}
      
      {% if check_name in ['null_primary_keys', 'duplicate_primary_keys', 'empty_names'] and value > 0 %}
        {{ log(\"⚠️  WARNING: \" ~ check_name ~ \" found!\", info=True) }}
      {% endif %}
    {% endif %}
  {% endfor %}
{% endmacro %}

-- Cách sử dụng
-- dbt run-operation data_quality_check --args '{\"table_name\": \"dim_customers\", \"primary_key\": \"customer_id\"}'
```

## 🏆 Best Practices

### 1. 📝 Naming Convention

```sql
-- ✅ GOOD - Descriptive names
{% macro clean_phone_number() %}
{% macro calculate_customer_age() %}
{% macro generate_audit_columns() %}
{% macro backup_table_with_timestamp() %}

-- ❌ BAD - Vague names
{% macro macro1() %}
{% macro helper() %}
{% macro do_stuff() %}
{% macro process() %}
```

### 2. 📚 Documentation

```sql
{% macro clean_phone_number(phone_column) %}
  {#
    Cleans and standardizes phone numbers
    
    This macro performs the following operations:
    - Removes all non-numeric characters
    - Converts empty strings to NULL
    - Validates minimum length (10 digits)
    - Returns NULL for invalid numbers
    
    Args:
      phone_column (str): Name of the phone column to clean
      
    Returns:
      str: Cleaned phone number in format '0901234567' or NULL
      
    Example:
      {{ clean_phone_number('customer_phone') }}
      
    Test cases:
      - '(090) 123-4567' → '0901234567'
      - '090.123.4567' → '0901234567'  
      - '123' → NULL (too short)
      - '' → NULL
      - NULL → NULL
  #}
  
  CASE 
    WHEN {{ phone_column }} IS NULL THEN NULL
    WHEN TRIM({{ phone_column }}) = '' THEN NULL
    WHEN LENGTH(REGEXP_REPLACE(TRIM({{ phone_column }}), '[^0-9]', '')) < 10 THEN NULL
    ELSE REGEXP_REPLACE(TRIM({{ phone_column }}), '[^0-9]', '')
  END
{% endmacro %}
```

### 3. 🛡️ Error Handling

```sql
{% macro safe_operation(table_name, operation='SELECT') %}
  {% if table_name is none or table_name == '' %}
    {{ log(\"Error: table_name cannot be empty\", info=True) }}
    {{ return('') }}
  {% endif %}
  
  {% set check_table_sql %}
    SELECT COUNT(*) FROM user_tables WHERE table_name = UPPER('{{ table_name }}')
  {% endset %}
  
  {% set results = run_query(check_table_sql) %}
  {% if execute %}
    {% if results.columns[0].values()[0] == 0 %}
      {{ log(\"Error: Table \" ~ table_name ~ \" does not exist\", info=True) }}
      {{ return('') }}
    {% endif %}
  {% endif %}
  
  -- Proceed with operation
  {{ operation }} * FROM {{ table_name }}
{% endmacro %}
```

### 4. 🔧 Parameterization

```sql
{% macro flexible_date_filter(date_column, filter_type='last_30_days', custom_date=none) %}
  {% if filter_type == 'last_30_days' %}
    {{ date_column }} >= CURRENT_DATE - 30
  {% elif filter_type == 'last_7_days' %}
    {{ date_column }} >= CURRENT_DATE - 7
  {% elif filter_type == 'current_month' %}
    {{ date_column }} >= TRUNC(CURRENT_DATE, 'MM')
  {% elif filter_type == 'custom' and custom_date %}
    {{ date_column }} >= DATE '{{ custom_date }}'
  {% else %}
    1=1  -- No filter
  {% endif %}
{% endmacro %}

-- Cách sử dụng
SELECT * FROM customers 
WHERE {{ flexible_date_filter('created_at', 'last_7_days') }};

SELECT * FROM customers 
WHERE {{ flexible_date_filter('created_at', 'custom', '2024-01-01') }};
```

### 5. 🧪 Testing Macros

```sql
-- macros/test_macros.sql
{% macro test_clean_phone_number() %}
  {% set test_cases = [
    ('(090) 123-4567', '0901234567'),
    ('090.123.4567', '0901234567'),
    ('090-123-4567', '0901234567'),
    ('123', null),
    ('', null),
    (null, null)
  ] %}
  
  WITH test_data AS (
    {% for input_val, expected in test_cases %}
      SELECT 
        '{{ input_val }}' as input_value,
        '{{ expected }}' as expected_output,
        {{ clean_phone_number(\"'\" ~ input_val ~ \"'\") }} as actual_output
      FROM dual
      {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
  ),
  
  test_results AS (
    SELECT 
      input_value,
      expected_output,
      actual_output,
      CASE 
        WHEN expected_output = actual_output THEN 'PASS'
        WHEN expected_output IS NULL AND actual_output IS NULL THEN 'PASS'
        ELSE 'FAIL'
      END as test_result
    FROM test_data
  )
  
  SELECT * FROM test_results
{% endmacro %}

-- Chạy test
-- dbt run-operation test_clean_phone_number
```

## 🚨 Troubleshooting

### Common Issues & Solutions

#### 1. **Macro không được tìm thấy**

```
Error: Macro 'my_macro' is undefined
```

**Giải pháp:**
- Kiểm tra file macro có trong thư mục `macros/`
- Đảm bảo syntax đúng: `{% macro %}...{% endmacro %}`
- Chạy `dbt parse` để refresh

#### 2. **Lỗi Jinja syntax**

```
Error: expected token 'end of statement block', got 'endmacro'
```

**Giải pháp:**
- Kiểm tra đóng mở `{% %}` đúng cách
- Đảm bảo indentation nhất quán
- Kiểm tra quotes và commas

#### 3. **Macro chạy chậm**

**Giải pháp:**
- Tránh `run_query()` trong loops lớn
- Cache kết quả với `{% set %}`
- Sử dụng `{% if execute %}` để tránh chạy khi compile

#### 4. **Lỗi Oracle-specific**

```
ORA-00942: table or view does not exist
```

**Giải pháp:**
- Kiểm tra schema/table name trong macro
- Sử dụng `{{ this }}` hoặc `{{ ref() }}` thay vì hard-code
- Test macro với dữ liệu thật

### Debug Tips

```sql
-- 1. Logging để debug
{% macro debug_macro(value) %}
  {{ log(\"Debug value: \" ~ value, info=True) }}
  {{ return(value) }}
{% endmacro %}

-- 2. Conditional execution
{% macro safe_macro() %}
  {% if execute %}
    -- Chỉ chạy khi thực thi thật
    {% set results = run_query('SELECT 1 FROM dual') %}
  {% endif %}
{% endmacro %}

-- 3. Print compiled SQL
{% macro show_compiled_sql(sql) %}
  {{ log(\"Compiled SQL: \" ~ sql, info=True) }}
  {{ return(sql) }}
{% endmacro %}
```

## 📖 Tài liệu tham khảo

### Official Documentation
- [dbt Jinja Functions](https://docs.getdbt.com/reference/dbt-jinja-functions)
- [dbt Macros Guide](https://docs.getdbt.com/docs/build/jinja-macros)
- [Jinja Template Designer](https://jinja.palletsprojects.com/templates/)

### Useful dbt Packages
```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
  - package: calogica/dbt_expectations  
    version: 0.10.1
  - package: dbt-labs/audit_helper
    version: 0.9.0
```

### Common dbt_utils Macros
```sql
-- Generate surrogate key
{{ dbt_utils.generate_surrogate_key(['customer_id', 'order_date']) }}

-- Pivot table
{{ dbt_utils.pivot('status', dbt_utils.get_column_values(ref('orders'), 'status')) }}

-- Date spine
{{ dbt_utils.date_spine(datepart='day', start_date='2024-01-01', end_date='2024-12-31') }}

-- Get column values
{% set payment_methods = dbt_utils.get_column_values(ref('payments'), 'payment_method') %}
```

### Performance Tips
- Sử dụng `{% if execute %}` cho expensive operations
- Cache results với `{% set %}`
- Tránh nested loops với `run_query()`
- Test macros với sample data trước

---

## 🎯 Kết luận

**Macros là công cụ mạnh mẽ nhất trong dbt** giúp bạn:

- ✅ **Tái sử dụng code hiệu quả** - DRY principle
- ✅ **Tự động hóa workflows** - Operations & maintenance  
- ✅ **Standardize business logic** - Consistent across models
- ✅ **Extend dbt capabilities** - Custom functionality
- ✅ **Improve maintainability** - Central logic management

**Hãy bắt đầu với macros đơn giản và dần phát triển thành các solutions phức tạp hơn!**

---

**Tác giả:** VPAURA  
**Ngày:** 2025-12-30  
**Slogan:** `Code chay như muối bỏ bể, nhờ VPAura thì ngon luôn` 🚀
