# dbt schema.yml - Hướng dẫn toàn diện

## 📋 Mục lục

1. [Giới thiệu về schema.yml](#giới-thiệu-về-schemayml)
2. [Cấu trúc cơ bản](#cấu-trúc-cơ-bản)
3. [Sources Configuration](#sources-configuration)
4. [Models Configuration](#models-configuration)
5. [Tests Configuration](#tests-configuration)
6. [Documentation](#documentation)
7. [Advanced Features](#advanced-features)
8. [Best Practices](#best-practices)
9. [Examples](#examples)

## 🎯 Giới thiệu về schema.yml

### schema.yml là gì?

**`schema.yml`** là file **metadata configuration** trong dbt dùng để:

- 📋 **Define Sources** - Định nghĩa bảng nguồn từ database
- 🏗️ **Describe Models** - Mô tả models và columns
- 🧪 **Configure Tests** - Thiết lập data quality tests
- 📚 **Documentation** - Tạo documentation tự động
- 🔧 **Model Settings** - Cấu hình materialization, tags, etc.

### Tại sao cần schema.yml?

- ✅ **Data Quality** - Automated testing
- ✅ **Documentation** - Self-documenting code
- ✅ **Data Lineage** - Track data flow
- ✅ **Collaboration** - Team understanding
- ✅ **Governance** - Data contracts

## 🏗️ Cấu trúc cơ bản

```yaml
version: 2

# Sources - Bảng nguồn từ database
sources:
  - name: source_group_name
    tables:
      - name: table_name

# Models - dbt models
models:
  - name: model_name
    columns:
      - name: column_name

# Snapshots - SCD tracking
snapshots:
  - name: snapshot_name

# Seeds - CSV data
seeds:
  - name: seed_name

# Analyses - Ad-hoc queries
analyses:
  - name: analysis_name

# Macros - Reusable functions
macros:
  - name: macro_name
```

## 📊 Sources Configuration

### Cơ bản

```yaml
version: 2

sources:
  - name: raw_data                    # Source group name
    description: \"Raw source tables\"
    schema: source_schema_name        # Database schema
    database: source_database_name    # Database name (optional)
    
    tables:
      - name: customers               # Table name
        description: \"Customer data\"
        
        columns:
          - name: customer_id
            description: \"Primary key\"
            tests:
              - unique
              - not_null
          - name: email
            description: \"Customer email\"
            tests:
              - unique
              - not_null
```

### Advanced Sources

```yaml
sources:
  - name: raw_data
    description: \"Raw source tables\"
    schema: \"{{ env_var('SOURCE_SCHEMA', 'raw') }}\"
    
    # Source-level configurations
    meta:
      owner: \"data-team@company.com\"
      refresh_frequency: \"daily\"
    
    # Data freshness monitoring
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    
    # Source-level tests
    tests:
      - dbt_utils.expression_is_true:
          expression: \"count(*) > 0\"
    
    tables:
      - name: customers
        description: \"Customer master data\"
        
        # Table-specific freshness
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {count: 12, period: hour}
        loaded_at_field: updated_at
        
        # External table configuration
        external:
          location: \"s3://bucket/path/\"
          file_format: parquet
        
        columns:
          - name: customer_id
            description: \"Unique customer identifier\"
            tests:
              - unique
              - not_null
              - relationships:
                  to: ref('dim_customers')
                  field: customer_id
          
          - name: email
            description: \"Customer email address\"
            tests:
              - unique
              - not_null
              - dbt_expectations.expect_column_values_to_match_regex:
                  regex: '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
          
          - name: status
            description: \"Customer status\"
            tests:
              - accepted_values:
                  values: ['active', 'inactive', 'pending']
          
          - name: created_at
            description: \"Record creation timestamp\"
            tests:
              - not_null
              - dbt_expectations.expect_column_values_to_be_of_type:
                  column_type: timestamp
```

## 🏗️ Models Configuration

### Cơ bản

```yaml
version: 2

models:
  - name: dim_customers
    description: \"Customer dimension table\"
    
    # Model-level configurations
    config:
      materialized: table
      indexes:
        - columns: ['customer_id']
          unique: true
    
    columns:
      - name: customer_id
        description: \"Primary key\"
        tests:
          - unique
          - not_null
      
      - name: full_name
        description: \"Customer full name\"
        tests:
          - not_null
```

### Advanced Models

```yaml
models:
  - name: dim_customers
    description: \"Customer dimension table with SCD Type 2\"
    
    # Model metadata
    meta:
      owner: \"analytics-team\"
      maturity: \"production\"
      update_frequency: \"daily\"
    
    # Model configuration
    config:
      materialized: table
      post-hook: \"ANALYZE TABLE {{ this }} COMPUTE STATISTICS\"
      tags: [\"dimension\", \"daily\", \"pii\"]
      
    # Model-level tests
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - customer_id
            - valid_from
      - dbt_utils.expression_is_true:
          expression: \"valid_from <= valid_to\"
    
    columns:
      - name: customer_id
        description: \"Business key for customer\"
        tests:
          - not_null
          - relationships:
              to: source('raw_data', 'customers')
              field: customer_id
      
      - name: customer_key
        description: \"Surrogate key\"
        tests:
          - unique
          - not_null
      
      - name: full_name
        description: \"Customer full name\"
        tests:
          - not_null
          - dbt_expectations.expect_column_value_lengths_to_be_between:
              min_value: 2
              max_value: 100
      
      - name: email
        description: \"Customer email\"
        tests:
          - unique
          - not_null
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
      
      - name: phone
        description: \"Customer phone number\"
        tests:
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: '^[0-9]{10,15}$'
      
      - name: birth_date
        description: \"Customer birth date\"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: \"'1900-01-01'\"
              max_value: \"current_date\"
      
      - name: valid_from
        description: \"SCD valid from date\"
        tests:
          - not_null
      
      - name: valid_to
        description: \"SCD valid to date\"
      
      - name: is_current
        description: \"Current record flag\"
        tests:
          - accepted_values:
              values: [true, false]
```

## 🧪 Tests Configuration

### Built-in Tests

```yaml
columns:
  - name: customer_id
    tests:
      # Uniqueness test
      - unique
      
      # Not null test
      - not_null
      
      # Accepted values test
      - accepted_values:
          values: ['A', 'B', 'C']
      
      # Relationships test (foreign key)
      - relationships:
          to: ref('dim_customers')
          field: customer_id
```

### Custom Generic Tests

```yaml
columns:
  - name: customer_id
    tests:
      # Custom range test
      - value_in_range:
          min_value: 1
          max_value: 999999
      
      # Custom pattern test
      - string_pattern:
          pattern: '^[0-9]{10,15}$'
          pattern_name: 'valid_phone_format'
      
      # Custom not empty test
      - not_empty_string
```

### dbt_expectations Tests

```yaml
columns:
  - name: email
    tests:
      # Regex pattern matching
      - dbt_expectations.expect_column_values_to_match_regex:
          regex: '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
      
      # Value length validation
      - dbt_expectations.expect_column_value_lengths_to_be_between:
          min_value: 5
          max_value: 100
      
      # Data type validation
      - dbt_expectations.expect_column_values_to_be_of_type:
          column_type: varchar
      
      # Range validation
      - dbt_expectations.expect_column_values_to_be_between:
          min_value: 0
          max_value: 100
      
      # Set membership
      - dbt_expectations.expect_column_values_to_be_in_set:
          value_set: ['active', 'inactive', 'pending']
```

### Model-level Tests

```yaml
models:
  - name: dim_customers
    tests:
      # Unique combination test
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - customer_id
            - valid_from
      
      # Expression test
      - dbt_utils.expression_is_true:
          expression: \"count(*) > 0\"
      
      # Equality test
      - dbt_utils.equality:
          compare_model: ref('customers_backup')
      
      # Recency test
      - dbt_utils.recency:
          datepart: day
          field: created_at
          interval: 1
```

## 📚 Documentation

### Column Documentation

```yaml
columns:
  - name: customer_id
    description: |
      **Primary Key**: Unique identifier for each customer
      
      - Generated automatically by the system
      - Used for joining with other tables
      - Never null or duplicate
      
      **Business Rules:**
      - Sequential numbering starting from 1
      - No gaps allowed in production
      
      **Data Quality:**
      - Must be unique across all records
      - Cannot be null
      - Must be positive integer
    
    meta:
      dimension_type: \"primary_key\"
      pii: false
      business_owner: \"sales-team\"
```

### Model Documentation

```yaml
models:
  - name: dim_customers
    description: |
      # Customer Dimension Table
      
      This table contains the master customer data used across all analytics.
      
      ## Business Logic
      - One record per customer
      - Includes current and historical data
      - Updated daily from source systems
      
      ## Data Sources
      - Primary: `raw_data.customers`
      - Secondary: `raw_data.customer_profiles`
      
      ## Update Schedule
      - **Frequency**: Daily at 2 AM
      - **Dependencies**: Source data refresh
      - **SLA**: Available by 6 AM
      
      ## Usage Guidelines
      - Use `customer_key` for joins (surrogate key)
      - Filter by `is_current = true` for active records
      - Check `valid_from`/`valid_to` for historical analysis
      
      ## Contact
      - **Owner**: Analytics Team
      - **Email**: analytics@company.com
      - **Slack**: #data-team
    
    meta:
      owner: \"analytics-team\"
      maturity: \"production\"
      update_frequency: \"daily\"
      sla_hours: 4
      business_critical: true
```

## 🔧 Advanced Features

### Environment-specific Configuration

```yaml
models:
  - name: dim_customers
    description: \"Customer dimension\"
    config:
      # Environment-specific materialization
      materialized: \"{{ 'table' if target.name == 'prod' else 'view' }}\"
      
      # Environment-specific tags
      tags: |
        {%- if target.name == 'prod' -%}
          [\"production\", \"critical\"]
        {%- else -%}
          [\"development\"]
        {%- endif -%}
```

### Conditional Tests

```yaml
columns:
  - name: customer_id
    tests:
      - unique
      - not_null
      # Only run in production
      - relationships:
          to: ref('customers_master')
          field: id
          config:
            enabled: \"{{ target.name == 'prod' }}\"
```

### Dynamic Documentation

```yaml
models:
  - name: \"{{ var('customer_table_name', 'dim_customers') }}\"
    description: \"Customer data for {{ target.name }} environment\"
    
    columns:
      - name: customer_id
        description: \"Customer ID (Environment: {{ target.name }})\"
```

### Macros in schema.yml

```yaml
models:
  - name: dim_customers
    tests:
      # Custom macro test
      - \"{{ test_row_count_matches_source('customers') }}\"
      
      # Macro with parameters
      - \"{{ validate_business_rules(min_customers=1000) }}\"
```

## 🏆 Best Practices

### 1. **Organization**

```yaml
# ✅ Good: Organized by layer
# models/staging/schema.yml - Sources + staging models
# models/marts/schema.yml - Mart models
# models/intermediate/schema.yml - Intermediate models

# ❌ Bad: One giant schema.yml file
```

### 2. **Naming Conventions**

```yaml
# ✅ Good: Consistent naming
sources:
  - name: raw_data        # snake_case
    tables:
      - name: customers   # singular, snake_case

models:
  - name: dim_customers   # prefix + singular
  - name: fct_orders      # prefix + plural for facts
```

### 3. **Documentation Standards**

```yaml
# ✅ Good: Comprehensive documentation
columns:
  - name: customer_id
    description: |
      Primary key for customer dimension.
      
      Business Rules:
      - Unique across all records
      - Generated by source system
      - Used for joining with fact tables
    
    tests:
      - unique
      - not_null

# ❌ Bad: Minimal documentation
columns:
  - name: customer_id
    description: \"ID\"
```

### 4. **Test Coverage**

```yaml
# ✅ Good: Comprehensive testing
columns:
  - name: customer_id
    tests:
      - unique
      - not_null
      - relationships:
          to: source('raw', 'customers')
          field: id
  
  - name: email
    tests:
      - unique
      - not_null
      - dbt_expectations.expect_column_values_to_match_regex:
          regex: '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'

# ❌ Bad: No tests
columns:
  - name: customer_id
  - name: email
```

### 5. **Metadata Usage**

```yaml
# ✅ Good: Rich metadata
models:
  - name: dim_customers
    meta:
      owner: \"analytics-team\"
      maturity: \"production\"
      update_frequency: \"daily\"
      business_critical: true
      pii_fields: [\"email\", \"phone\"]
      
# ❌ Bad: No metadata
models:
  - name: dim_customers
```

## 📊 Examples

### Complete Staging Schema

```yaml
version: 2

sources:
  - name: raw_data
    description: \"Raw source data from operational systems\"
    schema: \"{{ env_var('SOURCE_SCHEMA', 'raw') }}\"
    
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    
    tables:
      - name: customers
        description: \"Customer master data from CRM\"
        loaded_at_field: updated_at
        
        columns:
          - name: id
            description: \"Primary key from source system\"
            tests:
              - unique
              - not_null
          
          - name: email
            description: \"Customer email address\"
            tests:
              - unique
              - not_null
          
          - name: status
            description: \"Customer status\"
            tests:
              - accepted_values:
                  values: ['active', 'inactive', 'pending']

models:
  - name: stg_customers
    description: \"Staging model for customer data with basic cleaning\"
    
    columns:
      - name: customer_id
        description: \"Cleaned customer ID\"
        tests:
          - unique
          - not_null
      
      - name: full_name
        description: \"Cleaned customer name\"
        tests:
          - not_null
          - not_empty_string
      
      - name: email
        description: \"Validated email address\"
        tests:
          - unique
          - not_null
          - string_pattern:
              pattern: '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
      
      - name: phone
        description: \"Cleaned phone number\"
        tests:
          - string_pattern:
              pattern: '^[0-9]{10,15}$'
      
      - name: created_at
        description: \"Record creation timestamp\"
        tests:
          - not_null
```

### Complete Marts Schema

```yaml
version: 2

models:
  - name: dim_customers
    description: |
      # Customer Dimension Table
      
      Master customer dimension with SCD Type 2 implementation.
      
      ## Business Logic
      - One record per customer per time period
      - Tracks changes in customer attributes over time
      - Current records flagged with is_current = true
      
      ## Data Sources
      - `{{ source('raw_data', 'customers') }}`
      - `{{ ref('stg_customers') }}`
      
      ## Update Schedule
      - Daily at 2 AM
      - Full refresh on Sundays
    
    config:
      materialized: table
      post-hook: \"ANALYZE TABLE {{ this }} COMPUTE STATISTICS\"
      tags: [\"dimension\", \"scd2\", \"daily\"]
    
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - customer_id
            - valid_from
    
    columns:
      - name: customer_key
        description: \"Surrogate key for dimension\"
        tests:
          - unique
          - not_null
      
      - name: customer_id
        description: \"Business key from source system\"
        tests:
          - not_null
          - relationships:
              to: source('raw_data', 'customers')
              field: id
      
      - name: full_name
        description: \"Customer full name\"
        tests:
          - not_null
          - not_empty_string
      
      - name: email
        description: \"Customer email address\"
        tests:
          - not_null
          - string_pattern:
              pattern: '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
      
      - name: phone
        description: \"Customer phone number\"
        tests:
          - string_pattern:
              pattern: '^[0-9]{10,15}$'
      
      - name: valid_from
        description: \"SCD valid from timestamp\"
        tests:
          - not_null
      
      - name: valid_to
        description: \"SCD valid to timestamp\"
      
      - name: is_current
        description: \"Current record indicator\"
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
```

---

## 🎯 Kết luận

**schema.yml là backbone của dbt project:**

- ✅ **Data Quality** - Automated testing framework
- ✅ **Documentation** - Self-documenting data models
- ✅ **Data Lineage** - Clear source → model relationships
- ✅ **Collaboration** - Team understanding và governance
- ✅ **Automation** - CI/CD integration với testing

**Key Components:**
1. **Sources** - Define external data sources
2. **Models** - Describe dbt transformations
3. **Tests** - Ensure data quality
4. **Documentation** - Explain business logic
5. **Metadata** - Rich context for governance

**Remember:** Good schema.yml = Reliable data + Happy teams! 🎯

---

**Tác giả:** VPAURA  
**Ngày:** 2025-12-30  
**Slogan:** `Code chay như muối bỏ bể, nhờ VPAura thì ngon luôn` 🚀
