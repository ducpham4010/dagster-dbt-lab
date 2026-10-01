# 🌱 DBT SEEDS - COMPREHENSIVE GUIDE

## 📋 Table of Contents
1. [Seeds Overview](#seeds-overview)
2. [Seeds vs Other dbt Resources](#seeds-vs-other-dbt-resources)
3. [Basic Seeds Setup](#basic-seeds-setup)
4. [Advanced Seeds Configuration](#advanced-seeds-configuration)
5. [Seeds Best Practices](#seeds-best-practices)
6. [Real-world Examples](#real-world-examples)
7. [Seeds Testing](#seeds-testing)
8. [Seeds in Production](#seeds-in-production)
9. [Troubleshooting](#troubleshooting)
10. [Performance Optimization](#performance-optimization)

---

## 🌱 Seeds Overview

### What are dbt Seeds?
**Seeds** là CSV files được dbt load vào data warehouse như tables. Seeds thích hợp cho:
- Reference data (lookup tables)
- Configuration data
- Small datasets (< 1MB)
- Static data không thay đổi thường xuyên

### Key Characteristics:
- ✅ **Static data** - Không thay đổi thường xuyên
- ✅ **Small size** - Khuyến nghị < 1MB
- ✅ **Version controlled** - CSV files trong git repo
- ✅ **Testable** - Có thể apply tests như models
- ✅ **Referenceable** - Sử dụng `{{ ref('seed_name') }}`

---

## 🔄 Seeds vs Other dbt Resources

| Feature | Seeds | Sources | Models | Snapshots |
|---------|-------|---------|--------|-----------|
| **Data Location** | CSV in repo | External tables | Derived from other resources | Historical tracking |
| **Data Size** | Small (< 1MB) | Any size | Any size | Any size |
| **Update Frequency** | Rarely | Real-time | On dbt run | On schedule |
| **Version Control** | Yes (CSV) | No (schema only) | Yes (SQL) | Yes (SQL) |
| **Use Case** | Reference data | Raw data | Transformations | SCD tracking |

---

## 🚀 Basic Seeds Setup

### 1. Directory Structure
```
aml_transaction_pipeline/
├── seeds/
│   ├── reference/
│   │   ├── country_codes.csv
│   │   ├── currency_rates.csv
│   │   └── product_categories.csv
│   ├── configuration/
│   │   ├── business_rules.csv
│   │   └── validation_thresholds.csv
│   └── lookup/
│       ├── status_mappings.csv
│       └── department_hierarchy.csv
├── models/
└── dbt_project.yml
```

### 2. Basic CSV Example

**File: `seeds/reference/country_codes.csv`**
```csv
country_code,country_name,region,currency_code,is_active
US,United States,North America,USD,true
VN,Vietnam,Southeast Asia,VND,true
JP,Japan,East Asia,JPY,true
UK,United Kingdom,Europe,GBP,true
DE,Germany,Europe,EUR,true
```

**File: `seeds/reference/currency_rates.csv`**
```csv
from_currency,to_currency,exchange_rate,effective_date
USD,VND,24500.00,2024-01-01
EUR,VND,26800.00,2024-01-01
JPY,VND,165.50,2024-01-01
GBP,VND,31200.00,2024-01-01
```

### 3. Basic Configuration

**File: `dbt_project.yml`**
```yaml
name: 'aml_transaction_pipeline'
version: '1.0.0'

# Seeds configuration
seeds:
  aml_transaction_pipeline:
    # Global seeds config
    +materialized: table
    +schema: reference
    
    # Specific folder configs
    reference:
      +schema: reference_data
      country_codes:
        +column_types:
          country_code: varchar(2)
          country_name: varchar(100)
          region: varchar(50)
          currency_code: varchar(3)
          is_active: boolean
    
    configuration:
      +schema: config_data
      business_rules:
        +column_types:
          rule_id: integer
          rule_name: varchar(200)
          rule_value: varchar(500)
          is_enabled: boolean
```

### 4. Running Seeds

```bash
# Load all seeds
dbt seed

# Load specific seed
dbt seed --select country_codes

# Load seeds with full refresh
dbt seed --full-refresh

# Load seeds for specific environment
dbt seed --target prod
```

---

## ⚙️ Advanced Seeds Configuration

### 1. Schema Configuration

**File: `seeds/schema.yml`**
```yaml
version: 2

seeds:
  - name: country_codes
    description: "ISO country codes with regional information"
    columns:
      - name: country_code
        description: "ISO 3166-1 alpha-2 country code"
        tests:
          - unique
          - not_null
          - accepted_values:
              values: ['US', 'VN', 'JP', 'UK', 'DE', 'FR', 'CA', 'AU']
      
      - name: country_name
        description: "Official country name"
        tests:
          - unique
          - not_null
      
      - name: region
        description: "Geographic region"
        tests:
          - not_null
          - accepted_values:
              values: ['North America', 'South America', 'Europe', 'Asia', 'Africa', 'Oceania']
      
      - name: currency_code
        description: "ISO 4217 currency code"
        tests:
          - not_null
      
      - name: is_active
        description: "Whether country is active for business"
        tests:
          - not_null

  - name: currency_rates
    description: "Daily currency exchange rates"
    columns:
      - name: from_currency
        description: "Source currency code"
        tests:
          - not_null
      
      - name: to_currency
        description: "Target currency code"
        tests:
          - not_null
      
      - name: exchange_rate
        description: "Exchange rate from source to target"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "> 0"
      
      - name: effective_date
        description: "Date when rate is effective"
        tests:
          - not_null

  - name: product_categories
    description: "Product category hierarchy"
    columns:
      - name: category_id
        description: "Unique category identifier"
        tests:
          - unique
          - not_null
      
      - name: category_name
        description: "Category display name"
        tests:
          - unique
          - not_null
      
      - name: parent_category_id
        description: "Parent category for hierarchy"
      
      - name: level
        description: "Hierarchy level (1=top, 2=sub, etc.)"
        tests:
          - not_null
          - accepted_values:
              values: [1, 2, 3, 4]
      
      - name: is_active
        description: "Whether category is currently active"
        tests:
          - not_null
```

### 2. Advanced Column Types

**File: `dbt_project.yml`**
```yaml
seeds:
  aml_transaction_pipeline:
    reference:
      country_codes:
        +column_types:
          country_code: varchar(2)
          country_name: varchar(100)
          region: varchar(50)
          currency_code: varchar(3)
          is_active: boolean
      
      currency_rates:
        +column_types:
          from_currency: varchar(3)
          to_currency: varchar(3)
          exchange_rate: decimal(10,4)
          effective_date: date
      
      product_categories:
        +column_types:
          category_id: integer
          category_name: varchar(200)
          parent_category_id: integer
          level: integer
          is_active: boolean
          created_at: timestamp
          updated_at: timestamp
```

### 3. Environment-Specific Seeds

**File: `dbt_project.yml`**
```yaml
seeds:
  aml_transaction_pipeline:
    +schema: "{{ 'reference_' ~ target.name }}"
    
    # Development specific
    "{{ 'dev' if target.name == 'dev' else '' }}":
      +full_refresh: true
    
    # Production specific  
    "{{ 'prod' if target.name == 'prod' else '' }}":
      +full_refresh: false
      +on_schema_change: "fail"
```

---

## 📊 Real-world Examples

### 1. Banking Reference Data

**File: `seeds/banking/account_types.csv`**
```csv
account_type_code,account_type_name,category,min_balance,max_daily_limit,interest_rate,is_active
SAV,Savings Account,Personal,100000,5000000,0.025,true
CHK,Checking Account,Personal,50000,10000000,0.001,true
BUS,Business Account,Business,500000,50000000,0.015,true
INV,Investment Account,Investment,1000000,100000000,0.035,true
LON,Loan Account,Credit,-999999999,0,0.120,true
```

**File: `seeds/banking/transaction_codes.csv`**
```csv
transaction_code,transaction_name,category,direction,fee_applicable,requires_approval
DEP,Deposit,Cash,Credit,false,false
WTH,Withdrawal,Cash,Debit,true,false
TRF,Transfer,Electronic,Both,true,false
PAY,Payment,Electronic,Debit,true,false
INT,Interest,System,Credit,false,false
FEE,Fee,System,Debit,false,false
REV,Reversal,Adjustment,Both,false,true
```

### 2. AML Reference Data

**File: `seeds/aml/risk_categories.csv`**
```csv
risk_level,risk_score_min,risk_score_max,monitoring_frequency,approval_required,description
LOW,0,30,Monthly,false,Low risk customers with minimal monitoring
MEDIUM,31,70,Weekly,false,Medium risk requiring regular monitoring
HIGH,71,90,Daily,true,High risk requiring daily monitoring and approval
CRITICAL,91,100,Real-time,true,Critical risk requiring immediate attention
```

**File: `seeds/aml/country_risk_ratings.csv`**
```csv
country_code,country_name,fatf_rating,risk_score,sanctions_list,enhanced_dd_required,last_updated
US,United States,Compliant,15,false,false,2024-01-01
VN,Vietnam,Compliant,25,false,false,2024-01-01
AF,Afghanistan,Non-Compliant,95,true,true,2024-01-01
IR,Iran,Non-Compliant,98,true,true,2024-01-01
KP,North Korea,Non-Compliant,100,true,true,2024-01-01
```

### 3. Business Configuration

**File: `seeds/config/business_rules.csv`**
```csv
rule_id,rule_name,rule_category,rule_value,data_type,is_enabled,environment,description
1,MAX_DAILY_TRANSACTION_AMOUNT,Transaction,50000000,decimal,true,prod,Maximum daily transaction amount in VND
2,MIN_ACCOUNT_BALANCE,Account,100000,decimal,true,prod,Minimum account balance requirement
3,KYC_EXPIRY_DAYS,Compliance,365,integer,true,prod,Days before KYC documents expire
4,SUSPICIOUS_TRANSACTION_THRESHOLD,AML,10000000,decimal,true,prod,Amount threshold for suspicious transaction alerts
5,MAX_FAILED_LOGIN_ATTEMPTS,Security,3,integer,true,prod,Maximum failed login attempts before account lock
```

---

## 🧪 Seeds Testing

### 1. Basic Tests in schema.yml

```yaml
version: 2

seeds:
  - name: account_types
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - account_type_code
            - category
    columns:
      - name: account_type_code
        tests:
          - unique
          - not_null
          - dbt_utils.not_empty_string
      
      - name: min_balance
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      
      - name: interest_rate
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "between 0 and 1"
```

### 2. Custom Generic Tests for Seeds

**File: `tests/generic/valid_currency_code.sql`**
```sql
{% test valid_currency_code(model, column_name) %}

    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} not in (
        'USD', 'EUR', 'GBP', 'JPY', 'VND', 
        'AUD', 'CAD', 'CHF', 'CNY', 'SGD'
    )
    or {{ column_name }} is null
    or length({{ column_name }}) != 3

{% endtest %}
```

**File: `tests/generic/valid_country_code.sql`**
```sql
{% test valid_country_code(model, column_name) %}

    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} not rlike '^[A-Z]{2}$'
    or {{ column_name }} is null

{% endtest %}
```

### 3. Singular Tests for Business Logic

**File: `tests/seeds/test_currency_rates_consistency.sql`**
```sql
-- Test: Currency rates should be reciprocal
-- If USD to VND = 24500, then VND to USD should be ~0.0000408

with rate_pairs as (
    select 
        r1.from_currency,
        r1.to_currency,
        r1.exchange_rate as rate_1,
        r2.exchange_rate as rate_2,
        abs(1.0 - (r1.exchange_rate * r2.exchange_rate)) as deviation
    from {{ ref('currency_rates') }} r1
    join {{ ref('currency_rates') }} r2
        on r1.from_currency = r2.to_currency
        and r1.to_currency = r2.from_currency
        and r1.effective_date = r2.effective_date
)

select *
from rate_pairs
where deviation > 0.01  -- Allow 1% deviation for rounding
```

**File: `tests/seeds/test_account_types_business_rules.sql`**
```sql
-- Test: Business rules for account types

select *
from {{ ref('account_types') }}
where 
    -- Savings accounts should have positive minimum balance
    (account_type_code = 'SAV' and min_balance <= 0)
    
    -- Investment accounts should have higher minimum balance
    or (account_type_code = 'INV' and min_balance < 1000000)
    
    -- Loan accounts should have negative minimum balance
    or (account_type_code = 'LON' and min_balance >= 0)
    
    -- Interest rates should be reasonable
    or (interest_rate < 0 or interest_rate > 0.5)
```

---

## 🏭 Seeds in Production

### 1. Production Deployment Strategy

**File: `scripts/deploy_seeds_prod.sh`**
```bash
#!/bin/bash

# Production Seeds Deployment Script
set -e

echo "🌱 Starting Production Seeds Deployment..."

# Set environment
export DBT_PROFILES_DIR=~/.dbt
export DBT_TARGET=prod

# Backup existing seeds (if any)
echo "📦 Creating backup of existing seeds..."
dbt run-operation backup_seeds_tables

# Validate CSV files
echo "✅ Validating CSV files..."
python scripts/validate_seed_files.py

# Test seeds in staging first
echo "🧪 Testing seeds in staging..."
dbt seed --target staging
dbt test --select "resource_type:seed" --target staging

# Deploy to production
echo "🚀 Deploying seeds to production..."
dbt seed --target prod --full-refresh

# Run tests
echo "🧪 Running production tests..."
dbt test --select "resource_type:seed" --target prod

# Verify data quality
echo "📊 Verifying data quality..."
dbt run-operation validate_seed_data_quality

echo "✅ Seeds deployment completed successfully!"
```

### 2. Seed Validation Script

**File: `scripts/validate_seed_files.py`**
```python
#!/usr/bin/env python3
"""
Validate dbt seed CSV files before deployment
"""

import os
import csv
import sys
from pathlib import Path
import pandas as pd

def validate_csv_structure(file_path):
    """Validate CSV file structure and content"""
    errors = []
    
    try:
        # Read CSV
        df = pd.read_csv(file_path)
        
        # Check for empty file
        if df.empty:
            errors.append(f"File is empty: {file_path}")
            return errors
        
        # Check for duplicate headers
        if len(df.columns) != len(set(df.columns)):
            errors.append(f"Duplicate column names in: {file_path}")
        
        # Check for completely empty rows
        empty_rows = df.isnull().all(axis=1).sum()
        if empty_rows > 0:
            errors.append(f"Found {empty_rows} completely empty rows in: {file_path}")
        
        # Check for trailing spaces in headers
        for col in df.columns:
            if col != col.strip():
                errors.append(f"Column name has trailing spaces: '{col}' in {file_path}")
        
        # Validate specific files
        file_name = Path(file_path).stem
        
        if file_name == 'country_codes':
            errors.extend(validate_country_codes(df, file_path))
        elif file_name == 'currency_rates':
            errors.extend(validate_currency_rates(df, file_path))
        elif file_name == 'account_types':
            errors.extend(validate_account_types(df, file_path))
            
    except Exception as e:
        errors.append(f"Error reading {file_path}: {str(e)}")
    
    return errors

def validate_country_codes(df, file_path):
    """Validate country codes seed"""
    errors = []
    
    required_columns = ['country_code', 'country_name', 'region', 'currency_code', 'is_active']
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        errors.append(f"Missing required columns in {file_path}: {missing_cols}")
        return errors
    
    # Validate country codes format
    invalid_codes = df[~df['country_code'].str.match(r'^[A-Z]{2}$', na=False)]
    if not invalid_codes.empty:
        errors.append(f"Invalid country codes in {file_path}: {invalid_codes['country_code'].tolist()}")
    
    # Check for duplicates
    duplicates = df[df.duplicated('country_code', keep=False)]
    if not duplicates.empty:
        errors.append(f"Duplicate country codes in {file_path}: {duplicates['country_code'].tolist()}")
    
    return errors

def validate_currency_rates(df, file_path):
    """Validate currency rates seed"""
    errors = []
    
    required_columns = ['from_currency', 'to_currency', 'exchange_rate', 'effective_date']
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        errors.append(f"Missing required columns in {file_path}: {missing_cols}")
        return errors
    
    # Validate exchange rates are positive
    negative_rates = df[df['exchange_rate'] <= 0]
    if not negative_rates.empty:
        errors.append(f"Non-positive exchange rates in {file_path}")
    
    # Validate currency codes format
    for col in ['from_currency', 'to_currency']:
        invalid_currencies = df[~df[col].str.match(r'^[A-Z]{3}$', na=False)]
        if not invalid_currencies.empty:
            errors.append(f"Invalid currency codes in column {col} of {file_path}")
    
    return errors

def validate_account_types(df, file_path):
    """Validate account types seed"""
    errors = []
    
    required_columns = ['account_type_code', 'account_type_name', 'category', 'min_balance', 'is_active']
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        errors.append(f"Missing required columns in {file_path}: {missing_cols}")
        return errors
    
    # Validate account type codes
    invalid_codes = df[~df['account_type_code'].str.match(r'^[A-Z]{3}$', na=False)]
    if not invalid_codes.empty:
        errors.append(f"Invalid account type codes in {file_path}")
    
    return errors

def main():
    """Main validation function"""
    seeds_dir = Path('seeds')
    
    if not seeds_dir.exists():
        print("❌ Seeds directory not found!")
        sys.exit(1)
    
    all_errors = []
    csv_files = list(seeds_dir.rglob('*.csv'))
    
    if not csv_files:
        print("⚠️  No CSV files found in seeds directory")
        return
    
    print(f"🔍 Validating {len(csv_files)} seed files...")
    
    for csv_file in csv_files:
        print(f"  Checking: {csv_file}")
        errors = validate_csv_structure(csv_file)
        if errors:
            all_errors.extend(errors)
    
    if all_errors:
        print("\n❌ Validation failed with errors:")
        for error in all_errors:
            print(f"  - {error}")
        sys.exit(1)
    else:
        print("\n✅ All seed files validated successfully!")

if __name__ == "__main__":
    main()
```

### 3. Seed Monitoring Macro

**File: `macros/seed_monitoring.sql`**
```sql
{% macro backup_seeds_tables() %}
  {% set seeds_list = [] %}
  
  {% for node in graph.nodes.values() %}
    {% if node.resource_type == 'seed' %}
      {% set seeds_list = seeds_list.append(node.name) %}
    {% endif %}
  {% endfor %}
  
  {% for seed_name in seeds_list %}
    {% set backup_table = seed_name ~ '_backup_' ~ modules.datetime.datetime.now().strftime('%Y%m%d_%H%M%S') %}
    {% set backup_sql %}
      create table {{ backup_table }} as 
      select * from {{ ref(seed_name) }}
    {% endset %}
    
    {{ log("Creating backup: " ~ backup_table, info=True) }}
    {% do run_query(backup_sql) %}
  {% endfor %}
{% endmacro %}

{% macro validate_seed_data_quality() %}
  {% set validation_results = [] %}
  
  -- Check row counts
  {% for node in graph.nodes.values() %}
    {% if node.resource_type == 'seed' %}
      {% set count_sql %}
        select count(*) as row_count from {{ ref(node.name) }}
      {% endset %}
      
      {% set results = run_query(count_sql) %}
      {% if results %}
        {% set row_count = results.columns[0].values()[0] %}
        {% set validation_results = validation_results.append({
          'seed_name': node.name,
          'row_count': row_count,
          'status': 'OK' if row_count > 0 else 'EMPTY'
        }) %}
      {% endif %}
    {% endif %}
  {% endfor %}
  
  -- Log results
  {{ log("=== Seed Data Quality Report ===", info=True) }}
  {% for result in validation_results %}
    {{ log(result.seed_name ~ ": " ~ result.row_count ~ " rows (" ~ result.status ~ ")", info=True) }}
  {% endfor %}
{% endmacro %}

{% macro get_seed_freshness() %}
  {% set freshness_sql %}
    select 
      table_name,
      last_ddl_time,
      row_count
    from information_schema.tables 
    where table_schema = '{{ target.schema }}'
    and table_name in (
      {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'seed' %}
          '{{ node.name }}'{{ ',' if not loop.last }}
        {% endif %}
      {% endfor %}
    )
    order by last_ddl_time desc
  {% endset %}
  
  {% set results = run_query(freshness_sql) %}
  {{ return(results) }}
{% endmacro %}
```

---

## 🎯 Seeds Best Practices

### 1. File Organization
```
seeds/
├── reference/          # Static reference data
│   ├── countries.csv
│   ├── currencies.csv
│   └── regions.csv
├── configuration/      # Business configuration
│   ├── business_rules.csv
│   ├── thresholds.csv
│   └── mappings.csv
├── lookup/            # Lookup tables
│   ├── status_codes.csv
│   └── categories.csv
└── test_data/         # Test data for development
    ├── sample_customers.csv
    └── sample_transactions.csv
```

### 2. Naming Conventions
- **Files**: `snake_case.csv`
- **Columns**: `snake_case`
- **Tables**: Prefix with seed type (`ref_`, `cfg_`, `lkp_`)

### 3. Data Quality Rules
```yaml
# Always include these tests for seeds
tests:
  - unique              # For primary keys
  - not_null           # For required fields
  - accepted_values    # For enum fields
  - relationships      # For foreign keys
```

### 4. Size Guidelines
- **Small**: < 100 KB (✅ Perfect for seeds)
- **Medium**: 100 KB - 1 MB (⚠️ Use with caution)
- **Large**: > 1 MB (❌ Use sources instead)

### 5. Update Frequency
- **Static**: Never changes (country codes, currencies)
- **Rare**: Changes quarterly/yearly (business rules)
- **Regular**: Changes monthly (❌ Consider sources)
- **Frequent**: Changes daily (❌ Use sources)

---

## 🚨 Troubleshooting

### 1. Common Issues

#### Issue: "Seed file not found"
```bash
# Error
dbt seed --select country_codes
# Running with dbt=1.0.0
# Encountered an error:
# Could not find seed country_codes
```

**Solution:**
```bash
# Check file exists
ls seeds/reference/country_codes.csv

# Check dbt_project.yml configuration
# Ensure seed is in correct directory structure
```

#### Issue: "Column type mismatch"
```bash
# Error during seed load
# ORA-01722: invalid number
```

**Solution:**
```yaml
# Fix in dbt_project.yml
seeds:
  aml_transaction_pipeline:
    country_codes:
      +column_types:
        country_code: varchar(2)  # Not integer
        is_active: boolean        # Not varchar
```

#### Issue: "Duplicate key error"
```bash
# Error
# ORA-00001: unique constraint violated
```

**Solution:**
```bash
# Check for duplicates in CSV
python -c "
import pandas as pd
df = pd.read_csv('seeds/reference/country_codes.csv')
print(df[df.duplicated('country_code')])
"

# Use full-refresh to replace table
dbt seed --select country_codes --full-refresh
```

### 2. Debugging Commands

```bash
# Debug seed configuration
dbt debug --config-dir ~/.dbt

# Show compiled seed SQL
dbt compile --select country_codes
cat target/compiled/aml_transaction_pipeline/seeds/country_codes.sql

# Verbose logging
dbt seed --select country_codes --log-level debug

# Test specific seed
dbt test --select country_codes

# Show seed lineage
dbt ls --select country_codes --resource-type seed
```

### 3. Performance Issues

#### Slow Seed Loading
```yaml
# Optimize with proper column types
seeds:
  aml_transaction_pipeline:
    large_seed:
      +column_types:
        id: integer           # Not varchar
        amount: decimal(15,2) # Not varchar
        date_field: date      # Not varchar
      +pre-hook: "alter session set parallel_dml_mode = enable"
      +post-hook: "analyze table {{ this }} compute statistics"
```

#### Memory Issues
```bash
# Load seeds in batches
dbt seed --select tag:batch_1
dbt seed --select tag:batch_2

# Use smaller files
split -l 10000 large_seed.csv seed_part_
```

---

## ⚡ Performance Optimization

### 1. Efficient Column Types

```yaml
# Good: Specific types
seeds:
  aml_transaction_pipeline:
    optimized_seed:
      +column_types:
        id: integer
        code: varchar(10)
        amount: decimal(15,2)
        flag: boolean
        created_at: timestamp

# Bad: All varchar
seeds:
  aml_transaction_pipeline:
    slow_seed:
      +column_types:
        id: varchar(50)        # Should be integer
        code: varchar(255)     # Too large
        amount: varchar(100)   # Should be decimal
        flag: varchar(10)      # Should be boolean
```

### 2. Indexing Strategy

```yaml
# Add indexes via post-hooks
seeds:
  aml_transaction_pipeline:
    country_codes:
      +post-hook: 
        - "create index idx_country_code on {{ this }} (country_code)"
        - "create index idx_region on {{ this }} (region)"
        - "analyze table {{ this }} compute statistics"
```

### 3. Partitioning for Large Seeds

```yaml
# For time-based seeds
seeds:
  aml_transaction_pipeline:
    historical_rates:
      +materialized: table
      +post-hook: |
        alter table {{ this }} 
        partition by range (effective_date) 
        interval (numtoyminterval(1, 'MONTH'))
```

### 4. Parallel Loading

**File: `scripts/parallel_seed_load.py`**
```python
#!/usr/bin/env python3
"""
Load seeds in parallel for better performance
"""

import subprocess
import concurrent.futures
import sys

def load_seed(seed_name):
    """Load individual seed"""
    try:
        result = subprocess.run([
            'dbt', 'seed', '--select', seed_name
        ], capture_output=True, text=True, check=True)
        return f"✅ {seed_name}: Success"
    except subprocess.CalledProcessError as e:
        return f"❌ {seed_name}: {e.stderr}"

def main():
    seeds = [
        'country_codes',
        'currency_rates', 
        'account_types',
        'transaction_codes',
        'risk_categories'
    ]
    
    print(f"🚀 Loading {len(seeds)} seeds in parallel...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(load_seed, seed): seed for seed in seeds}
        
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            print(result)

if __name__ == "__main__":
    main()
```

---

## 📚 Advanced Use Cases

### 1. Dynamic Seeds with Macros

**File: `macros/generate_seed_sql.sql`**
```sql
{% macro generate_seed_from_query(seed_name, query) %}
  {% set results = run_query(query) %}
  {% set csv_content = [] %}
  
  -- Generate header
  {% set header = results.column_names | join(',') %}
  {% set csv_content = csv_content.append(header) %}
  
  -- Generate rows
  {% for row in results.rows %}
    {% set row_str = row | map('string') | join(',') %}
    {% set csv_content = csv_content.append(row_str) %}
  {% endfor %}
  
  -- Write to file
  {% set file_path = 'seeds/generated/' ~ seed_name ~ '.csv' %}
  {{ log("Generated seed file: " ~ file_path, info=True) }}
  
  {% do csv_content | join('\n') | as_text | write_file(file_path) %}
{% endmacro %}
```

### 2. Seed Versioning

**File: `macros/seed_versioning.sql`**
```sql
{% macro create_seed_version(seed_name, version) %}
  {% set versioned_name = seed_name ~ '_v' ~ version %}
  {% set create_sql %}
    create table {{ versioned_name }} as
    select *, '{{ version }}' as seed_version
    from {{ ref(seed_name) }}
  {% endset %}
  
  {{ log("Creating seed version: " ~ versioned_name, info=True) }}
  {% do run_query(create_sql) %}
{% endmacro %}

{% macro compare_seed_versions(seed_name, old_version, new_version) %}
  {% set old_table = seed_name ~ '_v' ~ old_version %}
  {% set new_table = seed_name ~ '_v' ~ new_version %}
  
  {% set comparison_sql %}
    select 
      'ADDED' as change_type,
      new.*
    from {{ new_table }} new
    left join {{ old_table }} old using ({{ get_primary_key(seed_name) }})
    where old.{{ get_primary_key(seed_name) }} is null
    
    union all
    
    select 
      'REMOVED' as change_type,
      old.*
    from {{ old_table }} old
    left join {{ new_table }} new using ({{ get_primary_key(seed_name) }})
    where new.{{ get_primary_key(seed_name) }} is null
    
    union all
    
    select 
      'MODIFIED' as change_type,
      new.*
    from {{ new_table }} new
    join {{ old_table }} old using ({{ get_primary_key(seed_name) }})
    where new != old
  {% endset %}
  
  {% set results = run_query(comparison_sql) %}
  {{ return(results) }}
{% endmacro %}
```

### 3. Seed Data Lineage

**File: `analyses/seed_lineage.sql`**
```sql
-- Analyze which models depend on seeds
with seed_dependencies as (
  select 
    '{{ node.name }}' as model_name,
    '{{ ref_name }}' as depends_on_seed
  from (
    {% for node in graph.nodes.values() %}
      {% if node.resource_type == 'model' %}
        {% for ref_name in node.refs %}
          {% set ref_node = graph.nodes.get('seed.' ~ project_name ~ '.' ~ ref_name[0]) %}
          {% if ref_node and ref_node.resource_type == 'seed' %}
            select '{{ node.name }}' as model_name, '{{ ref_name[0] }}' as depends_on_seed
            {{ 'union all' if not loop.last }}
          {% endif %}
        {% endfor %}
      {% endif %}
    {% endfor %}
  )
)

select 
  depends_on_seed as seed_name,
  count(*) as dependent_models,
  listagg(model_name, ', ') within group (order by model_name) as model_list
from seed_dependencies
group by depends_on_seed
order by dependent_models desc
```

---

## 🎓 Summary & Next Steps

### ✅ What You've Learned

1. **Seeds Fundamentals**
   - What seeds are and when to use them
   - Directory structure and organization
   - Basic CSV format and configuration

2. **Advanced Configuration**
   - Column types and schema definitions
   - Environment-specific configurations
   - Testing strategies for seeds

3. **Production Practices**
   - Deployment strategies
   - Validation scripts
   - Monitoring and backup procedures

4. **Performance Optimization**
   - Efficient column types
   - Indexing strategies
   - Parallel loading techniques

5. **Advanced Use Cases**
   - Dynamic seed generation
   - Versioning and comparison
   - Dependency analysis

### 🚀 Next Steps

1. **Implement in Your Project**
   ```bash
   # Create seeds directory
   mkdir -p seeds/{reference,configuration,lookup}
   
   # Add your first seed
   # Copy examples and customize for your use case
   ```

2. **Add to CI/CD Pipeline**
   ```yaml
   # Add to .github/workflows/
   - name: Validate and Load Seeds
     run: |
       python scripts/validate_seed_files.py
       dbt seed --target prod
       dbt test --select "resource_type:seed"
   ```

3. **Monitor in Production**
   ```bash
   # Add monitoring
   dbt run-operation validate_seed_data_quality
   dbt run-operation get_seed_freshness
   ```

### 📖 Related Guides
- `DBT_SOURCES_GUIDE.md` - For large external data
- `DBT_SNAPSHOTS_GUIDE.md` - For tracking changes over time
- `DBT_TESTS_GUIDE.md` - Comprehensive testing strategies
- `DBT_MACROS_GUIDE.md` - Reusable SQL functions

---

## 🎯 Key Takeaways

> **Seeds are perfect for small, static reference data that needs to be version controlled with your dbt project. Use them wisely for lookup tables, configuration data, and business rules - but remember the size limitations!**

**Happy seeding! 🌱✨**
