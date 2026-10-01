# dbt Multi-Environment Configuration - Hướng dẫn toàn diện

## 📋 Mục lục

1. [Giới thiệu về Multi-Environment](#giới-thiệu-về-multi-environment)
2. [Cấu hình profiles.yml](#cấu-hình-profilesyml)
3. [Environment Variables](#environment-variables)
4. [dbt_project.yml Configuration](#dbt_projectyml-configuration)
5. [Environment-specific Models](#environment-specific-models)
6. [Environment Macros](#environment-macros)
7. [Deployment Strategies](#deployment-strategies)
8. [CI/CD Integration](#cicd-integration)
9. [Best Practices](#best-practices)
10. [Troubleshooting](#troubleshooting)

## 🌍 Giới thiệu về Multi-Environment

### Multi-Environment là gì?

**Multi-Environment Configuration** trong dbt cho phép bạn quản lý và deploy dbt project trên nhiều môi trường khác nhau với các cấu hình riêng biệt.

### Tại sao cần Multi-Environment?

- 🔧 **Development** - Môi trường phát triển với dữ liệu mẫu
- 🧪 **Staging** - Môi trường test với dữ liệu gần giống production
- 🚀 **Production** - Môi trường sản xuất với dữ liệu thật
- 🔍 **Testing** - Môi trường CI/CD cho automated testing

### Lợi ích của Multi-Environment:

- ✅ **Isolated Development** - Phát triển không ảnh hưởng production
- ✅ **Safe Testing** - Test trên staging trước khi deploy production
- ✅ **Environment-specific Configuration** - Cấu hình riêng cho từng môi trường
- ✅ **Automated Deployment** - CI/CD pipeline tự động
- ✅ **Data Governance** - Kiểm soát access và permissions

## 🔧 Cấu hình profiles.yml

### Cấu trúc cơ bản

```yaml
# ~/.dbt/profiles.yml
aml_transaction_pipeline:
  target: dev  # Default environment
  outputs:
    
    # Development Environment
    dev:
      type: oracle
      host: \"{{ env_var('DEV_DB_HOST', 'localhost') }}\"
      user: \"{{ env_var('DEV_DB_USER', 'dev_user') }}\"
      password: \"{{ env_var('DEV_DB_PASSWORD', 'dev_password') }}\"
      port: \"{{ env_var('DEV_DB_PORT', 1521) | as_number }}\"
      service: \"{{ env_var('DEV_DB_SERVICE', 'dev_db') }}\"
      schema: \"{{ env_var('DEV_DB_SCHEMA', 'dev_schema') }}\"
      threads: 4
      
    # Staging Environment  
    staging:
      type: oracle
      host: \"{{ env_var('STAGING_DB_HOST') }}\"
      user: \"{{ env_var('STAGING_DB_USER') }}\"
      password: \"{{ env_var('STAGING_DB_PASSWORD') }}\"
      port: \"{{ env_var('STAGING_DB_PORT', 1521) | as_number }}\"
      service: \"{{ env_var('STAGING_DB_SERVICE') }}\"
      schema: \"{{ env_var('STAGING_DB_SCHEMA') }}\"
      threads: 8
      
    # Production Environment
    prod:
      type: oracle
      host: \"{{ env_var('PROD_DB_HOST') }}\"
      user: \"{{ env_var('PROD_DB_USER') }}\"
      password: \"{{ env_var('PROD_DB_PASSWORD') }}\"
      port: \"{{ env_var('PROD_DB_PORT', 1521) | as_number }}\"
      service: \"{{ env_var('PROD_DB_SERVICE') }}\"
      schema: \"{{ env_var('PROD_DB_SCHEMA') }}\"
      threads: 16
```

### Environment Variables trong profiles.yml

```yaml
# Sử dụng environment variables với default values
host: \"{{ env_var('DB_HOST', 'localhost') }}\"

# Required environment variables (không có default)
password: \"{{ env_var('DB_PASSWORD') }}\"

# Type conversion
port: \"{{ env_var('DB_PORT', 1521) | as_number }}\"
threads: \"{{ env_var('DB_THREADS', 4) | as_number }}\"

# Boolean values
ssl: \"{{ env_var('DB_SSL', 'false') | as_bool }}\"
```

## 🌐 Environment Variables

### Development (.env.dev)

```bash
# Database Configuration
DEV_DB_HOST=localhost
DEV_DB_USER=dev_user
DEV_DB_PASSWORD=dev_password
DEV_DB_PORT=1521
DEV_DB_SERVICE=dev_db
DEV_DB_SCHEMA=dev_schema

# dbt Configuration
DBT_TARGET=dev
DBT_THREADS=4
DBT_LOG_LEVEL=info

# Application Configuration
APP_ENV=development
DEBUG_MODE=true
ENABLE_TESTS=true
ENABLE_DOCS=true

# Data Configuration
DATA_RETENTION_DAYS=7
BATCH_SIZE=1000
```

### Staging (.env.staging)

```bash
# Database Configuration
STAGING_DB_HOST=staging-db.company.com
STAGING_DB_USER=staging_user
STAGING_DB_PASSWORD=staging_secure_password
STAGING_DB_PORT=1521
STAGING_DB_SERVICE=staging_db
STAGING_DB_SCHEMA=staging_schema

# dbt Configuration
DBT_TARGET=staging
DBT_THREADS=8
DBT_LOG_LEVEL=info

# Application Configuration
APP_ENV=staging
DEBUG_MODE=false
ENABLE_TESTS=true
ENABLE_DOCS=true

# Data Configuration
DATA_RETENTION_DAYS=90
BATCH_SIZE=10000

# Notification Configuration
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/STAGING/WEBHOOK
EMAIL_NOTIFICATIONS=true
```

### Production (.env.prod)

```bash
# Database Configuration
PROD_DB_HOST=prod-db.company.com
PROD_DB_USER=prod_user
PROD_DB_PASSWORD=prod_super_secure_password
PROD_DB_PORT=1521
PROD_DB_SERVICE=prod_db
PROD_DB_SCHEMA=prod_schema

# dbt Configuration
DBT_TARGET=prod
DBT_THREADS=16
DBT_LOG_LEVEL=warn

# Application Configuration
APP_ENV=production
DEBUG_MODE=false
ENABLE_TESTS=true
ENABLE_DOCS=true

# Data Configuration
DATA_RETENTION_DAYS=365
BATCH_SIZE=100000

# Security Configuration
ENABLE_AUDIT_LOG=true
ENCRYPT_SENSITIVE_DATA=true
```

### Loading Environment Variables

```bash
# Linux/Mac
export $(grep -v '^#' .env.dev | xargs)

# Windows
for /f \"usebackq tokens=1,2 delims==\" %a in (\".env.dev\") do set \"%a=%b\"

# Python script
import os
from dotenv import load_dotenv

load_dotenv('.env.dev')
```

## ⚙️ dbt_project.yml Configuration

### Environment-specific Variables

```yaml
# dbt_project.yml
vars:
  # Global variables
  project_name: 'aml_transaction_pipeline'
  
  # Environment-specific variables
  data_retention_days: \"{{ env_var('DATA_RETENTION_DAYS', '30') | as_number }}\"
  enable_tests: \"{{ env_var('ENABLE_TESTS', 'true') | as_bool }}\"
  debug_mode: \"{{ env_var('DEBUG_MODE', 'false') | as_bool }}\"
  batch_size: \"{{ env_var('BATCH_SIZE', '10000') | as_number }}\"
  
  # Schema configurations
  source_schema: \"{{ env_var('SOURCE_SCHEMA', target.schema) }}\"
  target_schema: \"{{ env_var('TARGET_SCHEMA', target.schema) }}\"
```

### Environment-specific Hooks

```yaml
# Environment-specific hooks
on-run-start:
  - \"{{ log('Starting ETL job in ' ~ target.name ~ ' environment', info=True) }}\"
  - \"{% if target.name == 'prod' %}{{ create_job_log_table() }}{% endif %}\"
  - \"{% if target.name in ['staging', 'prod'] %}{{ log_job_start('etl_pipeline') }}{% endif %}\"

on-run-end:
  - \"{{ log('ETL job completed in ' ~ target.name ~ ' environment', info=True) }}\"
  - \"{% if target.name in ['staging', 'prod'] %}{{ log_job_end('etl_pipeline') }}{% endif %}\"
  - \"{% if target.name == 'prod' %}{{ cleanup_temp_tables() }}{% endif %}\"
```

### Environment-specific Model Configuration

```yaml
models:
  aml_transaction_pipeline:
    # Global configurations
    +on_schema_change: \"fail\"
    
    # Staging models
    staging:
      +materialized: view
      +tags: [\"staging\", \"daily\"]
      
    # Mart models with environment-specific materialization
    marts:
      +materialized: \"{% if target.name == 'dev' %}view{% else %}table{% endif %}\"
      +tags: [\"marts\", \"critical\"]
      # Production-specific post-hooks
      +post-hook: |
        {% if target.name == 'prod' %}
          ANALYZE TABLE {{ this }} COMPUTE STATISTICS;
        {% endif %}
```

### Environment-specific Test Configuration

```yaml
tests:
  +store_failures: \"{{ true if target.name in ['staging', 'prod'] else false }}\"
  +severity: \"{{ 'error' if target.name == 'prod' else 'warn' }}\"
  
  # Environment-specific test selection
  +enabled: |
    {% if target.name == 'dev' %}
      {{ true if 'critical' in (model.tags or []) else false }}
    {% else %}
      {{ true }}
    {% endif %}
```

## 🎯 Environment-specific Models

### Conditional Logic trong Models

```sql
-- models/staging/stg_customers_env_aware.sql
{{ config(
    materialized='view',
    tags=['staging', 'environment_aware']
) }}

-- Environment-specific data filtering
{% if target.name == 'dev' %}
  {% set date_filter = \"created_at >= CURRENT_DATE - 7\" %}
  {% set row_limit = \"FETCH FIRST 1000 ROWS ONLY\" %}
{% elif target.name == 'staging' %}
  {% set date_filter = \"created_at >= CURRENT_DATE - 30\" %}
  {% set row_limit = \"\" %}
{% else %}
  {% set date_filter = \"1=1\" %}
  {% set row_limit = \"\" %}
{% endif %}

WITH source_data AS (
    SELECT 
        customer_id,
        full_name,
        phone,
        created_at
    FROM {{ source('raw_data', 'source_customers_test_dbt') }}
    WHERE {{ date_filter }}
    
    -- Environment-specific data quality filters
    {% if target.name in ['staging', 'prod'] %}
      AND full_name IS NOT NULL
      AND TRIM(full_name) != ''
    {% endif %}
),

processed_data AS (
    SELECT 
        customer_id,
        TRIM(full_name) as full_name,
        CASE 
            WHEN TRIM(phone) = '' THEN NULL 
            ELSE TRIM(phone) 
        END as phone,
        created_at,
        
        -- Environment-specific metadata
        '{{ target.name }}' as source_environment,
        CURRENT_TIMESTAMP as processed_at
        
        {% if target.name == 'dev' %}
        , 'DEV_PROCESSED' as debug_flag
        {% endif %}
        
        {% if target.name == 'prod' %}
        , '{{ run_started_at }}' as batch_id
        {% endif %}
        
    FROM source_data
)

SELECT * FROM processed_data
{% if target.name == 'dev' %}
ORDER BY created_at DESC
{{ row_limit }}
{% endif %}
```

### Environment-specific Table Names

```sql
-- Dynamic table naming based on environment
{% set table_suffix = get_env_table_suffix() %}

CREATE TABLE customer_data{{ table_suffix }} AS
SELECT * FROM {{ ref('stg_customers') }}
```

## 🔧 Environment Macros

### Environment Configuration Macro

```sql
-- macros/environment_macros.sql

{% macro get_env_config(config_name, default_value=none) %}
  {% set env_configs = {
    'dev': {
      'batch_size': 1000,
      'retention_days': 7,
      'enable_sampling': true,
      'log_level': 'debug'
    },
    'staging': {
      'batch_size': 10000,
      'retention_days': 30,
      'enable_sampling': false,
      'log_level': 'info'
    },
    'prod': {
      'batch_size': 100000,
      'retention_days': 365,
      'enable_sampling': false,
      'log_level': 'warn'
    }
  } %}
  
  {% set current_env = target.name %}
  {% set env_config = env_configs.get(current_env, {}) %}
  {% set config_value = env_config.get(config_name, default_value) %}
  
  {{ return(config_value) }}
{% endmacro %}
```

### Environment Validation Macro

```sql
{% macro validate_environment() %}
  {% set required_vars = ['source_schema', 'target_schema'] %}
  {% set missing_vars = [] %}
  
  {% for var_name in required_vars %}
    {% if not var(var_name) %}
      {% set _ = missing_vars.append(var_name) %}
    {% endif %}
  {% endfor %}
  
  {% if missing_vars %}
    {{ exceptions.raise_compiler_error(\"Missing required variables: \" ~ missing_vars | join(\", \")) }}
  {% endif %}
  
  {{ log(\"Environment validation passed for \" ~ target.name, info=True) }}
{% endmacro %}
```

### Environment-specific Data Sampling

```sql
{% macro apply_env_sampling(table_name, sample_percentage=10) %}
  {% if target.name == 'dev' and get_env_config('enable_sampling') %}
    SELECT * FROM (
      SELECT *, ROW_NUMBER() OVER (ORDER BY DBMS_RANDOM.VALUE) as rn
      FROM {{ table_name }}
    ) WHERE MOD(rn, {{ 100 // sample_percentage }}) = 0
  {% else %}
    SELECT * FROM {{ table_name }}
  {% endif %}
{% endmacro %}
```

## 🚀 Deployment Strategies

### 1. **Manual Deployment**

```bash
# Development
dbt run --target dev

# Staging
dbt run --target staging --store-failures

# Production
dbt run --target prod --threads 16 --store-failures
```

### 2. **Script-based Deployment**

```bash
# Using environment setup script
./scripts/setup_environment.sh dev
./scripts/setup_environment.sh staging
./scripts/setup_environment.sh prod --full-refresh
```

### 3. **Environment Promotion Pipeline**

```bash
#!/bin/bash
# promote_to_production.sh

echo \"🚀 Starting production promotion pipeline\"

# Step 1: Deploy to staging
echo \"📋 Step 1: Deploying to staging\"
./scripts/setup_environment.sh staging
if [ $? -ne 0 ]; then
    echo \"❌ Staging deployment failed\"
    exit 1
fi

# Step 2: Run comprehensive tests in staging
echo \"🧪 Step 2: Running staging tests\"
dbt test --target staging --store-failures
if [ $? -ne 0 ]; then
    echo \"❌ Staging tests failed\"
    exit 1
fi

# Step 3: Manual approval (can be automated)
echo \"⏳ Step 3: Waiting for approval to deploy to production\"
read -p \"Deploy to production? (y/n): \" -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo \"❌ Production deployment cancelled\"
    exit 1
fi

# Step 4: Deploy to production
echo \"🚀 Step 4: Deploying to production\"
./scripts/setup_environment.sh prod
if [ $? -ne 0 ]; then
    echo \"❌ Production deployment failed\"
    exit 1
fi

echo \"✅ Production promotion completed successfully\"
```

### 4. **Blue-Green Deployment**

```bash
#!/bin/bash
# blue_green_deployment.sh

CURRENT_ENV=$(dbt run-operation get_current_environment --target prod --quiet)
NEW_ENV=$([ \"$CURRENT_ENV\" == \"blue\" ] && echo \"green\" || echo \"blue\")

echo \"🔄 Switching from $CURRENT_ENV to $NEW_ENV\"

# Deploy to new environment
dbt run --target $NEW_ENV --full-refresh

# Test new environment
dbt test --target $NEW_ENV --store-failures

# Switch traffic to new environment
if [ $? -eq 0 ]; then
    echo \"✅ Switching traffic to $NEW_ENV\"
    dbt run-operation switch_environment --args \"{\\\"target_env\\\": \\\"$NEW_ENV\\\"}\"
else
    echo \"❌ Deployment failed, keeping $CURRENT_ENV active\"
    exit 1
fi
```

## 🔄 CI/CD Integration

### GitHub Actions Multi-Environment Pipeline

```yaml
# .github/workflows/multi_env_pipeline.yml
name: Multi-Environment dbt Pipeline

on:
  push:
    branches: [main, develop, staging]
  pull_request:
    branches: [main]

jobs:
  determine-environment:
    runs-on: ubuntu-latest
    outputs:
      environment: ${{ steps.set-env.outputs.environment }}
    steps:
      - name: Determine environment
        id: set-env
        run: |
          if [ \"${{ github.ref }}\" == \"refs/heads/main\" ]; then
            echo \"environment=prod\" >> $GITHUB_OUTPUT
          elif [ \"${{ github.ref }}\" == \"refs/heads/staging\" ]; then
            echo \"environment=staging\" >> $GITHUB_OUTPUT
          else
            echo \"environment=dev\" >> $GITHUB_OUTPUT
          fi

  dbt-run:
    runs-on: ubuntu-latest
    needs: determine-environment
    environment: ${{ needs.determine-environment.outputs.environment }}
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dbt
        run: pip install dbt-oracle

      - name: Setup dbt profile
        run: |
          mkdir -p ~/.dbt
          cat > ~/.dbt/profiles.yml << EOF
          aml_transaction_pipeline:
            target: ${{ needs.determine-environment.outputs.environment }}
            outputs:
              ${{ needs.determine-environment.outputs.environment }}:
                type: oracle
                host: ${{ secrets[format('{0}_DB_HOST', needs.determine-environment.outputs.environment | upper)] }}
                user: ${{ secrets[format('{0}_DB_USER', needs.determine-environment.outputs.environment | upper)] }}
                password: ${{ secrets[format('{0}_DB_PASSWORD', needs.determine-environment.outputs.environment | upper)] }}
                schema: ${{ secrets[format('{0}_DB_SCHEMA', needs.determine-environment.outputs.environment | upper)] }}
          EOF

      - name: Run dbt pipeline
        run: |
          cd aml_transaction_pipeline
          dbt run --target ${{ needs.determine-environment.outputs.environment }}
          dbt test --target ${{ needs.determine-environment.outputs.environment }}
```

### Jenkins Pipeline

```groovy
// Jenkinsfile
pipeline {
    agent any
    
    parameters {
        choice(
            name: 'ENVIRONMENT',
            choices: ['dev', 'staging', 'prod'],
            description: 'Target environment'
        )
        booleanParam(
            name: 'FULL_REFRESH',
            defaultValue: false,
            description: 'Run with full refresh'
        )
    }
    
    environment {
        DBT_PROFILES_DIR = \"${WORKSPACE}/.dbt\"
        DBT_TARGET = \"${params.ENVIRONMENT}\"
    }
    
    stages {
        stage('Setup Environment') {
            steps {
                script {
                    // Load environment-specific variables
                    def envFile = \"config/.env.${params.ENVIRONMENT}\"
                    if (fileExists(envFile)) {
                        def props = readProperties file: envFile
                        props.each { key, value ->
                            env.\"${key}\" = value
                        }
                    }
                }
            }
        }
        
        stage('Validate Configuration') {
            steps {
                sh '''
                    cd aml_transaction_pipeline
                    dbt debug --target ${ENVIRONMENT}
                '''
            }
        }
        
        stage('Run dbt Pipeline') {
            steps {
                script {
                    def dbtCommand = \"dbt run --target ${params.ENVIRONMENT}\"
                    if (params.FULL_REFRESH) {
                        dbtCommand += \" --full-refresh\"
                    }
                    
                    sh \"\"\"
                        cd aml_transaction_pipeline
                        ${dbtCommand}
                        dbt test --target ${params.ENVIRONMENT} --store-failures
                    \"\"\"
                }
            }
        }
        
        stage('Generate Documentation') {
            steps {
                sh '''
                    cd aml_transaction_pipeline
                    dbt docs generate --target ${ENVIRONMENT}
                '''
                
                publishHTML([
                    allowMissing: false,
                    alwaysLinkToLastBuild: true,
                    keepAll: true,
                    reportDir: 'aml_transaction_pipeline/target',
                    reportFiles: 'index.html',
                    reportName: 'dbt Documentation'
                ])
            }
        }
    }
    
    post {
        always {
            archiveArtifacts artifacts: 'aml_transaction_pipeline/target/**/*', fingerprint: true
        }
        failure {
            emailext (
                subject: \"dbt Pipeline Failed - ${params.ENVIRONMENT}\",
                body: \"The dbt pipeline failed for environment: ${params.ENVIRONMENT}\",
                to: \"data-team@company.com\"
            )
        }
    }
}
```

## 🏆 Best Practices

### 1. **Environment Naming Convention**

```
dev       - Development environment
staging   - Staging/UAT environment  
prod      - Production environment
test      - CI/CD testing environment
local     - Local development
sandbox   - Experimental environment
```

### 2. **Security Best Practices**

```bash
# Use environment variables for sensitive data
export PROD_DB_PASSWORD=\"$(aws secretsmanager get-secret-value --secret-id prod-db-password --query SecretString --output text)\"

# Use different service accounts per environment
DEV_DB_USER=dev_service_account
STAGING_DB_USER=staging_service_account
PROD_DB_USER=prod_service_account

# Encrypt sensitive configuration files
gpg --encrypt --recipient data-team@company.com .env.prod
```

### 3. **Data Management**

```sql
-- Environment-specific data retention
{% macro apply_data_retention() %}
  {% if target.name == 'dev' %}
    -- Keep only recent data in dev
    DELETE FROM {{ this }} WHERE created_at < CURRENT_DATE - 7
  {% elif target.name == 'staging' %}
    -- Keep 30 days in staging
    DELETE FROM {{ this }} WHERE created_at < CURRENT_DATE - 30
  {% endif %}
{% endmacro %}
```

### 4. **Resource Management**

```yaml
# Environment-specific resource allocation
models:
  aml_transaction_pipeline:
    marts:
      +materialized: |
        {%- if target.name == 'dev' -%}
          view
        {%- elif target.name == 'staging' -%}
          table
        {%- else -%}
          incremental
        {%- endif -%}
      
      +threads: |
        {%- if target.name == 'prod' -%}
          16
        {%- elif target.name == 'staging' -%}
          8
        {%- else -%}
          4
        {%- endif -%}
```

### 5. **Testing Strategy**

```yaml
# Environment-specific test configuration
tests:
  # Run all tests in staging and prod
  +enabled: \"{{ true if target.name in ['staging', 'prod'] else false }}\"
  
  # Store failures only in non-dev environments
  +store_failures: \"{{ target.name != 'dev' }}\"
  
  # Different severity levels per environment
  +severity: |
    {%- if target.name == 'prod' -%}
      error
    {%- elif target.name == 'staging' -%}
      warn
    {%- else -%}
      warn
    {%- endif -%}
```

### 6. **Monitoring and Alerting**

```sql
-- Environment-specific monitoring
{% macro setup_monitoring() %}
  {% if target.name in ['staging', 'prod'] %}
    -- Create monitoring tables
    {{ create_job_log_table() }}
    {{ create_data_quality_log_table() }}
    
    -- Setup alerting
    {% if target.name == 'prod' %}
      {{ setup_production_alerts() }}
    {% endif %}
  {% endif %}
{% endmacro %}
```

## 🚨 Troubleshooting

### Common Issues & Solutions

#### 1. **Environment Variable Not Found**

```
Error: Required environment variable 'PROD_DB_PASSWORD' is not set
```

**Solution:**
```bash
# Check if variable is set
echo $PROD_DB_PASSWORD

# Set the variable
export PROD_DB_PASSWORD=\"your_password\"

# Or load from .env file
source .env.prod
```

#### 2. **Wrong Target Environment**

```
Error: Relation 'dev_schema.customers' does not exist
```

**Solution:**
```bash
# Check current target
dbt debug --target prod

# Explicitly specify target
dbt run --target prod

# Check profiles.yml configuration
cat ~/.dbt/profiles.yml
```

#### 3. **Schema Permissions**

```
Error: Permission denied for schema 'prod_schema'
```

**Solution:**
```sql
-- Grant necessary permissions
GRANT CREATE, USAGE ON SCHEMA prod_schema TO prod_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA prod_schema TO prod_user;
```

#### 4. **Environment-specific Model Failures**

```bash
# Debug environment-specific issues
dbt run --target staging --debug --select failing_model

# Check compiled SQL
dbt compile --target staging --select failing_model
cat target/compiled/aml_transaction_pipeline/models/failing_model.sql
```

### Debug Commands

```bash
# Check environment configuration
dbt debug --target prod

# List models for specific environment
dbt ls --target staging --resource-type model

# Show compiled SQL for environment
dbt show --select model_name --target prod

# Test specific environment
dbt test --target staging --select tag:critical
```

## 📊 Environment Comparison

| Feature | Dev | Staging | Prod |
|---------|-----|---------|------|
| **Data Volume** | Sample (1K rows) | Subset (100K rows) | Full (Millions) |
| **Threads** | 2-4 | 4-8 | 8-16 |
| **Materialization** | Views | Tables | Incremental |
| **Tests** | Critical only | All tests | All tests |
| **Monitoring** | Basic | Enhanced | Full |
| **Alerts** | None | Email | Slack + Email |
| **Retention** | 7 days | 30 days | 365 days |
| **Backups** | None | Daily | Hourly |

---

## 🎯 Kết luận

**Multi-Environment Configuration là foundation của enterprise dbt deployment:**

- ✅ **Isolated Development** - Safe development và testing
- ✅ **Automated Deployment** - CI/CD pipeline với multiple environments
- ✅ **Environment-specific Configuration** - Tối ưu cho từng môi trường
- ✅ **Data Governance** - Kiểm soát access và permissions
- ✅ **Scalable Architecture** - Dễ mở rộng và maintain

**Implementation Strategy:**
1. **Start Simple** - Dev và Prod environments
2. **Add Staging** - Comprehensive testing environment
3. **Implement CI/CD** - Automated deployment pipeline
4. **Add Monitoring** - Observability và alerting
5. **Optimize Performance** - Environment-specific tuning

**Remember:** Good multi-environment setup = Reliable deployments = Happy teams! 🎯

---

**Tác giả:** VPAURA  
**Ngày:** 2025-12-30  
**Slogan:** `Code chay như muối bỏ bể, nhờ VPAura thì ngon luôn` 🚀
