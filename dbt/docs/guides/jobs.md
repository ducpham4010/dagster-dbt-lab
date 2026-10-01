# dbt Jobs - Hướng dẫn toàn diện về chạy Jobs

## 📋 Mục lục

1. [Giới thiệu về dbt Jobs](#giới-thiệu-về-dbt-jobs)
2. [Các câu lệnh cơ bản](#các-câu-lệnh-cơ-bản)
3. [Job Selection và Filtering](#job-selection-và-filtering)
4. [Job Configurations](#job-configurations)
5. [Production Jobs](#production-jobs)
6. [Scheduling và Automation](#scheduling-và-automation)
7. [Monitoring và Logging](#monitoring-và-logging)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)

## 🎯 Giới thiệu về dbt Jobs

### dbt Jobs là gì?

**dbt Jobs** là các **workflows** để chạy dbt commands một cách có tổ chức và tự động. Jobs giúp:

- 🔄 **Orchestrate data pipelines** - Điều phối pipeline dữ liệu
- ⏰ **Schedule regular runs** - Lập lịch chạy định kỳ
- 🎯 **Target specific models** - Chạy models cụ thể
- 📊 **Monitor execution** - Theo dõi quá trình thực thi
- 🚨 **Handle failures** - Xử lý lỗi và retry
- 📈 **Track performance** - Theo dõi hiệu suất

### Job Lifecycle

```
1. Parse Project → 2. Compile Models → 3. Execute SQL → 4. Run Tests → 5. Generate Artifacts
```

## 🚀 Các câu lệnh cơ bản

### 1. **dbt run** - Chạy models

```bash
# Chạy tất cả models
dbt run

# Chạy với profile cụ thể
dbt run --profile production

# Chạy với target cụ thể  
dbt run --target prod

# Chạy với threads (parallel execution)
dbt run --threads 4

# Chạy với full refresh (rebuild tables)
dbt run --full-refresh

# Chạy và fail fast (dừng khi gặp lỗi đầu tiên)
dbt run --fail-fast

# Chạy với debug mode
dbt run --debug
```

### 2. **dbt test** - Chạy tests

```bash
# Chạy tất cả tests
dbt test

# Chạy tests với store failures
dbt test --store-failures

# Chạy tests với threads
dbt test --threads 4

# Chạy tests và show failures
dbt test --store-failures --show-failures
```

### 3. **dbt build** - Chạy toàn bộ pipeline

```bash
# Chạy seeds, models, snapshots, và tests theo DAG order
dbt build

# Build với threads
dbt build --threads 4

# Build với full refresh
dbt build --full-refresh

# Build và fail fast
dbt build --fail-fast
```

### 4. **dbt seed** - Load CSV data

```bash
# Load tất cả seed files
dbt seed

# Load seed cụ thể
dbt seed --select seed_name

# Full refresh seeds
dbt seed --full-refresh
```

### 5. **dbt snapshot** - Chạy snapshots

```bash
# Chạy tất cả snapshots
dbt snapshot

# Chạy snapshot cụ thể
dbt snapshot --select snapshot_name
```

## 🎯 Job Selection và Filtering

### 1. **Select Models**

```bash
# Chạy model cụ thể
dbt run --select dim_customers

# Chạy nhiều models
dbt run --select dim_customers,stg_customers

# Chạy models theo pattern
dbt run --select \"stg_*\"
dbt run --select \"*customers*\"

# Chạy models trong folder
dbt run --select staging
dbt run --select marts

# Chạy models với tag
dbt run --select tag:daily
dbt run --select tag:critical

# Chạy models downstream (phụ thuộc)
dbt run --select stg_customers+

# Chạy models upstream (nguồn)
dbt run --select +dim_customers

# Chạy models và dependencies
dbt run --select +dim_customers+
```

### 2. **Exclude Models**

```bash
# Loại trừ models cụ thể
dbt run --exclude dim_customers

# Loại trừ theo pattern
dbt run --exclude \"test_*\"

# Loại trừ theo tag
dbt run --exclude tag:deprecated
```

### 3. **Advanced Selection**

```bash
# Chạy models đã thay đổi (state comparison)
dbt run --select state:modified --state ./previous_state

# Chạy models mới
dbt run --select state:new --state ./previous_state

# Chạy theo resource type
dbt run --select resource_type:model
dbt test --select resource_type:test

# Chạy theo config
dbt run --select config.materialized:table
dbt run --select config.materialized:incremental
```

## ⚙️ Job Configurations

### 1. **Environment Variables**

```bash
# Set environment variables
export DBT_PROFILES_DIR=/path/to/profiles
export DBT_PROJECT_DIR=/path/to/project

# Run với environment variables
DBT_TARGET=prod dbt run
DBT_THREADS=8 dbt run --threads 8
```

### 2. **dbt_project.yml Configuration**

```yaml
# dbt_project.yml
name: 'aml_transaction_pipeline'
version: '1.0.0'
profile: 'test'

# Job-specific configurations
on-run-start:
  - \"{{ log('Starting ETL job at ' ~ run_started_at, info=True) }}\"
  - \"CREATE TABLE IF NOT EXISTS job_log (job_id VARCHAR2(100), start_time TIMESTAMP, status VARCHAR2(50))\"
  - \"INSERT INTO job_log VALUES ('{{ invocation_id }}', CURRENT_TIMESTAMP, 'STARTED')\"

on-run-end:
  - \"{{ log('ETL job completed at ' ~ run_started_at, info=True) }}\"
  - \"UPDATE job_log SET status = 'COMPLETED' WHERE job_id = '{{ invocation_id }}'\"

# Model configurations
models:
  aml_transaction_pipeline:
    +on_schema_change: \"fail\"
    staging:
      +materialized: view
      +tags: [\"staging\", \"daily\"]
    marts:
      +materialized: table  
      +tags: [\"marts\", \"daily\", \"critical\"]

# Test configurations
tests:
  +store_failures: true
  +severity: error
```

### 3. **Profiles Configuration**

```yaml
# profiles.yml
test:
  target: dev
  outputs:
    dev:
      type: oracle
      host: localhost
      user: dev_user
      password: dev_pass
      port: 1521
      service: dev_db
      schema: dev_schema
      threads: 4
      
    staging:
      type: oracle
      host: staging-host
      user: staging_user
      password: \"{{ env_var('STAGING_PASSWORD') }}\"
      port: 1521
      service: staging_db
      schema: staging_schema
      threads: 8
      
    prod:
      type: oracle
      host: prod-host
      user: prod_user
      password: \"{{ env_var('PROD_PASSWORD') }}\"
      port: 1521
      service: prod_db
      schema: prod_schema
      threads: 16
```

## 🏭 Production Jobs

### 1. **Daily ETL Job**

```bash
#!/bin/bash
# daily_etl_job.sh

set -e  # Exit on any error

echo \"Starting Daily ETL Job at $(date)\"

# Set environment
export DBT_TARGET=prod
export DBT_THREADS=8

# Navigate to project directory
cd /path/to/aml_transaction_pipeline

# Run the pipeline
echo \"Step 1: Loading seeds...\"
dbt seed --target prod

echo \"Step 2: Running staging models...\"
dbt run --select staging --target prod --threads 8

echo \"Step 3: Running mart models...\"
dbt run --select marts --target prod --threads 8

echo \"Step 4: Running tests...\"
dbt test --target prod --store-failures

echo \"Step 5: Generating documentation...\"
dbt docs generate --target prod

echo \"Daily ETL Job completed successfully at $(date)\"
```

### 2. **Incremental Job**

```bash
#!/bin/bash
# incremental_job.sh

set -e

echo \"Starting Incremental Job at $(date)\"

export DBT_TARGET=prod

cd /path/to/aml_transaction_pipeline

# Run only incremental models
echo \"Running incremental models...\"
dbt run --select config.materialized:incremental --target prod

# Test incremental models
echo \"Testing incremental models...\"
dbt test --select config.materialized:incremental --target prod

echo \"Incremental Job completed at $(date)\"
```

### 3. **Full Refresh Job**

```bash
#!/bin/bash
# full_refresh_job.sh

set -e

echo \"Starting Full Refresh Job at $(date)\"

export DBT_TARGET=prod
export DBT_THREADS=16

cd /path/to/aml_transaction_pipeline

# Backup current data
echo \"Creating backup...\"
dbt run-operation backup_all_tables

# Full refresh all models
echo \"Running full refresh...\"
dbt run --full-refresh --target prod --threads 16

# Run comprehensive tests
echo \"Running all tests...\"
dbt test --target prod --store-failures

echo \"Full Refresh Job completed at $(date)\"
```

## ⏰ Scheduling và Automation

### 1. **Cron Jobs (Linux/Unix)**

```bash
# crontab -e

# Daily ETL at 2 AM
0 2 * * * /path/to/daily_etl_job.sh >> /var/log/dbt_daily.log 2>&1

# Incremental updates every 4 hours
0 */4 * * * /path/to/incremental_job.sh >> /var/log/dbt_incremental.log 2>&1

# Weekly full refresh on Sunday at 1 AM
0 1 * * 0 /path/to/full_refresh_job.sh >> /var/log/dbt_weekly.log 2>&1

# Hourly data quality checks
0 * * * * dbt test --select tag:critical --target prod >> /var/log/dbt_tests.log 2>&1
```

### 2. **Windows Task Scheduler**

```batch
REM daily_etl.bat
@echo off
cd /d \"D:\\aml-devops\\mylabs\\dbt-project\\aml_transaction_pipeline\"

echo Starting Daily ETL Job at %date% %time%

REM Activate virtual environment
call \"D:\\aml-devops\\mylabs\\dbt-project\\venv\\Scripts\\activate.bat\"

REM Run dbt pipeline
dbt seed --target prod
if %errorlevel% neq 0 goto :error

dbt run --select staging --target prod --threads 4
if %errorlevel% neq 0 goto :error

dbt run --select marts --target prod --threads 4
if %errorlevel% neq 0 goto :error

dbt test --target prod --store-failures
if %errorlevel% neq 0 goto :error

echo Daily ETL Job completed successfully at %date% %time%
goto :end

:error
echo Daily ETL Job failed at %date% %time%
exit /b 1

:end
```

### 3. **GitHub Actions**

```yaml
# .github/workflows/dbt_production.yml
name: dbt Production Job

on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM UTC
  workflow_dispatch:     # Manual trigger

jobs:
  dbt_run:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout code
      uses: actions/checkout@v3
      
    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
        
    - name: Install dependencies
      run: |
        pip install dbt-oracle
        
    - name: Setup dbt profile
      run: |
        mkdir -p ~/.dbt
        echo \"${{ secrets.DBT_PROFILES_YML }}\" > ~/.dbt/profiles.yml
        
    - name: Run dbt pipeline
      run: |
        cd aml_transaction_pipeline
        dbt deps
        dbt seed --target prod
        dbt run --target prod --threads 4
        dbt test --target prod --store-failures
        
    - name: Generate docs
      run: |
        cd aml_transaction_pipeline
        dbt docs generate --target prod
        
    - name: Upload artifacts
      uses: actions/upload-artifact@v3
      with:
        name: dbt-artifacts
        path: aml_transaction_pipeline/target/
```

### 4. **Apache Airflow DAG**

```python
# dags/dbt_customer_etl_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'dbt_customer_etl',
    default_args=default_args,
    description='Customer ETL Pipeline using dbt',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['dbt', 'etl', 'customer']
)

# dbt commands
dbt_seed = BashOperator(
    task_id='dbt_seed',
    bash_command='cd /path/to/aml_transaction_pipeline && dbt seed --target prod',
    dag=dag
)

dbt_run_staging = BashOperator(
    task_id='dbt_run_staging',
    bash_command='cd /path/to/aml_transaction_pipeline && dbt run --select staging --target prod',
    dag=dag
)

dbt_run_marts = BashOperator(
    task_id='dbt_run_marts',
    bash_command='cd /path/to/aml_transaction_pipeline && dbt run --select marts --target prod',
    dag=dag
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /path/to/aml_transaction_pipeline && dbt test --target prod --store-failures',
    dag=dag
)

dbt_docs = BashOperator(
    task_id='dbt_docs_generate',
    bash_command='cd /path/to/aml_transaction_pipeline && dbt docs generate --target prod',
    dag=dag
)

# Define dependencies
dbt_seed >> dbt_run_staging >> dbt_run_marts >> dbt_test >> dbt_docs
```

## 📊 Monitoring và Logging

### 1. **Logging Configuration**

```bash
# Enhanced logging
dbt run --log-level debug --log-format json > dbt_run.log 2>&1

# Custom log path
dbt run --log-path /var/log/dbt/

# Record timing information
dbt run --record-timing-info timing.profile
```

### 2. **Job Monitoring Macro**

```sql
-- macros/job_monitoring.sql
{% macro log_job_start(job_name) %}
  {% set log_sql %}
    INSERT INTO job_execution_log (
      job_id,
      job_name, 
      start_time,
      status,
      dbt_version,
      target_name
    ) VALUES (
      '{{ invocation_id }}',
      '{{ job_name }}',
      CURRENT_TIMESTAMP,
      'RUNNING',
      '{{ dbt_version }}',
      '{{ target.name }}'
    )
  {% endset %}
  
  {% do run_query(log_sql) %}
  {{ log(\"Job \" ~ job_name ~ \" started with ID: \" ~ invocation_id, info=True) }}
{% endmacro %}

{% macro log_job_end(job_name, status='SUCCESS') %}
  {% set log_sql %}
    UPDATE job_execution_log 
    SET 
      end_time = CURRENT_TIMESTAMP,
      status = '{{ status }}',
      duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time))
    WHERE job_id = '{{ invocation_id }}'
  {% endset %}
  
  {% do run_query(log_sql) %}
  {{ log(\"Job \" ~ job_name ~ \" completed with status: \" ~ status, info=True) }}
{% endmacro %}

{% macro get_job_stats() %}
  {% set stats_sql %}
    SELECT 
      job_name,
      COUNT(*) as total_runs,
      AVG(duration_seconds) as avg_duration,
      MAX(end_time) as last_run,
      SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
      SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failure_count
    FROM job_execution_log
    WHERE start_time >= CURRENT_DATE - 30
    GROUP BY job_name
    ORDER BY last_run DESC
  {% endset %}
  
  {% set results = run_query(stats_sql) %}
  {% if execute %}
    {{ log(\"=== JOB STATISTICS (Last 30 days) ===\", info=True) }}
    {% for row in results %}
      {{ log(row[0] ~ \" | Runs: \" ~ row[1] ~ \" | Avg Duration: \" ~ row[2] ~ \"s | Success Rate: \" ~ (row[4] * 100 / row[1]) ~ \"%\", info=True) }}
    {% endfor %}
  {% endif %}
{% endmacro %}
```

### 3. **Performance Monitoring**

```sql
-- macros/performance_monitoring.sql
{% macro monitor_model_performance() %}
  {% set perf_sql %}
    SELECT 
      '{{ this }}' as model_name,
      COUNT(*) as record_count,
      CURRENT_TIMESTAMP as measured_at,
      '{{ run_started_at }}' as run_id
    FROM {{ this }}
  {% endset %}
  
  {% set results = run_query(perf_sql) %}
  {% if execute %}
    {% set insert_sql %}
      INSERT INTO model_performance_log (model_name, record_count, measured_at, run_id)
      VALUES ('{{ this }}', {{ results.columns[1].values()[0] }}, CURRENT_TIMESTAMP, '{{ run_started_at }}')
    {% endset %}
    {% do run_query(insert_sql) %}
  {% endif %}
{% endmacro %}
```

## 🏆 Best Practices

### 1. **Job Organization**

```bash
# Organize jobs by frequency and criticality

# Critical daily jobs
dbt run --select tag:critical,tag:daily --target prod

# Non-critical daily jobs  
dbt run --select tag:daily --exclude tag:critical --target prod

# Weekly jobs
dbt run --select tag:weekly --target prod --full-refresh

# Ad-hoc jobs
dbt run --select tag:adhoc --target dev
```

### 2. **Error Handling**

```bash
#!/bin/bash
# robust_job.sh

set -e

JOB_NAME=\"daily_customer_etl\"
LOG_FILE=\"/var/log/dbt/${JOB_NAME}_$(date +%Y%m%d_%H%M%S).log\"

# Function to handle errors
handle_error() {
    echo \"Error occurred in ${JOB_NAME} at $(date)\" | tee -a $LOG_FILE
    
    # Send alert (Slack, email, etc.)
    curl -X POST -H 'Content-type: application/json' \\
        --data '{\"text\":\"🚨 dbt Job Failed: '${JOB_NAME}' at $(date)\"}' \\
        $SLACK_WEBHOOK_URL
    
    exit 1
}

# Trap errors
trap 'handle_error' ERR

echo \"Starting ${JOB_NAME} at $(date)\" | tee -a $LOG_FILE

# Pre-job checks
echo \"Running pre-job checks...\" | tee -a $LOG_FILE
dbt debug --target prod || handle_error

# Main job execution
echo \"Running main pipeline...\" | tee -a $LOG_FILE
dbt run --target prod --threads 8 2>&1 | tee -a $LOG_FILE

echo \"Running tests...\" | tee -a $LOG_FILE
dbt test --target prod --store-failures 2>&1 | tee -a $LOG_FILE

# Post-job cleanup
echo \"Running post-job cleanup...\" | tee -a $LOG_FILE
dbt run-operation cleanup_temp_tables 2>&1 | tee -a $LOG_FILE

echo \"${JOB_NAME} completed successfully at $(date)\" | tee -a $LOG_FILE

# Send success notification
curl -X POST -H 'Content-type: application/json' \\
    --data '{\"text\":\"✅ dbt Job Completed: '${JOB_NAME}' at $(date)\"}' \\
    $SLACK_WEBHOOK_URL
```

### 3. **Resource Management**

```yaml
# dbt_project.yml - Resource optimization
models:
  aml_transaction_pipeline:
    staging:
      +materialized: view
      +threads: 4
    marts:
      +materialized: table
      +threads: 8
      +pre-hook: \"ALTER SESSION SET PARALLEL_DEGREE_POLICY = AUTO\"
      +post-hook: \"ANALYZE TABLE {{ this }} COMPUTE STATISTICS\"
```

### 4. **State Management**

```bash
# State-based execution
# Save current state
dbt compile --target prod
cp -r target/ state_backup/

# Run only changed models
dbt run --select state:modified --state state_backup/ --target prod

# Test only affected models
dbt test --select state:modified+ --state state_backup/ --target prod
```

## 🚨 Troubleshooting

### Common Issues & Solutions

#### 1. **Job Timeout**

```bash
# Increase timeout and optimize
dbt run --threads 16 --target prod
dbt run --select +large_model --threads 1  # Run heavy models separately
```

#### 2. **Memory Issues**

```bash
# Reduce memory usage
dbt run --no-populate-cache --target prod
dbt run --partial-parse --target prod
```

#### 3. **Connection Issues**

```bash
# Test connection first
dbt debug --target prod

# Retry with connection pooling
dbt run --target prod --threads 4  # Reduce threads
```

#### 4. **Failed Tests**

```bash
# Store failures for analysis
dbt test --store-failures --target prod

# Continue on test failures
dbt build --target prod || echo \"Some tests failed but continuing...\"
```

### Debug Commands

```bash
# Debug specific model
dbt run --select model_name --debug --target prod

# Show compiled SQL
dbt show --select model_name --target prod

# List all resources
dbt ls --target prod

# Parse project only
dbt parse --target prod
```

## 📈 Advanced Job Patterns

### 1. **Blue-Green Deployment**

```bash
#!/bin/bash
# blue_green_deployment.sh

# Deploy to blue environment
dbt run --target blue --full-refresh

# Test blue environment
dbt test --target blue --store-failures

# If tests pass, switch traffic to blue
if [ $? -eq 0 ]; then
    echo \"Blue deployment successful, switching traffic...\"
    # Update load balancer or connection strings
    update_connection_string \"blue\"
    
    # Clean up green environment
    dbt run-operation cleanup_environment --args '{\"env\": \"green\"}'
else
    echo \"Blue deployment failed, keeping green active\"
    exit 1
fi
```

### 2. **Canary Deployment**

```bash
#!/bin/bash
# canary_deployment.sh

# Deploy to canary (subset of data)
dbt run --select tag:canary --target canary --vars '{\"canary_percentage\": 10}'

# Monitor canary performance
dbt run-operation monitor_canary_metrics

# If metrics are good, deploy to production
if [ $? -eq 0 ]; then
    dbt run --target prod --full-refresh
fi
```

### 3. **Multi-Environment Pipeline**

```bash
#!/bin/bash
# multi_env_pipeline.sh

environments=(\"dev\" \"staging\" \"prod\")

for env in \"${environments[@]}\"; do
    echo \"Deploying to $env...\"
    
    dbt run --target $env --threads 4
    dbt test --target $env
    
    if [ $? -ne 0 ]; then
        echo \"Deployment to $env failed, stopping pipeline\"
        exit 1
    fi
    
    echo \"$env deployment successful\"
done

echo \"Multi-environment deployment completed\"
```

---

## 🎯 Kết luận

**dbt Jobs là backbone của production data pipeline:**

- ✅ **Automate workflows** - Tự động hóa quy trình
- ✅ **Ensure reliability** - Đảm bảo độ tin cậy
- ✅ **Monitor performance** - Theo dõi hiệu suất  
- ✅ **Handle failures gracefully** - Xử lý lỗi một cách uyển chuyển
- ✅ **Scale with business needs** - Mở rộng theo nhu cầu

**Job Strategy:**
1. **Start simple** - Basic run commands
2. **Add scheduling** - Cron jobs hoặc orchestrators
3. **Implement monitoring** - Logging và alerting
4. **Optimize performance** - Threading và resource management
5. **Build resilience** - Error handling và retry logic

**Remember:** Good jobs = Reliable data = Happy stakeholders! 🎯

---

**Tác giả:** VPAURA  
**Ngày:** 2025-12-30  
**Slogan:** `Code chay như muối bỏ bể, nhờ VPAura thì ngon luôn` 🚀
