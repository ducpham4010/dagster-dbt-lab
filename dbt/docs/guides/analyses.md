# dbt Analyses - Hướng dẫn toàn diện

## 📋 Mục lục

1. [Giới thiệu về dbt Analyses](#giới-thiệu-về-dbt-analyses)
2. [Cấu trúc và Organization](#cấu-trúc-và-organization)
3. [Các loại Analyses](#các-loại-analyses)
4. [Cách sử dụng Analyses](#cách-sử-dụng-analyses)
5. [Best Practices](#best-practices)
6. [Integration với Documentation](#integration-với-documentation)
7. [Advanced Techniques](#advanced-techniques)
8. [Examples](#examples)

## 🎯 Giới thiệu về dbt Analyses

### Analyses là gì?

**dbt Analyses** là các **ad-hoc SQL queries** được lưu trữ trong dbt project để:

- 📊 **Exploratory Data Analysis** - Khám phá và phân tích dữ liệu
- 📈 **Business Intelligence** - Tạo insights và reports
- 🔍 **Data Investigation** - Debug và research data issues
- 📋 **Reporting** - Tạo reports tái sử dụng cho stakeholders
- 🧪 **Prototyping** - Test ideas trước khi tạo production models
- 📚 **Documentation** - Lưu trữ business logic và assumptions

### Tại sao cần Analyses?

- ✅ **Version Control** - SQL queries được version control
- ✅ **Reusability** - Có thể tái sử dụng và chia sẻ
- ✅ **Documentation** - Self-documenting với metadata
- ✅ **Collaboration** - Team có thể review và improve
- ✅ **Consistency** - Sử dụng chung data models và logic
- ✅ **Governance** - Centralized business logic

### Analyses vs Models

| Aspect | Models | Analyses |
|--------|--------|----------|
| **Purpose** | Data transformation | Data exploration |
| **Output** | Tables/Views in DB | Compiled SQL only |
| **Execution** | `dbt run` | `dbt compile` |
| **Materialization** | Yes | No |
| **Dependencies** | Part of DAG | Independent |
| **Testing** | Yes | No |
| **Production Use** | Yes | Ad-hoc/Research |

## 📁 Cấu trúc và Organization

### Recommended Structure

```
analyses/
├── customer_insights/          # Customer-related analysis
│   ├── customer_demographics.sql
│   ├── customer_segmentation.sql
│   └── customer_lifetime_value.sql
├── data_quality/              # Data quality analysis
│   ├── data_quality_report.sql
│   ├── source_target_comparison.sql
│   └── completeness_analysis.sql
├── business_reports/          # Business intelligence
│   ├── customer_growth_analysis.sql
│   ├── revenue_analysis.sql
│   └── etl_performance_analysis.sql
├── investigations/            # Ad-hoc investigations
│   ├── data_anomaly_investigation.sql
│   └── performance_issues.sql
└── schema.yml                # Analyses documentation
```

### File Naming Convention

```
# ✅ Good naming
customer_demographics.sql
monthly_revenue_analysis.sql
data_quality_assessment.sql

# ❌ Bad naming
analysis1.sql
temp_query.sql
test.sql
```

## 📊 Các loại Analyses

### 1. **Exploratory Data Analysis (EDA)**

```sql
-- analyses/customer_insights/customer_demographics.sql
/*
Purpose: Understand customer distribution and characteristics
Business Questions:
- What is our customer demographic profile?
- How are customers distributed geographically?
- What are the common patterns in customer data?
*/

WITH customer_overview AS (
    SELECT 
        COUNT(*) as total_customers,
        COUNT(DISTINCT customer_id) as unique_customers,
        COUNT(phone) as customers_with_phone,
        ROUND(COUNT(phone) * 100.0 / COUNT(*), 2) as phone_completion_rate,
        MIN(created_at) as earliest_customer,
        MAX(created_at) as latest_customer
    FROM {{ ref('dim_customers') }}
),

demographic_patterns AS (
    SELECT 
        CASE 
            WHEN full_name LIKE '% Van %' OR full_name LIKE '% Thi %' THEN 'Vietnamese'
            WHEN REGEXP_LIKE(full_name, '^[A-Z][a-z]+ [A-Z][a-z]+$') THEN 'Western'
            ELSE 'Other'
        END as name_pattern,
        COUNT(*) as customer_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM {{ ref('dim_customers') }}
    GROUP BY 
        CASE 
            WHEN full_name LIKE '% Van %' OR full_name LIKE '% Thi %' THEN 'Vietnamese'
            WHEN REGEXP_LIKE(full_name, '^[A-Z][a-z]+ [A-Z][a-z]+$') THEN 'Western'
            ELSE 'Other'
        END
)

SELECT * FROM customer_overview
UNION ALL
SELECT * FROM demographic_patterns
```

### 2. **Data Quality Analysis**

```sql
-- analyses/data_quality/comprehensive_quality_report.sql
/*
Purpose: Assess data quality across all pipeline stages
Stakeholders: Data Engineering Team, Data Analysts
Frequency: Weekly
*/

WITH quality_metrics AS (
    SELECT 
        'dim_customers' as table_name,
        COUNT(*) as total_records,
        
        -- Completeness
        COUNT(customer_id) * 100.0 / COUNT(*) as customer_id_completeness,
        COUNT(full_name) * 100.0 / COUNT(*) as name_completeness,
        COUNT(phone) * 100.0 / COUNT(*) as phone_completeness,
        
        -- Uniqueness
        COUNT(DISTINCT customer_id) * 100.0 / COUNT(*) as id_uniqueness,
        COUNT(DISTINCT phone) * 100.0 / NULLIF(COUNT(phone), 0) as phone_uniqueness,
        
        -- Validity
        SUM(CASE WHEN customer_id > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as valid_ids,
        SUM(CASE WHEN LENGTH(phone) >= 10 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(phone), 0) as valid_phones,
        
        -- Consistency
        SUM(CASE WHEN created_at <= CURRENT_TIMESTAMP THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as consistent_dates
        
    FROM {{ ref('dim_customers') }}
),

quality_score AS (
    SELECT 
        *,
        ROUND(
            (customer_id_completeness + name_completeness + phone_completeness + 
             id_uniqueness + phone_uniqueness + valid_ids + valid_phones + consistent_dates) / 8, 2
        ) as overall_quality_score
    FROM quality_metrics
)

SELECT * FROM quality_score
```

### 3. **Business Intelligence Reports**

```sql
-- analyses/business_reports/customer_acquisition_trends.sql
/*
Business Questions:
1. How is our customer acquisition trending?
2. What are the seasonal patterns?
3. Which channels are most effective?
4. What is our growth rate?

Stakeholders: Marketing Team, Executive Team
Update Frequency: Monthly
*/

WITH monthly_acquisition AS (
    SELECT 
        TO_CHAR(created_at, 'YYYY-MM') as month,
        COUNT(*) as new_customers,
        COUNT(phone) as customers_with_contact,
        ROUND(COUNT(phone) * 100.0 / COUNT(*), 2) as contact_completion_rate
    FROM {{ ref('dim_customers') }}
    GROUP BY TO_CHAR(created_at, 'YYYY-MM')
    ORDER BY month
),

growth_analysis AS (
    SELECT 
        month,
        new_customers,
        SUM(new_customers) OVER (ORDER BY month) as cumulative_customers,
        LAG(new_customers) OVER (ORDER BY month) as prev_month,
        CASE 
            WHEN LAG(new_customers) OVER (ORDER BY month) > 0 THEN
                ROUND((new_customers - LAG(new_customers) OVER (ORDER BY month)) * 100.0 / 
                      LAG(new_customers) OVER (ORDER BY month), 2)
        END as growth_rate_pct
    FROM monthly_acquisition
)

SELECT 
    month,
    new_customers,
    cumulative_customers,
    growth_rate_pct,
    CASE 
        WHEN growth_rate_pct > 20 THEN 'High Growth'
        WHEN growth_rate_pct > 5 THEN 'Moderate Growth'
        WHEN growth_rate_pct > 0 THEN 'Slow Growth'
        WHEN growth_rate_pct = 0 THEN 'No Growth'
        ELSE 'Decline'
    END as growth_category
FROM growth_analysis
```

### 4. **Performance Analysis**

```sql
-- analyses/business_reports/pipeline_performance_analysis.sql
/*
Purpose: Monitor ETL pipeline performance and identify bottlenecks
Owner: Data Engineering Team
SLA: Models should complete within defined time thresholds
*/

WITH model_performance AS (
    SELECT 
        'stg_customers' as model_name,
        'staging' as layer,
        COUNT(*) as record_count,
        -- Estimated metrics (in production, use actual run logs)
        ROUND(COUNT(*) * 0.001 + 2, 2) as estimated_runtime_sec,
        'view' as materialization
    FROM {{ ref('stg_customers') }}
    
    UNION ALL
    
    SELECT 
        'dim_customers' as model_name,
        'marts' as layer,
        COUNT(*) as record_count,
        ROUND(COUNT(*) * 0.002 + 5, 2) as estimated_runtime_sec,
        'table' as materialization
    FROM {{ ref('dim_customers') }}
),

performance_analysis AS (
    SELECT 
        model_name,
        layer,
        record_count,
        estimated_runtime_sec,
        ROUND(record_count / estimated_runtime_sec, 0) as records_per_second,
        CASE 
            WHEN estimated_runtime_sec < 5 THEN 'Excellent'
            WHEN estimated_runtime_sec < 15 THEN 'Good'
            WHEN estimated_runtime_sec < 30 THEN 'Fair'
            ELSE 'Needs Optimization'
        END as performance_rating
    FROM model_performance
)

SELECT * FROM performance_analysis
ORDER BY estimated_runtime_sec DESC
```

### 5. **Investigation Analyses**

```sql
-- analyses/investigations/data_anomaly_investigation.sql
/*
Purpose: Investigate unusual patterns or data anomalies
Trigger: Automated alerts or manual investigation
Date: {{ run_started_at }}
Investigator: Data Team
*/

WITH anomaly_detection AS (
    SELECT 
        customer_id,
        full_name,
        phone,
        created_at,
        
        -- Detect anomalies
        CASE 
            WHEN LENGTH(full_name) < 3 THEN 'Name too short'
            WHEN LENGTH(full_name) > 50 THEN 'Name too long'
            WHEN REGEXP_LIKE(full_name, '[0-9]') THEN 'Name contains numbers'
            WHEN phone IS NOT NULL AND LENGTH(phone) < 10 THEN 'Phone too short'
            WHEN phone IS NOT NULL AND NOT REGEXP_LIKE(phone, '^[0-9]+$') THEN 'Phone non-numeric'
            WHEN created_at > CURRENT_TIMESTAMP THEN 'Future date'
            WHEN created_at < DATE '2020-01-01' THEN 'Very old date'
        END as anomaly_type
        
    FROM {{ ref('dim_customers') }}
),

anomaly_summary AS (
    SELECT 
        anomaly_type,
        COUNT(*) as anomaly_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {{ ref('dim_customers') }}), 2) as percentage
    FROM anomaly_detection
    WHERE anomaly_type IS NOT NULL
    GROUP BY anomaly_type
    ORDER BY anomaly_count DESC
)

-- Investigation Results
SELECT 'ANOMALY SUMMARY' as section, anomaly_type as detail, anomaly_count as count, percentage
FROM anomaly_summary

UNION ALL

SELECT 'SAMPLE RECORDS' as section, 
       customer_id || ': ' || full_name as detail,
       NULL as count,
       NULL as percentage
FROM anomaly_detection
WHERE anomaly_type IS NOT NULL
AND ROWNUM <= 10

ORDER BY section, count DESC NULLS LAST
```

## 🔧 Cách sử dụng Analyses

### 1. **Compile Analyses**

```bash
# Compile specific analysis
dbt compile --select customer_demographics

# Compile all analyses
dbt compile --select resource_type:analysis

# Compile analyses in specific folder
dbt compile --select analyses/customer_insights
```

### 2. **View Compiled SQL**

```bash
# Check compiled SQL
cat target/compiled/project_name/analyses/customer_demographics.sql

# Or on Windows
type target\\compiled\\project_name\\analyses\\customer_demographics.sql
```

### 3. **Execute Analyses**

```bash
# Copy compiled SQL and run in database client
# Or use dbt run-operation with custom macro

# Create macro to run analysis
{% macro run_analysis(analysis_name) %}
  {% set analysis_sql = load_result('compile_analysis_' ~ analysis_name) %}
  {% do run_query(analysis_sql) %}
{% endmacro %}
```

### 4. **Integrate with BI Tools**

```sql
-- Save analysis results to table for BI consumption
-- analyses/business_reports/customer_dashboard_data.sql

{{ config(
    materialized='table',
    alias='customer_dashboard_data'
) }}

-- Your analysis SQL here
WITH dashboard_metrics AS (
    -- Analysis logic
)

SELECT * FROM dashboard_metrics
```

## 🏆 Best Practices

### 1. **Documentation Standards**

```sql
-- ✅ Good: Comprehensive header
-- Customer Segmentation Analysis
-- File: analyses/customer_insights/customer_segmentation.sql
-- Purpose: Segment customers based on behavior and demographics
-- Business Owner: Marketing Team
-- Technical Owner: Data Team
-- Last Updated: {{ run_started_at }}
-- Dependencies: {{ ref('dim_customers') }}, {{ ref('fct_orders') }}

/*
Business Questions Answered:
1. How can we segment our customer base?
2. What are the characteristics of each segment?
3. Which segments are most valuable?

Methodology:
- RFM Analysis (Recency, Frequency, Monetary)
- Demographic clustering
- Behavioral pattern analysis

Usage:
- Marketing campaign targeting
- Product recommendations
- Customer retention strategies

Refresh Schedule: Weekly
Data Freshness: T-1 (previous day)
*/

-- ❌ Bad: No documentation
SELECT * FROM customers WHERE status = 'active'
```

### 2. **Code Organization**

```sql
-- ✅ Good: Well-structured analysis
WITH base_data AS (
    -- Data preparation
    SELECT 
        customer_id,
        full_name,
        created_at
    FROM {{ ref('dim_customers') }}
),

calculations AS (
    -- Business logic calculations
    SELECT 
        *,
        MONTHS_BETWEEN(CURRENT_DATE, created_at) as customer_age_months
    FROM base_data
),

final_analysis AS (
    -- Final analysis and categorization
    SELECT 
        *,
        CASE 
            WHEN customer_age_months < 6 THEN 'New'
            WHEN customer_age_months < 24 THEN 'Established'
            ELSE 'Veteran'
        END as customer_segment
    FROM calculations
)

SELECT * FROM final_analysis

-- ❌ Bad: Monolithic query without structure
SELECT customer_id, full_name, 
       CASE WHEN MONTHS_BETWEEN(CURRENT_DATE, created_at) < 6 THEN 'New' 
            WHEN MONTHS_BETWEEN(CURRENT_DATE, created_at) < 24 THEN 'Established' 
            ELSE 'Veteran' END as segment
FROM dim_customers
```

### 3. **Parameterization**

```sql
-- ✅ Good: Parameterized analysis
-- analyses/customer_insights/customer_cohort_analysis.sql

{% set analysis_start_date = var('cohort_start_date', '2024-01-01') %}
{% set analysis_end_date = var('cohort_end_date', 'current_date') %}

WITH cohort_data AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', created_at) as cohort_month
    FROM {{ ref('dim_customers') }}
    WHERE created_at >= DATE '{{ analysis_start_date }}'
    {% if analysis_end_date != 'current_date' %}
      AND created_at <= DATE '{{ analysis_end_date }}'
    {% endif %}
)

SELECT * FROM cohort_data
```

### 4. **Error Handling**

```sql
-- ✅ Good: Defensive programming
WITH validated_data AS (
    SELECT 
        customer_id,
        full_name,
        phone,
        created_at,
        -- Data validation flags
        CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END as missing_id,
        CASE WHEN full_name IS NULL OR TRIM(full_name) = '' THEN 1 ELSE 0 END as missing_name,
        CASE WHEN created_at IS NULL THEN 1 ELSE 0 END as missing_date
    FROM {{ ref('dim_customers') }}
),

data_quality_check AS (
    SELECT 
        SUM(missing_id) as total_missing_ids,
        SUM(missing_name) as total_missing_names,
        SUM(missing_date) as total_missing_dates,
        COUNT(*) as total_records
    FROM validated_data
)

-- Only proceed if data quality is acceptable
SELECT 
    CASE 
        WHEN total_missing_ids > total_records * 0.01 THEN 'ERROR: Too many missing IDs'
        WHEN total_missing_names > total_records * 0.05 THEN 'ERROR: Too many missing names'
        ELSE 'Data quality acceptable'
    END as quality_status,
    total_records,
    total_missing_ids,
    total_missing_names,
    total_missing_dates
FROM data_quality_check
```

### 5. **Performance Optimization**

```sql
-- ✅ Good: Optimized for performance
WITH recent_customers AS (
    -- Filter early to reduce data volume
    SELECT customer_id, full_name, created_at
    FROM {{ ref('dim_customers') }}
    WHERE created_at >= CURRENT_DATE - 365  -- Last year only
),

aggregated_metrics AS (
    -- Pre-aggregate where possible
    SELECT 
        DATE_TRUNC('month', created_at) as month,
        COUNT(*) as customer_count,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM recent_customers
    GROUP BY DATE_TRUNC('month', created_at)
)

SELECT * FROM aggregated_metrics
ORDER BY month

-- ❌ Bad: Inefficient full table scan
SELECT 
    DATE_TRUNC('month', created_at) as month,
    COUNT(*) as customer_count
FROM {{ ref('dim_customers') }}  -- Full table scan
GROUP BY DATE_TRUNC('month', created_at)
HAVING DATE_TRUNC('month', created_at) >= CURRENT_DATE - 365  -- Filter after aggregation
```

## 📚 Integration với Documentation

### 1. **schema.yml Configuration**

```yaml
# analyses/schema.yml
version: 2

analyses:
  - name: customer_demographics
    description: |
      # Customer Demographics Analysis
      
      Comprehensive demographic analysis providing insights into:
      - Customer distribution patterns
      - Geographic concentration
      - Demographic characteristics
      - Data quality metrics
      
      ## Business Value
      - Support marketing segmentation
      - Identify growth opportunities
      - Monitor data quality trends
      
      ## Usage
      ```bash
      dbt compile --select customer_demographics
      ```
      
      ## Stakeholders
      - **Primary**: Marketing Team
      - **Secondary**: Executive Team, Data Team
      
      ## Refresh Schedule
      - **Frequency**: Weekly
      - **Owner**: Data Team
      - **SLA**: Monday 9 AM
    
    columns:
      - name: section
        description: "Analysis section (Overview, Demographics, Quality)"
      - name: metric
        description: "Specific metric being measured"
      - name: value
        description: "Metric value or description"
      - name: percentage
        description: "Percentage representation where applicable"
    
    meta:
      owner: "data-team@company.com"
      business_owner: "marketing-team@company.com"
      criticality: "medium"
      refresh_frequency: "weekly"
      estimated_runtime: "< 30 seconds"
```

### 2. **Auto-generated Documentation**

```bash
# Generate documentation including analyses
dbt docs generate

# Serve documentation
dbt docs serve
```

### 3. **Analysis Catalog**

```sql
-- analyses/catalog/analysis_inventory.sql
/*
Purpose: Maintain inventory of all analyses with metadata
This helps teams discover and understand available analyses
*/

SELECT 
    'customer_demographics' as analysis_name,
    'Customer Insights' as category,
    'Marketing Team' as business_owner,
    'Data Team' as technical_owner,
    'Weekly' as refresh_frequency,
    'Active' as status,
    'Demographic analysis and segmentation' as description
    
UNION ALL

SELECT 
    'data_quality_report' as analysis_name,
    'Data Quality' as category,
    'Data Engineering' as business_owner,
    'Data Team' as technical_owner,
    'Daily' as refresh_frequency,
    'Active' as status,
    'Comprehensive data quality assessment' as description

-- Add more analyses...
ORDER BY category, analysis_name
```

## 🚀 Advanced Techniques

### 1. **Parameterized Analyses**

```sql
-- analyses/advanced/parameterized_cohort_analysis.sql
/*
Parameterized cohort analysis allowing flexible date ranges and metrics
Usage: dbt compile --select parameterized_cohort_analysis --vars '{"start_date": "2024-01-01", "metric": "revenue"}'
*/

{% set start_date = var('start_date', '2024-01-01') %}
{% set end_date = var('end_date', modules.datetime.date.today().strftime('%Y-%m-%d')) %}
{% set metric = var('metric', 'customers') %}

WITH cohort_base AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', created_at) as cohort_month,
        created_at
    FROM {{ ref('dim_customers') }}
    WHERE created_at BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
),

{% if metric == 'revenue' %}
cohort_metrics AS (
    SELECT 
        cb.cohort_month,
        cb.customer_id,
        COALESCE(SUM(o.order_amount), 0) as metric_value
    FROM cohort_base cb
    LEFT JOIN {{ ref('fct_orders') }} o ON cb.customer_id = o.customer_id
    GROUP BY cb.cohort_month, cb.customer_id
)
{% else %}
cohort_metrics AS (
    SELECT 
        cohort_month,
        customer_id,
        1 as metric_value  -- Customer count
    FROM cohort_base
)
{% endif %}

SELECT 
    cohort_month,
    COUNT(DISTINCT customer_id) as cohort_size,
    SUM(metric_value) as total_{{ metric }},
    AVG(metric_value) as avg_{{ metric }}_per_customer
FROM cohort_metrics
GROUP BY cohort_month
ORDER BY cohort_month
```

### 2. **Cross-Database Analysis**

```sql
-- analyses/advanced/cross_database_comparison.sql
/*
Compare data across different databases/environments
Useful for data validation and environment consistency checks
*/

WITH prod_metrics AS (
    SELECT 
        'production' as environment,
        COUNT(*) as customer_count,
        COUNT(phone) as phone_count,
        MAX(created_at) as latest_record
    FROM {{ ref('dim_customers') }}  -- Production data
),

staging_metrics AS (
    SELECT 
        'staging' as environment,
        COUNT(*) as customer_count,
        COUNT(phone) as phone_count,
        MAX(created_at) as latest_record
    FROM {{ source('staging_db', 'customers') }}  -- Staging data
),

comparison AS (
    SELECT * FROM prod_metrics
    UNION ALL
    SELECT * FROM staging_metrics
)

SELECT 
    environment,
    customer_count,
    phone_count,
    latest_record,
    LAG(customer_count) OVER (ORDER BY environment) as prev_count,
    customer_count - LAG(customer_count) OVER (ORDER BY environment) as count_diff
FROM comparison
```

### 3. **Time Series Analysis**

```sql
-- analyses/advanced/customer_trend_analysis.sql
/*
Advanced time series analysis with trend detection and forecasting
*/

WITH daily_metrics AS (
    SELECT 
        DATE_TRUNC('day', created_at) as date,
        COUNT(*) as new_customers
    FROM {{ ref('dim_customers') }}
    WHERE created_at >= CURRENT_DATE - 90  -- Last 90 days
    GROUP BY DATE_TRUNC('day', created_at)
),

trend_analysis AS (
    SELECT 
        date,
        new_customers,
        AVG(new_customers) OVER (
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as seven_day_avg,
        LAG(new_customers, 7) OVER (ORDER BY date) as same_day_last_week,
        new_customers - LAG(new_customers, 7) OVER (ORDER BY date) as week_over_week_change
    FROM daily_metrics
),

trend_classification AS (
    SELECT 
        *,
        CASE 
            WHEN week_over_week_change > 5 THEN 'Strong Growth'
            WHEN week_over_week_change > 0 THEN 'Growth'
            WHEN week_over_week_change = 0 THEN 'Stable'
            WHEN week_over_week_change > -5 THEN 'Decline'
            ELSE 'Strong Decline'
        END as trend_category
    FROM trend_analysis
)

SELECT * FROM trend_classification
ORDER BY date DESC
```

---

## 🎯 Kết luận

**dbt Analyses là công cụ mạnh mẽ cho data exploration và business intelligence:**

- ✅ **Version Controlled** - SQL queries được quản lý như code
- ✅ **Reusable** - Có thể tái sử dụng và chia sẻ trong team
- ✅ **Self-Documenting** - Metadata và documentation tích hợp
- ✅ **Consistent** - Sử dụng chung data models và business logic
- ✅ **Collaborative** - Team có thể review và improve together

**Key Benefits:**
1. **Exploration** - Ad-hoc analysis và data discovery
2. **Intelligence** - Business insights và reporting
3. **Quality** - Data quality monitoring và validation
4. **Investigation** - Debug và troubleshoot data issues
5. **Prototyping** - Test ideas trước khi production

**Best Practices:**
- Organize analyses by business domain
- Document thoroughly with business context
- Use consistent naming conventions
- Parameterize for flexibility
- Optimize for performance

**Remember:** Good analyses = Better insights = Smarter decisions! 🎯

---

**Tác giả:** VPAURA  
**Ngày:** 2025-12-30  
**Slogan:** `Code chay như muối bỏ bể, nhờ VPAura thì ngon luôn` 🚀
