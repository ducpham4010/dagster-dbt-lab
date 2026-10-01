# Multi-Environment Quick Reference

## 🚀 Quick Commands

### Switch Environments
```bash
# Development
dbt run --target dev

# Staging  
dbt run --target staging

# Production
dbt run --target prod
```

### Environment Setup
```bash
# Load environment variables
source config/.env.dev
source config/.env.staging  
source config/.env.prod

# Setup environment
./scripts/setup_environment.sh dev
./scripts/setup_environment.sh staging
./scripts/setup_environment.sh prod
```

### Environment-specific Operations
```bash
# Dev - Quick testing
dbt run --target dev --select tag:critical

# Staging - Full validation
dbt build --target staging --store-failures

# Prod - Production deployment
dbt run --target prod --threads 16 --store-failures
```

## 📁 File Structure

```
aml_transaction_pipeline/
├── config/
│   ├── profiles_example.yml      # Multi-env profiles template
│   ├── dbt_project_multi_env.yml # Multi-env project config
│   ├── .env.dev                  # Dev environment variables
│   ├── .env.staging              # Staging environment variables
│   └── .env.prod                 # Prod environment variables
├── scripts/
│   ├── setup_environment.sh      # Linux environment setup
│   └── setup_environment.bat     # Windows environment setup
├── .github/workflows/
│   └── multi_env_pipeline.yml    # CI/CD pipeline
├── models/staging/
│   └── stg_customers_env_aware.sql # Environment-aware model
├── macros/
│   └── environment_macros.sql    # Environment utility macros
└── DBT_MULTI_ENVIRONMENT_GUIDE.md # Complete guide
```

## 🌍 Environment Variables

### Required Variables
```bash
# Database connection
{ENV}_DB_HOST
{ENV}_DB_USER  
{ENV}_DB_PASSWORD
{ENV}_DB_SCHEMA

# dbt configuration
DBT_TARGET
DBT_THREADS
```

### Optional Variables
```bash
# Application settings
DEBUG_MODE
ENABLE_TESTS
ENABLE_DOCS
DATA_RETENTION_DAYS
BATCH_SIZE

# Notifications
SLACK_WEBHOOK_URL
EMAIL_NOTIFICATIONS
```

## 🔧 Environment Macros

```sql
-- Get environment-specific config
{{ get_env_config('batch_size', 1000) }}

-- Apply environment sampling
{{ apply_env_sampling('customers', 10) }}

-- Environment-specific logging
{{ env_log('Processing started', 'info') }}

-- Validate environment
{{ validate_environment() }}
```

## 🎯 Environment Comparison

| Environment | Purpose | Data | Threads | Tests |
|-------------|---------|------|---------|-------|
| **dev** | Development | Sample | 2-4 | Critical only |
| **staging** | Testing | Subset | 4-8 | All tests |
| **prod** | Production | Full | 8-16 | All tests |

## 🚨 Troubleshooting

### Check Environment
```bash
# Verify target
dbt debug --target prod

# Check variables
echo $DBT_TARGET
echo $PROD_DB_HOST
```

### Common Issues
- Environment variables not set → `source .env.{env}`
- Wrong target → `--target {environment}`
- Permission denied → Check database permissions
- Model failures → Check environment-specific logic

---

**Quick Setup:**
1. Copy `config/profiles_example.yml` to `~/.dbt/profiles.yml`
2. Set environment variables: `source config/.env.dev`
3. Run setup: `./scripts/setup_environment.sh dev`
4. Deploy: `dbt run --target dev`

**Tác giả:** VPAURA 🚀
