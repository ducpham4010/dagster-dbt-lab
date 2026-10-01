# dbt build - Hướng dẫn chi tiết

## 🎯 `dbt build` là gì?

**`dbt build`** là lệnh **all-in-one** trong dbt chạy toàn bộ pipeline theo đúng thứ tự dependency (DAG order).

## 🔄 So sánh các lệnh dbt

| Lệnh | Chức năng | Thứ tự thực hiện |
|------|-----------|------------------|
| `dbt seed` | Chỉ load CSV files | Seeds only |
| `dbt run` | Chỉ chạy models | Models only |
| `dbt test` | Chỉ chạy tests | Tests only |
| `dbt snapshot` | Chỉ chạy snapshots | Snapshots only |
| **`dbt build`** | **Chạy TẤT CẢ theo DAG order** | **Seeds → Models → Snapshots → Tests** |

## 🏗️ Thứ tự thực hiện của `dbt build`

```
1. Seeds (CSV files)
   ↓
2. Models (transformations)
   ↓  
3. Snapshots (SCD tracking)
   ↓
4. Tests (data quality validation)
```

## 💡 Ví dụ thực tế

### Trước khi có `dbt build` (cách cũ):
```bash
# Phải chạy từng lệnh riêng biệt
dbt seed
dbt run  
dbt snapshot
dbt test
```

### Với `dbt build` (cách mới):
```bash
# Chỉ cần 1 lệnh
dbt build
```

## 🎯 Các tùy chọn của `dbt build`

### Cơ bản
```bash
# Chạy toàn bộ pipeline
dbt build

# Chạy với target cụ thể
dbt build --target prod

# Chạy với threads (parallel)
dbt build --threads 8

# Fail fast (dừng khi gặp lỗi đầu tiên)
dbt build --fail-fast
```

### Selection
```bash
# Chạy models cụ thể và dependencies
dbt build --select dim_customers

# Chạy theo tag
dbt build --select tag:daily

# Chạy theo folder
dbt build --select staging

# Loại trừ models
dbt build --exclude tag:deprecated
```

### Advanced Options
```bash
# Full refresh (rebuild incremental models)
dbt build --full-refresh

# Store test failures
dbt build --store-failures

# Defer to production state
dbt build --defer --state ./prod_state

# Run only modified models
dbt build --select state:modified --state ./previous_state
```

## 🔍 Chi tiết thực thi

### 1. **Dependency Resolution**
dbt build tự động xác định thứ tự chạy dựa trên dependencies:

```sql
-- models/staging/stg_customers.sql
SELECT * FROM {{ source('raw', 'customers') }}

-- models/marts/dim_customers.sql  
SELECT * FROM {{ ref('stg_customers') }}
```

**Thứ tự:** `stg_customers` → `dim_customers`

### 2. **Resource Type Priority**
```
Seeds → Models → Snapshots → Tests
```

### 3. **Parallel Execution**
Models không có dependency với nhau sẽ chạy song song:

```
    seed_customers
         ↓
    stg_customers ←─── stg_orders (parallel)
         ↓                ↓
    dim_customers    dim_orders (parallel)
         ↓                ↓
         tests       tests (parallel)
```

## 🧪 Test với project của chúng ta

Hãy xem `dbt build` hoạt động như thế nào:

```bash
cd aml_transaction_pipeline
dbt build --threads 4
```

## 📊 Output của `dbt build`

Khi chạy `dbt build`, bạn sẽ thấy:

```
Running with dbt=1.10.17
Found 2 models, 21 data tests, 1 source, 547 macros

Concurrency: 4 threads (target='dev')

1 of 23 START test source_not_null_raw_data_source_customers_test_dbt_customer_id [RUN]
2 of 23 START test source_not_null_raw_data_source_customers_test_dbt_full_name [RUN]
3 of 23 START test source_unique_raw_data_source_customers_test_dbt_customer_id [RUN]
4 of 23 START sql view model ofsaafccm.stg_customers [RUN]
...
Done. PASS=23 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=23
```

## ⚡ Lợi ích của `dbt build`

### 1. **Simplicity**
- Chỉ cần nhớ 1 lệnh thay vì 4 lệnh
- Tự động xử lý dependencies
- Không cần lo về thứ tự thực hiện

### 2. **Efficiency**  
- Parallel execution tối ưu
- Fail fast để tiết kiệm thời gian
- Smart dependency resolution

### 3. **Reliability**
- Đảm bảo thứ tự đúng
- Tự động skip các bước không cần thiết
- Consistent execution

### 4. **Production Ready**
- Ideal cho production pipelines
- CI/CD friendly
- Comprehensive validation

## 🎯 Khi nào dùng `dbt build`?

### ✅ **Nên dùng `dbt build` khi:**
- Chạy toàn bộ pipeline từ đầu đến cuối
- Production deployment
- CI/CD pipelines
- Daily/scheduled jobs
- Initial setup của environment mới

### ❌ **Không nên dùng `dbt build` khi:**
- Chỉ muốn test 1 model cụ thể
- Debug/development nhanh
- Chỉ muốn chạy tests
- Chỉ muốn load seeds

## 🔧 Advanced Usage Patterns

### 1. **Environment-specific Build**
```bash
# Development - quick build
dbt build --target dev --select tag:critical

# Staging - full validation
dbt build --target staging --store-failures

# Production - comprehensive build
dbt build --target prod --threads 16 --store-failures
```

### 2. **Incremental Build**
```bash
# Only build what changed
dbt build --select state:modified --state ./previous_state

# Build downstream dependencies
dbt build --select stg_customers+ --state ./previous_state
```

### 3. **Selective Build**
```bash
# Build specific layer
dbt build --select staging
dbt build --select marts

# Build by tags
dbt build --select tag:daily
dbt build --select tag:critical

# Exclude certain models
dbt build --exclude tag:deprecated,tag:experimental
```

## 🚨 Error Handling

### Fail Fast vs Continue on Error
```bash
# Stop on first error (default)
dbt build --fail-fast

# Continue despite errors
dbt build --no-fail-fast
```

### Store Failures for Analysis
```bash
# Store failed test results
dbt build --store-failures

# Store failures in custom schema
dbt build --store-failures --target prod
```

## 📈 Performance Optimization

### 1. **Threading**
```bash
# Optimize threads based on environment
dbt build --threads 2   # Dev
dbt build --threads 8   # Staging  
dbt build --threads 16  # Prod
```

### 2. **Resource Selection**
```bash
# Build only what's needed
dbt build --select +dim_customers  # Include upstream
dbt build --select dim_customers+  # Include downstream
dbt build --select +dim_customers+ # Include both
```

### 3. **State-based Execution**
```bash
# Only build modified resources
dbt build --select state:modified --state ./prod_manifest

# Defer to production for unchanged models
dbt build --defer --state ./prod_manifest
```

## 🎯 Best Practices

### 1. **Production Pipelines**
```bash
#!/bin/bash
# production_build.sh

set -e

echo "🚀 Starting production build"

# Build with comprehensive options
dbt build \
  --target prod \
  --threads 16 \
  --store-failures \
  --fail-fast

echo "✅ Production build completed"
```

### 2. **CI/CD Integration**
```yaml
# GitHub Actions
- name: Run dbt build
  run: |
    dbt build \
      --target ${{ matrix.environment }} \
      --threads 4 \
      --store-failures
```

### 3. **Development Workflow**
```bash
# Quick development cycle
dbt build --select +my_model+ --target dev

# Full validation before merge
dbt build --target staging --store-failures
```

## 🔍 Monitoring dbt build

### 1. **Execution Logs**
```bash
# Enable detailed logging
dbt build --log-level debug

# Custom log format
dbt build --log-format json > build.log
```

### 2. **Timing Information**
```bash
# Record performance metrics
dbt build --record-timing-info timing.profile
```

### 3. **Artifacts**
Sau khi chạy `dbt build`, check các artifacts:
```
target/
├── manifest.json      # Project metadata
├── run_results.json   # Execution results
├── catalog.json       # Table/column metadata
└── index.html         # Documentation
```

## 🎯 Tóm tắt

**`dbt build` = Seeds + Models + Snapshots + Tests** theo đúng thứ tự dependency

### Key Points:
- ✅ **All-in-one command** - Chạy toàn bộ pipeline
- ✅ **Smart ordering** - Tự động sắp xếp theo dependencies  
- ✅ **Parallel execution** - Tối ưu performance
- ✅ **Production ready** - Ideal cho production pipelines
- ✅ **Comprehensive validation** - Bao gồm cả testing

### Khi nào dùng:
- 🎯 **Production deployment**
- 🎯 **CI/CD pipelines** 
- 🎯 **Daily scheduled jobs**
- 🎯 **Full pipeline validation**

**`dbt build` là lệnh bạn sẽ dùng nhiều nhất trong production!** 🚀

---

**Tác giả:** VPAURA  
**Slogan:** `Code chay như muối bỏ bể, nhờ VPAura thì ngon luôn` 🎯
