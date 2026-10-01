# So sánh dbt build vs các lệnh riêng lẻ

## 🔄 Cách cũ (chạy từng lệnh):

```bash
# Bước 1: Chạy source tests
dbt test --select source:*
# ⏱️ ~3 seconds

# Bước 2: Chạy staging models  
dbt run --select staging
# ⏱️ ~2 seconds

# Bước 3: Chạy staging tests
dbt test --select staging
# ⏱️ ~3 seconds

# Bước 4: Chạy mart models
dbt run --select marts
# ⏱️ ~2 seconds

# Bước 5: Chạy mart tests
dbt test --select marts
# ⏱️ ~8 seconds

# 📊 Total: ~18 seconds (sequential)
```

## ⚡ Cách mới (dbt build):

```bash
# Chỉ 1 lệnh
dbt build --threads 4
# ⏱️ ~14 seconds (parallel)

# 📊 Tiết kiệm: ~4 seconds (22% faster)
```

## 🎯 Lợi ích của dbt build:

### 1. **Simplicity**
- ❌ Cũ: 5 lệnh riêng biệt
- ✅ Mới: 1 lệnh duy nhất

### 2. **Performance**  
- ❌ Cũ: Sequential execution (~18s)
- ✅ Mới: Parallel execution (~14s)

### 3. **Reliability**
- ❌ Cũ: Phải nhớ đúng thứ tự
- ✅ Mới: Tự động dependency resolution

### 4. **Error Handling**
- ❌ Cũ: Phải handle từng step
- ✅ Mới: Unified error handling

## 🏗️ DAG Execution Order

```
Source Tests (parallel)
    ↓
Staging Models (parallel)
    ↓  
Staging Tests (parallel)
    ↓
Mart Models
    ↓
Mart Tests (parallel)
```

## 📊 Execution Statistics

| Metric | Value |
|--------|-------|
| **Total Tasks** | 24 |
| **Models** | 3 (2 views + 1 table) |
| **Tests** | 21 |
| **Threads** | 4 |
| **Execution Time** | 13.80s |
| **Success Rate** | 100% |

---

**Kết luận:** `dbt build` là lệnh tối ưu nhất cho production pipelines! 🚀
