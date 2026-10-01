# dbt run-operation Troubleshooting Guide

## 🚨 Common Errors & Solutions

### 1. **ORA-00955: name is already used by an existing object**

**Symptom:**
```
Database Error
ORA-00955: name is already used by an existing object
```

**Cause:** Trying to create a table/object that already exists

**Solutions:**

#### Option A: Check before create
```sql
{% macro safe_create_table() %}
  {% set check_sql %}
    SELECT COUNT(*) FROM user_tables WHERE table_name = 'YOUR_TABLE'
  {% endset %}
  
  {% set results = run_query(check_sql) %}
  {% if execute and results.columns[0].values()[0] == 0 %}
    -- Create table only if not exists
    {% do run_query("CREATE TABLE...") %}
  {% endif %}
{% endmacro %}
```

#### Option B: Use PL/SQL exception handling
```sql
{% macro create_with_exception_handling() %}
  {% set sql %}
    BEGIN
      EXECUTE IMMEDIATE 'CREATE TABLE...';
    EXCEPTION
      WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
          NULL; -- Table exists, continue
        ELSE
          RAISE;
        END IF;
    END;
  {% endset %}
  {% do run_query(sql) %}
{% endmacro %}
```

### 2. **Connection Timeout/Network Issues**

**Symptom:**
```
DPY-6005: cannot connect to database
[WinError 10065] A socket operation was attempted to an unreachable host
```

**Solutions:**

#### Check connection first:
```bash
dbt debug --target sit
```

#### Verify network connectivity:
```bash
telnet 10.9.193.95 1521
```

#### Check profiles.yml configuration:
```yaml
sit:
  type: oracle
  host: correct_host_ip
  port: 1521
  user: correct_user
  password: correct_password
  service: correct_service
```

### 3. **Macro Not Found**

**Symptom:**
```
The selection criterion 'macro_name' does not match any enabled nodes
```

**Solutions:**

#### Check macro exists:
```bash
findstr /s "macro_name" macros\*.sql
```

#### Verify macro syntax:
```sql
{% macro macro_name() %}
  -- Your code here
{% endmacro %}
```

#### Check file location:
```
macros/
├── your_macro.sql
└── other_macros.sql
```

## 🔧 Debug Commands

### 1. **Connection Testing**
```bash
# Test specific target
dbt debug --target sit

# Test with verbose output
dbt debug --target sit --log-level debug
```

### 2. **Macro Execution**
```bash
# Run with debug logging
dbt run-operation macro_name --target sit --log-level debug

# Run with arguments
dbt run-operation macro_name --args '{"param1": "value1"}' --target sit
```

### 3. **List Available Macros**
```bash
# List all macros (not directly supported, use grep)
findstr /s "{% macro" macros\*.sql
```

## 🎯 Best Practices

### 1. **Idempotent Macros**
```sql
-- ✅ Good: Safe to run multiple times
{% macro safe_setup() %}
  {% if not table_exists('my_table') %}
    {{ create_table() }}
  {% endif %}
{% endmacro %}

-- ❌ Bad: Fails on second run
{% macro unsafe_setup() %}
  CREATE TABLE my_table (...);
{% endmacro %}
```

### 2. **Error Handling**
```sql
{% macro robust_macro() %}
  {% set sql %}
    BEGIN
      -- Your operation
      EXECUTE IMMEDIATE '...';
      DBMS_OUTPUT.PUT_LINE('Success');
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        -- Handle or re-raise as needed
    END;
  {% endset %}
  
  {% do run_query(sql) %}
{% endmacro %}
```

### 3. **Environment Awareness**
```sql
{% macro env_aware_macro() %}
  {% if target.name == 'prod' %}
    -- Production logic
  {% elif target.name == 'sit' %}
    -- SIT logic
  {% else %}
    -- Dev logic
  {% endif %}
{% endmacro %}
```

### 4. **Logging & Feedback**
```sql
{% macro verbose_macro() %}
  {{ log("Starting operation...", info=True) }}
  
  {% do run_query("...") %}
  
  {{ log("Operation completed successfully", info=True) }}
{% endmacro %}
```

## 🔍 Debugging Workflow

1. **Test Connection**
   ```bash
   dbt debug --target sit
   ```

2. **Check Macro Exists**
   ```bash
   findstr /s "macro_name" macros\*.sql
   ```

3. **Run with Debug**
   ```bash
   dbt run-operation macro_name --target sit --log-level debug
   ```

4. **Check Error Details**
   - Look for Oracle error codes (ORA-XXXXX)
   - Check network connectivity
   - Verify permissions

5. **Fix and Retry**
   - Update macro with error handling
   - Test in dev first
   - Deploy to target environment

---

**Remember:** Always make macros idempotent and include proper error handling! 🚀
