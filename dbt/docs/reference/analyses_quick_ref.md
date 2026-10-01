# dbt Analyses - Quick Reference

## 🎯 What are Analyses?

**Analyses** = **Ad-hoc SQL queries** stored in dbt project for:
- 📊 Data exploration
- 📈 Business intelligence  
- 🔍 Data investigation
- 📋 Reporting
- 🧪 Prototyping

## 📁 Structure

```
analyses/
├── customer_insights/     # Customer analysis
├── data_quality/         # Quality reports
├── business_reports/     # BI reports
├── investigations/       # Ad-hoc research
└── schema.yml           # Documentation
```

## 🚀 Quick Commands

```bash
# List all analyses
dbt ls --resource-type analysis

# Compile specific analysis
dbt compile --select customer_demographics

# Compile all analyses
dbt compile --select resource_type:analysis

# View compiled SQL
type target\compiled\project\analyses\analysis_name.sql
```

## 📊 Analysis Types Created

### 1. **Customer Demographics**
- Customer distribution patterns
- Phone completion rates
- Name pattern analysis
- Data quality metrics

### 2. **Data Quality Report**
- Completeness metrics across pipeline
- Uniqueness validation
- Quality scoring (0-100)
- Source vs staging vs mart comparison

### 3. **Customer Growth Analysis**
- Monthly acquisition trends
- Growth rate calculations
- Cumulative metrics
- Business intelligence for marketing

### 4. **Source Target Comparison**
- ETL validation
- Record count verification
- Data quality improvement tracking
- Transformation accuracy

### 5. **ETL Performance Analysis**
- Model execution time analysis
- Resource utilization
- Bottleneck identification
- Performance optimization

## 🔧 Usage Examples

### Compile and View
```bash
dbt compile --select customer_demographics
type target\compiled\aml_transaction_pipeline\analyses\customer_insights\customer_demographics.sql
```

### With Parameters
```sql
-- In analysis file
{% set start_date = var('start_date', '2024-01-01') %}
```

```bash
dbt compile --select analysis_name --vars '{"start_date": "2024-06-01"}'
```

## 📚 Documentation

Each analysis includes:
- **Purpose** - What business questions it answers
- **Stakeholders** - Who uses this analysis
- **Methodology** - How it works
- **Usage** - How to run it
- **Owner** - Who maintains it

## 🏆 Best Practices

### ✅ Do:
- Document thoroughly with business context
- Use consistent naming conventions
- Organize by business domain
- Include data validation
- Parameterize for flexibility

### ❌ Don't:
- Create analyses without documentation
- Use generic names like "analysis1.sql"
- Include sensitive data in version control
- Create overly complex single queries

## 🎯 Key Benefits

- **Version Control** - SQL queries managed as code
- **Reusability** - Share and reuse across team
- **Documentation** - Self-documenting with metadata
- **Consistency** - Use common data models
- **Collaboration** - Team review and improvement

## 📈 Project Results

✅ **5 analyses created**
✅ **Comprehensive documentation**
✅ **Business intelligence ready**
✅ **Data quality monitoring**
✅ **Performance analysis**

---

**Usage:** Analyses are perfect for exploration, reporting, and investigation!
**Remember:** Compile with `dbt compile`, don't run with `dbt run`

**Tác giả:** VPAURA 🚀
