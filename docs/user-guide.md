# Data Platform User Guide for Analysts

Welcome to the Data Platform! This guide helps data analysts access and work with data.

## Getting Started

### Accessing the Platform

| Tool | URL | Purpose |
|------|-----|---------|
| Superset | http://localhost:8088 | Dashboards and SQL queries |
| OpenMetadata | http://localhost:8585 | Data catalog and discovery |
| Airflow | http://localhost:8080 | Pipeline monitoring |

### Default Credentials

| Role | Username | Password | Access Level |
|------|----------|----------|--------------|
| Admin | admin | admin | Full access |
| Analyst | analyst | analyst123 | Create dashboards, SQL Lab |
| Viewer | viewer | viewer123 | View dashboards only |

---

## Understanding the Data

### Data Layers (Medallion Architecture)

The platform organizes data into three layers:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA LAYERS                                      │
├─────────────────┬───────────────────┬───────────────────────────────────┤
│     BRONZE      │      SILVER       │              GOLD                 │
│  (Raw Schema)   │ (Staging Schema)  │          (Marts Schema)           │
├─────────────────┼───────────────────┼───────────────────────────────────┤
│                 │                   │                                   │
│  raw.customers  │ staging.stg_      │ marts.dim_customers              │
│  raw.orders     │   customers       │ marts.dim_products               │
│  raw.products   │ staging.stg_      │ marts.fct_orders                 │
│  raw.transact.. │   orders          │ marts.mart_customer_analytics    │
│                 │ staging.stg_      │ marts.mart_product_analytics     │
│                 │   products        │ marts.mart_daily_summary         │
│                 │                   │                                   │
├─────────────────┼───────────────────┼───────────────────────────────────┤
│  Raw data as    │  Cleaned and      │  Business-ready tables           │
│  received       │  validated        │  optimized for analytics         │
│                 │                   │                                   │
│  ❌ Don't query │  ⚠️ Query if      │  ✅ Primary query target         │
│  directly       │  needed           │                                   │
└─────────────────┴───────────────────┴───────────────────────────────────┘
```

### Key Tables for Analysts

| Table | Description | Common Use Cases |
|-------|-------------|------------------|
| `marts.dim_customers` | Customer dimension | Customer segmentation, profiles |
| `marts.dim_products` | Product dimension | Product catalog, categories |
| `marts.fct_orders` | Order facts | Revenue analysis, order trends |
| `marts.mart_customer_analytics` | Customer metrics | CLV, retention, behavior |
| `marts.mart_product_analytics` | Product performance | Best sellers, trends |
| `marts.mart_daily_summary` | Daily KPIs | Executive dashboards |

---

## Using Superset

### Logging In

1. Navigate to http://localhost:8088
2. Enter credentials (analyst / analyst123)
3. You'll see the dashboard list

### Exploring Dashboards

**Pre-built Dashboards**:

| Dashboard | Description | Key Metrics |
|-----------|-------------|-------------|
| Customer Insights | Customer segmentation and behavior | CLV, segments, retention |
| Product Performance | Product sales and trends | Revenue, units sold, categories |
| Daily Operations | Daily business KPIs | Revenue, orders, new customers |
| Platform Operations | Platform health | Pipeline status, freshness |

### Using SQL Lab

SQL Lab allows you to write custom queries:

1. Click **SQL** > **SQL Lab** in the navigation
2. Select database: **Data Warehouse (PostgreSQL)**
3. Select schema: **marts** (recommended)
4. Write your query
5. Click **Run**

**Example Queries**:

```sql
-- Top 10 customers by total orders
SELECT
    customer_id,
    first_name,
    last_name,
    total_orders,
    total_revenue
FROM marts.mart_customer_analytics
ORDER BY total_revenue DESC
LIMIT 10;
```

```sql
-- Daily revenue trend (last 30 days)
SELECT
    date,
    total_orders,
    total_revenue,
    new_customers
FROM marts.mart_daily_summary
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY date;
```

```sql
-- Product category performance
SELECT
    category,
    COUNT(*) as product_count,
    SUM(total_units_sold) as units_sold,
    SUM(total_revenue) as revenue
FROM marts.mart_product_analytics
GROUP BY category
ORDER BY revenue DESC;
```

### Creating Charts

1. **From SQL Lab**:
   - Run your query
   - Click **Create Chart**
   - Choose visualization type
   - Configure chart options
   - Save to dashboard

2. **From Explore**:
   - Go to **Data** > **Datasets**
   - Click on a dataset (e.g., mart_customer_analytics)
   - Configure dimensions and metrics
   - Choose chart type
   - Save

### Chart Types for Common Use Cases

| Use Case | Recommended Chart | Configuration |
|----------|-------------------|---------------|
| Time series trends | Line Chart | X: date, Y: metric |
| Category comparison | Bar Chart | X: category, Y: metric |
| Composition | Pie Chart | Dimension + Metric |
| Distribution | Histogram | Metric + bins |
| Correlation | Scatter Plot | X: metric1, Y: metric2 |
| KPI display | Big Number | Single metric |
| Rankings | Table | Columns + sort |

### Creating Dashboards

1. Click **Dashboards** > **+ Dashboard**
2. Give it a name and save
3. Click **Edit Dashboard**
4. Drag charts from the sidebar
5. Arrange layout
6. Add filters if needed
7. Save

### Sharing and Scheduling

**Share Dashboard**:
- Click **Share** button
- Copy link or embed code

**Schedule Reports**:
1. Open dashboard
2. Click **...** > **Set up an email report**
3. Configure recipients and schedule
4. Save

---

## Using OpenMetadata (Data Catalog)

### Finding Data

1. Navigate to http://localhost:8585
2. Use the search bar to find tables
3. Browse by database, schema, or tags

### Understanding Data

For each table, you can view:
- **Schema**: Column names, types, descriptions
- **Sample Data**: Preview of actual data
- **Lineage**: Where data comes from
- **Quality**: Recent validation results
- **Usage**: Query frequency and users

### Data Lineage

The lineage view shows data flow:

```
Source Tables → Staging Models → Intermediate → Marts
```

To view lineage:
1. Open a table
2. Click **Lineage** tab
3. Expand upstream/downstream

### Data Quality

View data quality status:
1. Open a table
2. Check **Quality** tab
3. See recent test results

---

## Data Refresh Schedule

| Data | Refresh Frequency | Expected Freshness |
|------|-------------------|-------------------|
| Raw data | Every 4-6 hours | < 6 hours |
| Staging | After raw refresh | < 8 hours |
| Marts | Daily at 8 AM | < 24 hours |

### Checking Data Freshness

1. **In Superset**: Check the Platform Operations dashboard
2. **In OpenMetadata**: View table metadata for last update time
3. **In Airflow**: Check DAG run status

---

## Best Practices

### Query Performance

1. **Always query marts schema first** - it's optimized for analytics
2. **Use date filters** - narrow down the time range
3. **Limit results during exploration** - add `LIMIT 1000` while testing
4. **Avoid SELECT *** - specify only needed columns

### Data Quality

1. **Check data freshness** before making decisions
2. **Verify row counts** match expectations
3. **Look for nulls** in key fields
4. **Cross-reference** totals with known values

### Dashboard Design

1. **Start with KPIs** at the top
2. **Add filters** for user interactivity
3. **Group related charts** together
4. **Use consistent colors** for the same metrics
5. **Add descriptions** to charts for context

---

## Common Questions

### "Why is my data not up to date?"

1. Check Airflow for pipeline status
2. Look for failed tasks
3. Contact data engineering if issues persist

### "Why are my numbers different from the source system?"

1. Check the transformation logic in dbt
2. Review data quality checks
3. Verify you're using the correct date range

### "How do I add a new data source?"

Contact the data engineering team with:
- Source system details
- Required tables/fields
- Refresh frequency needs
- Business definitions

### "How do I request a new metric or dashboard?"

1. Submit a request via your internal ticketing system
2. Include:
   - Business use case
   - Required metrics and dimensions
   - Expected refresh frequency
   - Access requirements

---

## Glossary

| Term | Definition |
|------|------------|
| **Bronze** | Raw data layer (raw schema) |
| **Silver** | Cleaned data layer (staging schema) |
| **Gold** | Analytics-ready layer (marts schema) |
| **Dimension** | Descriptive attributes (who, what, where) |
| **Fact** | Measurable events (orders, transactions) |
| **Mart** | Subject-oriented data mart |
| **DAG** | Directed Acyclic Graph (pipeline definition) |
| **dbt** | Data Build Tool (transformation framework) |
| **SLA** | Service Level Agreement (freshness guarantee) |

---

## Getting Help

### Self-Service Resources

- **OpenMetadata**: Browse data catalog for table descriptions
- **Data Docs**: View data quality reports (link from OpenMetadata)
- **This Guide**: Reference for common tasks

### Support Channels

- **Slack**: #data-platform-support
- **Email**: data-team@example.com
- **Ticketing**: Submit via internal system

### Escalation Path

1. Check documentation and Data Docs
2. Ask in Slack channel
3. Submit support ticket
4. Contact data engineering directly for urgent issues

---

## Quick Reference Card

### Superset Shortcuts

| Action | How To |
|--------|--------|
| Run query | Ctrl/Cmd + Enter |
| Format SQL | Ctrl/Cmd + Shift + F |
| New tab | Ctrl/Cmd + T |
| Save query | Ctrl/Cmd + S |

### Useful SQL Snippets

```sql
-- Current date
SELECT CURRENT_DATE;

-- Date arithmetic
SELECT CURRENT_DATE - INTERVAL '30 days';

-- Fiscal quarters (example)
SELECT
    date,
    EXTRACT(QUARTER FROM date) as quarter,
    EXTRACT(YEAR FROM date) as year
FROM marts.mart_daily_summary;

-- Running total
SELECT
    date,
    total_revenue,
    SUM(total_revenue) OVER (ORDER BY date) as running_total
FROM marts.mart_daily_summary;

-- Year-over-year comparison
SELECT
    date,
    total_revenue,
    LAG(total_revenue, 365) OVER (ORDER BY date) as revenue_last_year
FROM marts.mart_daily_summary;
```

---

## Related Documentation

- [Data Flow](data-flow.md) - How data moves through the platform
- [Data Governance Guide](data-governance-guide.md) - Data ownership and quality
- [Superset Guide](superset-guide.md) - Detailed Superset documentation
