# Superset BI Layer Guide

Apache Superset serves as the Business Intelligence (BI) layer for the data platform, providing interactive dashboards, charts, and SQL exploration capabilities for the dbt mart models.

## Architecture Overview

```mermaid
sequenceDiagram
    participant User
    participant Superset
    participant PostgreSQL
    participant dbt

    Note over dbt,PostgreSQL: Data Transformation Layer
    dbt->>PostgreSQL: Transform raw data into marts
    dbt->>PostgreSQL: Create mart_customer_analytics
    dbt->>PostgreSQL: Create mart_product_analytics
    dbt->>PostgreSQL: Create mart_daily_summary

    Note over Superset,PostgreSQL: BI Layer
    User->>Superset: Access dashboard
    Superset->>PostgreSQL: Query mart tables
    PostgreSQL-->>Superset: Return aggregated data
    Superset-->>User: Render visualizations

    User->>Superset: Create custom chart
    Superset->>PostgreSQL: Execute SQL query
    PostgreSQL-->>Superset: Return results
    Superset-->>User: Display chart
```

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Data Platform                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────┐  │
│  │   Airflow    │───>│     dbt      │───>│      PostgreSQL          │  │
│  │ Orchestrator │    │  Transform   │    │    Data Warehouse        │  │
│  └──────────────┘    └──────────────┘    │  ┌────────────────────┐  │  │
│                                          │  │  marts schema       │  │  │
│                                          │  │  ├─ customer_analytics│  │
│                                          │  │  ├─ product_analytics │  │
│                                          │  │  └─ daily_summary    │  │  │
│                                          │  └────────────────────┘  │  │
│                                          └────────────┬─────────────┘  │
│                                                       │                 │
│                                                       ▼                 │
│                                          ┌──────────────────────────┐  │
│                                          │       Superset           │  │
│                                          │   BI / Visualization     │  │
│                                          │  ┌────────────────────┐  │  │
│                                          │  │  Dashboards        │  │  │
│                                          │  │  ├─ Customer Insights│  │
│                                          │  │  ├─ Product Perf    │  │  │
│                                          │  │  └─ Daily Ops       │  │  │
│                                          │  └────────────────────┘  │  │
│                                          └──────────────────────────┘  │
│                                                       │                 │
└───────────────────────────────────────────────────────┼─────────────────┘
                                                        ▼
                                               ┌────────────────┐
                                               │     Users      │
                                               │ Analysts/Exec  │
                                               └────────────────┘
```

## Access Information

| Item | Value |
|------|-------|
| URL | http://localhost:8088 |
| Default Admin | admin / admin |
| Analyst User | analyst / analyst123 |
| Viewer User | viewer / viewer123 |

## Getting Started

### Accessing the UI

1. Start the platform: `make start`
2. Wait for services to initialize (check with `make status`)
3. Open http://localhost:8088 in your browser
4. Log in with your credentials

### Navigation Overview

After logging in, you'll see the main navigation menu:

- **Dashboards**: Pre-built and custom dashboards
- **Charts**: Individual visualizations
- **Datasets**: Data sources connected to dbt marts
- **SQL Lab**: Ad-hoc SQL query interface
- **Data**: Manage databases and datasets (Admin only)
- **Settings**: User preferences and configuration

## Pre-configured Dashboards

### Customer Insights

**Purpose**: Analyze customer behavior, lifetime value, and spending patterns.

**Key Metrics**:
- Total unique customers
- Average order value
- Customer lifetime (days between first and last purchase)
- Top customers by total spend

**Charts Included**:
| Chart | Type | Description |
|-------|------|-------------|
| Total Customers KPI | Big Number | Count of unique customers |
| Average Transaction Value KPI | Big Number | Mean order value |
| Top Customers by Total Spend | Bar Chart | Top 10 customers ranked by spend |
| Customer Lifetime Distribution | Histogram | Distribution of customer tenure |

**Use Cases**:
- Identify high-value customers
- Understand customer retention patterns
- Track customer acquisition trends

### Product Performance

**Purpose**: Monitor product sales, revenue, and category performance.

**Key Metrics**:
- Total revenue by product
- Units sold
- Revenue distribution by category
- Customer reach per product

**Charts Included**:
| Chart | Type | Description |
|-------|------|-------------|
| Top Products by Revenue | Horizontal Bar | Top 10 products by revenue |
| Revenue by Category | Pie Chart | Revenue breakdown by product category |
| Product Performance Table | Table | Detailed metrics for all products |
| Unique Customers per Product | Bar Chart | Customer count by product |

**Use Cases**:
- Identify best-selling products
- Analyze category performance
- Plan inventory and promotions

### Daily Operations

**Purpose**: Track daily business performance and operational metrics.

**Key Metrics**:
- Daily revenue
- Transaction count
- Unique daily customers
- Average transaction value

**Charts Included**:
| Chart | Type | Description |
|-------|------|-------------|
| Total Revenue KPI | Big Number | Aggregate revenue |
| Total Transactions KPI | Big Number | Transaction count |
| Daily Revenue Trend | Line Chart | Revenue over time |
| Transaction Volume | Area Chart | Transactions over time |

**Use Cases**:
- Monitor daily performance
- Identify trends and anomalies
- Support operational decisions

## Working with Datasets

### Available Datasets

Datasets are automatically created for each dbt mart model:

| Dataset | Source Table | Schema | Description |
|---------|--------------|--------|-------------|
| mart_customer_analytics | marts.mart_customer_analytics | marts | Customer metrics and LTV |
| mart_product_analytics | marts.mart_product_analytics | marts | Product sales and revenue |
| mart_daily_summary | marts.mart_daily_summary | marts | Daily aggregated metrics |

### Viewing Dataset Configuration

1. Navigate to **Data** > **Datasets**
2. Click on a dataset name to view details
3. Explore columns, metrics, and settings

### Editing Datasets

To modify a dataset (Analyst or Admin role):

1. Click the **Edit** button on the dataset page
2. **Columns tab**: Add calculated columns, set data types
3. **Metrics tab**: Define custom aggregations
4. **Settings tab**: Configure caching, permissions

### Adding Calculated Columns

1. Go to dataset edit mode
2. Click **Calculated Columns** tab
3. Add a new column with a SQL expression:
   ```sql
   CASE
     WHEN total_spend > 1000 THEN 'High Value'
     WHEN total_spend > 500 THEN 'Medium Value'
     ELSE 'Low Value'
   END
   ```
4. Save the dataset

### Refreshing Dataset Metadata

When dbt models change:

1. Navigate to the dataset
2. Click **Sync columns from source**
3. Verify new/changed columns appear
4. Update any affected charts

## Creating Charts

### Step-by-Step Guide

1. Navigate to **Charts** > **+ Chart**
2. Select a dataset (e.g., `mart_customer_analytics`)
3. Choose a visualization type
4. Configure the chart:
   - **Metrics**: What to measure (SUM, COUNT, AVG, etc.)
   - **Dimensions**: How to group/slice data
   - **Filters**: Limit data included
   - **Sort**: Order of results
5. Click **Run Query** to preview
6. Click **Save** to store the chart

### Visualization Types

| Type | Best For |
|------|----------|
| Big Number | Single KPI metrics |
| Bar Chart | Comparing categories |
| Line Chart | Time series trends |
| Pie/Donut | Part-to-whole relationships |
| Table | Detailed data exploration |
| Area Chart | Volume over time |
| Scatter Plot | Correlation analysis |
| Histogram | Distribution analysis |
| Heatmap | Two-dimensional patterns |

### Example: Creating a Revenue by Month Chart

1. Select `mart_daily_summary` dataset
2. Choose **Time-series Line Chart**
3. Configure:
   - **Time Column**: `date`
   - **Time Grain**: `Month`
   - **Metrics**: `SUM(total_revenue)`
4. Add formatting:
   - Y-axis format: `$,.0f`
   - Enable legend
5. Save as "Monthly Revenue Trend"

## Creating Dashboards

### Building a New Dashboard

1. Navigate to **Dashboards** > **+ Dashboard**
2. Give it a title and optional description
3. Click **Edit Dashboard**
4. Drag charts from the right panel to the canvas
5. Resize and arrange as needed
6. Click **Save**

### Adding Filters

1. In dashboard edit mode, click **+ Filter**
2. Choose filter type:
   - **Value**: Dropdown selection
   - **Range**: Numeric/date range
   - **Time Range**: Date picker
3. Configure which charts the filter affects
4. Save the dashboard

### Cross-Filtering

Enable users to click on chart elements to filter other charts:

1. Edit dashboard settings
2. Enable **Cross-filtering**
3. Charts on the same dataset will interact automatically

### Dashboard Permissions

Set access controls:

1. Edit dashboard
2. Go to **Settings** > **Access**
3. Assign owners and viewers
4. Optionally make the dashboard public

## SQL Lab

SQL Lab provides an ad-hoc query interface for exploring data directly.

### Running Queries

1. Navigate to **SQL Lab** > **SQL Editor**
2. Select database: `postgres_warehouse`
3. Select schema: `marts`
4. Write your query:
   ```sql
   SELECT
     customer_name,
     total_spend,
     total_orders
   FROM marts.mart_customer_analytics
   WHERE total_spend > 500
   ORDER BY total_spend DESC
   LIMIT 100;
   ```
5. Click **Run**

### Saving Queries

1. Write and run your query
2. Click **Save** > **Save query**
3. Give it a name and description
4. Access later from **SQL Lab** > **Saved Queries**

### Creating Charts from Queries

1. Run a query in SQL Lab
2. Click **Explore**
3. Choose a visualization type
4. Configure and save as a chart

### Query History

View past queries:

1. Navigate to **SQL Lab** > **Query History**
2. Filter by user, status, or date
3. Re-run or edit previous queries

## User Roles and Permissions

### Role Overview

| Role | Description | Access Level |
|------|-------------|--------------|
| Admin | Full platform access | Create/edit/delete all objects, manage users |
| Analyst | Create and modify content | Create/edit dashboards, charts, datasets, SQL Lab |
| Viewer | Read-only access | View dashboards and charts only |

### Permissions Matrix

| Permission | Admin | Analyst | Viewer |
|------------|-------|---------|--------|
| View dashboards | ✓ | ✓ | ✓ |
| Create dashboards | ✓ | ✓ | ✗ |
| Edit dashboards | ✓ | ✓ | ✗ |
| Delete dashboards | ✓ | ✗ | ✗ |
| View charts | ✓ | ✓ | ✓ |
| Create charts | ✓ | ✓ | ✗ |
| Edit charts | ✓ | ✓ | ✗ |
| SQL Lab access | ✓ | ✓ | ✗ |
| Manage datasets | ✓ | ✓ | ✗ |
| Manage databases | ✓ | ✗ | ✗ |
| Manage users | ✓ | ✗ | ✗ |

### Managing Users (Admin Only)

1. Navigate to **Settings** > **List Users**
2. Click **+ User** to add new user
3. Fill in details and assign role
4. Click **Save**

### Assigning Roles

1. Edit user from user list
2. Select role(s) from dropdown
3. Save changes

## Database Connections

### Current Configuration

The `postgres_warehouse` connection is pre-configured to connect to the PostgreSQL data warehouse:

| Setting | Value |
|---------|-------|
| Database Name | postgres_warehouse |
| Host | postgres |
| Port | 5432 |
| Database | airflow |
| User | airflow |
| SQL Lab | Enabled |
| Charts/Dashboards | Enabled |

### Adding New Connections (Admin Only)

1. Navigate to **Data** > **Databases**
2. Click **+ Database**
3. Select database type
4. Enter connection details:
   ```
   postgresql://user:password@host:port/database
   ```
5. Test connection
6. Configure permissions
7. Save

### Connection Testing

Test an existing connection:

1. Go to **Data** > **Databases**
2. Click on the database
3. Click **Test Connection**

## Best Practices

### Dashboard Design

- **Keep it focused**: One dashboard per use case
- **Prioritize**: Place most important metrics at the top
- **Use KPIs**: Big numbers for key metrics
- **Consistent colors**: Use the same color scheme
- **Add context**: Include descriptions and labels
- **Mobile-friendly**: Test on different screen sizes

### Performance Optimization

- **Limit rows**: Use `row_limit` in charts
- **Aggregate early**: Let dbt do heavy aggregations
- **Use caching**: Enable cache for slow queries
- **Index columns**: Ensure filter columns are indexed
- **Avoid SELECT ***: Query only needed columns

### Naming Conventions

- **Dashboards**: `[Area] - [Purpose]` (e.g., "Sales - Monthly Review")
- **Charts**: `[Metric] by [Dimension]` (e.g., "Revenue by Product")
- **Datasets**: Match source table names
- **Metrics**: Descriptive names (e.g., `total_revenue`, `customer_count`)

### Documentation

- Add descriptions to dashboards and charts
- Document custom metrics and calculations
- Keep a changelog for major dashboard updates
- Tag dashboards by department/team

## Troubleshooting

### Common Issues

#### "Connection refused" or database errors

**Cause**: Database not accessible or credentials incorrect.

**Solution**:
1. Verify PostgreSQL is running: `make status`
2. Check connection string in database settings
3. Test connection from Superset UI

#### Charts not loading or timing out

**Cause**: Query too slow or data too large.

**Solution**:
1. Add filters to reduce data volume
2. Increase row limit or timeout settings
3. Check if source table needs indexing
4. Consider pre-aggregating in dbt

#### Permission denied errors

**Cause**: User role lacks required permissions.

**Solution**:
1. Check user's assigned role
2. Verify role has necessary permissions
3. Contact admin to adjust permissions

#### Dashboard not showing latest data

**Cause**: Caching or stale dataset.

**Solution**:
1. Click refresh on dashboard
2. Clear cache: **Settings** > **Cache** > **Clear**
3. Re-sync dataset columns if schema changed

### Checking Logs

View Superset container logs:

```bash
make superset-logs
# or
docker compose logs -f superset
```

### Resetting Admin Password

If admin password is lost:

```bash
docker compose exec superset superset fab reset-password --username admin --password newpassword
```

### Re-running Initialization

To reset Superset configuration:

```bash
make superset-reset
```

This will:
1. Stop the Superset container
2. Remove Superset data
3. Restart with fresh initialization

## Integration with dbt

### How Superset Consumes dbt Models

1. **dbt runs transformations**: Creates mart tables in PostgreSQL
2. **Superset connects**: Reads from `marts` schema
3. **Auto-refresh**: Data updates on next dbt run
4. **No ETL needed**: Direct SQL access to marts

### Workflow: dbt Changes → Superset Updates

```
1. Update dbt model
2. Run: dbt run --select model_name
3. Superset automatically sees new data
4. If schema changed: sync dataset columns
5. Update affected charts if needed
```

### Handling Schema Changes

When dbt model columns change:

1. Run dbt to apply changes
2. In Superset, go to affected dataset
3. Click **Sync columns from source**
4. Verify columns match expectations
5. Update charts using changed columns

### Using dbt Documentation

dbt generates documentation that complements Superset:

1. Run `dbt docs generate`
2. View at http://localhost:8080 (if served)
3. Cross-reference column descriptions
4. Use dbt lineage to understand data flow

## Quick Reference

### Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl+Enter` | Run query (SQL Lab) |
| `Ctrl+S` | Save (various contexts) |
| `Esc` | Close modal |

### Useful URLs

| URL | Purpose |
|-----|---------|
| /superset/dashboard/customer-insights/ | Customer dashboard |
| /superset/dashboard/product-performance/ | Product dashboard |
| /superset/dashboard/daily-operations/ | Operations dashboard |
| /superset/sqllab/ | SQL Lab |
| /api/v1/database/ | Database API |

### Make Commands

```bash
make superset-logs    # View Superset logs
make superset-shell   # Access container shell
make superset-init    # Re-run initialization
make superset-reset   # Reset and reinitialize
```
