# Dataset Documentation Template

Use this template to document datasets in the data catalog. Copy and customize for each dataset.

---

## Dataset Overview

| Attribute | Value |
|-----------|-------|
| **Dataset Name** | `[schema.table_name]` |
| **Display Name** | [Human-readable name] |
| **Description** | [Brief description of the dataset's purpose] |
| **Data Layer** | [ ] Bronze  [ ] Silver  [ ] Gold |
| **Business Domain** | [Customer / Product / Transaction / Financial / Operations / Marketing] |
| **Data Classification** | [ ] Public  [ ] Internal  [ ] Confidential  [ ] Restricted |
| **Contains PII** | [ ] Yes  [ ] No |
| **Certification Status** | [ ] Draft  [ ] Validated  [ ] Certified  [ ] Deprecated |

---

## Ownership & Stewardship

| Role | Name | Email | Team |
|------|------|-------|------|
| **Data Owner** | | | |
| **Data Steward** | | | |
| **Technical Owner** | | | |

---

## Data Source & Lineage

### Source Systems
- **Primary Source**: [Source system name]
- **Extraction Method**: [Full / Incremental / CDC]
- **Update Frequency**: [Real-time / Hourly / Daily / Weekly]

### Upstream Dependencies
| Dataset | Relationship | Description |
|---------|--------------|-------------|
| | | |

### Downstream Consumers
| Dataset/Dashboard | Relationship | Description |
|-------------------|--------------|-------------|
| | | |

---

## Schema Definition

### Columns

| Column Name | Data Type | Nullable | Description | Business Definition | Example Values |
|-------------|-----------|----------|-------------|---------------------|----------------|
| | | | | | |
| | | | | | |
| | | | | | |

### Primary Key
- **Column(s)**:
- **Uniqueness**: [Natural key / Surrogate key]

### Foreign Keys
| Column | References | Description |
|--------|------------|-------------|
| | | |

---

## Data Quality

### Quality Rules

| Rule Name | Rule Type | Column(s) | Threshold | Severity |
|-----------|-----------|-----------|-----------|----------|
| | Completeness | | | |
| | Uniqueness | | | |
| | Validity | | | |
| | Consistency | | | |
| | Timeliness | | | |

### Quality Metrics (Current)
- **Completeness**: %
- **Uniqueness**: %
- **Validity**: %
- **Last Validated**: [Date]

---

## Access & Security

### Access Level
- [ ] Open - All authenticated users
- [ ] Restricted - Specific teams/roles required
- [ ] Sensitive - Approval required

### Required Roles
- [ ] DataConsumer (read-only)
- [ ] DataAnalyst (read + query)
- [ ] DataEngineer (full access)
- [ ] DataOwner (owner access)

### PII Handling
*If Contains PII = Yes*
- **PII Columns**: [List columns]
- **Masking Applied**: [ ] Yes  [ ] No
- **Encryption**: [ ] At rest  [ ] In transit  [ ] Both

---

## Technical Details

### Storage Information
- **Database**:
- **Schema**:
- **Table Type**: [Table / View / Materialized View]
- **Partitioning**: [Column name or N/A]
- **Clustering**: [Column name(s) or N/A]

### Volume & Performance
- **Approximate Row Count**:
- **Approximate Size**:
- **Growth Rate**: [rows/day or GB/month]
- **Retention Period**: [days/months/years]

### Refresh Schedule
- **Frequency**:
- **DAG Name**:
- **Typical Duration**:
- **SLA**:

---

## Usage Guidelines

### Best Practices
1. [Guidance on how to best use this dataset]
2. [Common query patterns]
3. [Performance tips]

### Common Use Cases
- [ ] Reporting
- [ ] Analytics
- [ ] Machine Learning
- [ ] Operational
- [ ] Other: ___

### Known Limitations
- [List any known limitations or caveats]

---

## Change Log

| Date | Version | Change Description | Author |
|------|---------|-------------------|--------|
| | 1.0 | Initial creation | |
| | | | |

---

## Related Resources

- **dbt Model**: [Link to dbt model]
- **Dashboard**: [Link to Superset dashboard]
- **Data Quality Suite**: [Link to Great Expectations suite]
- **Runbook**: [Link to operational runbook]
