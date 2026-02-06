# Data Product Template

Use this template to document data products - curated, quality-assured datasets designed for specific use cases.

---

## Data Product Overview

| Attribute | Value |
|-----------|-------|
| **Product Name** | [Data product name] |
| **Product ID** | [Unique identifier] |
| **Version** | [Semantic version: X.Y.Z] |
| **Description** | [Purpose and value proposition] |
| **Domain** | [Business domain] |
| **Tier** | [ ] Tier 1 (Critical)  [ ] Tier 2 (Important)  [ ] Tier 3 (Standard) |

---

## Ownership & Contacts

| Role | Name | Email | Responsibilities |
|------|------|-------|-----------------|
| **Product Owner** | | | Business decisions, prioritization |
| **Data Steward** | | | Quality, governance, metadata |
| **Technical Lead** | | | Implementation, maintenance |
| **Support Contact** | | | Operational issues |

---

## Value Proposition

### Target Audience
- [Primary consumers of this data product]

### Use Cases
1. **Use Case 1**: [Description]
   - Business Question: [What question does this answer?]
   - Expected Outcome: [What decision/action does this enable?]

2. **Use Case 2**: [Description]
   - Business Question:
   - Expected Outcome:

### Business Metrics Enabled
- [Metric 1]: [Description]
- [Metric 2]: [Description]

---

## Data Assets

### Included Datasets

| Dataset | Schema | Description | Refresh Frequency |
|---------|--------|-------------|-------------------|
| | | | |
| | | | |

### Data Lineage Summary
```
[Source Systems]
       ↓
   [Bronze Layer]
       ↓
   [Silver Layer]
       ↓
   [Gold Layer / This Product]
       ↓
[Consumers: Dashboards, Reports, ML Models]
```

---

## Quality Assurance

### SLA Commitments

| Metric | Target | Current |
|--------|--------|---------|
| **Data Freshness** | [e.g., < 4 hours] | |
| **Data Completeness** | [e.g., > 99.5%] | |
| **Data Accuracy** | [e.g., > 99%] | |
| **Availability** | [e.g., 99.9%] | |

### Quality Checks
- [ ] Automated quality checks in place
- [ ] Anomaly detection enabled
- [ ] Data reconciliation with source

### Quality Dashboard
- **Link**: [Link to quality monitoring dashboard]

---

## Access & Security

### Access Requirements
| Access Level | Role Required | Use Case |
|--------------|---------------|----------|
| Read | DataConsumer | Browsing, basic queries |
| Analyze | DataAnalyst | Analytics, reporting |
| Full | DataEngineer | Development, debugging |

### Data Classification
- **Overall Classification**: [ ] Public  [ ] Internal  [ ] Confidential  [ ] Restricted
- **PII Present**: [ ] Yes  [ ] No
- **Export Restrictions**: [Any restrictions on data export]

### How to Request Access
1. [Step 1: Submit request via...]
2. [Step 2: Approval workflow...]
3. [Step 3: Access provisioning...]

---

## Technical Specifications

### Architecture
- **Storage Layer**: [PostgreSQL / MinIO / Both]
- **Primary Interface**: [SQL / API / File Export]
- **Supported Formats**: [Parquet / CSV / JSON / etc.]

### Endpoints & Connection

#### SQL Access
```
Host: postgres
Port: 5432
Database: airflow
Schema: marts
```

#### Sample Query
```sql
SELECT *
FROM marts.[table_name]
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
LIMIT 100;
```

---

## Operational Information

### Refresh Pipeline
- **DAG Name**: [Airflow DAG name]
- **Schedule**: [Cron expression and description]
- **Typical Runtime**: [Duration]
- **Dependencies**: [Upstream DAGs/datasets]

### Monitoring
- **Health Dashboard**: [Link]
- **Alerts**: [Alert channels - Slack, email, etc.]
- **On-Call**: [On-call rotation or contact]

### Incident Response
- **Runbook**: [Link to operational runbook]
- **Escalation Path**: [Escalation process]

---

## Versioning & Compatibility

### Current Version
- **Version**:
- **Release Date**:
- **Release Notes**:

### Breaking Changes Policy
[Describe how breaking changes are communicated and handled]

### Deprecation Policy
[Describe deprecation timeline and process]

### Previous Versions
| Version | Status | Sunset Date | Migration Guide |
|---------|--------|-------------|-----------------|
| | | | |

---

## Documentation & Resources

### User Documentation
- **User Guide**: [Link]
- **FAQ**: [Link]
- **Training Materials**: [Link]

### Technical Documentation
- **dbt Documentation**: [Link]
- **ERD Diagram**: [Link]
- **API Documentation**: [Link if applicable]

### Feedback & Support
- **Feedback Channel**: [Slack channel, email, etc.]
- **Feature Requests**: [How to submit]
- **Bug Reports**: [How to report]

---

## Roadmap

### Planned Enhancements
| Feature | Target Date | Status |
|---------|-------------|--------|
| | | |

### Known Limitations
- [Limitation 1]
- [Limitation 2]

---

## Change Log

| Date | Version | Change | Author |
|------|---------|--------|--------|
| | 1.0.0 | Initial release | |
