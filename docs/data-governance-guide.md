# Data Governance Guide

This guide establishes the data governance framework for the data platform, defining ownership, stewardship, access controls, and operational procedures.

## Table of Contents

1. [Overview](#overview)
2. [Governance Framework](#governance-framework)
3. [Roles and Responsibilities](#roles-and-responsibilities)
4. [Data Ownership](#data-ownership)
5. [Data Stewardship](#data-stewardship)
6. [Data Classification](#data-classification)
7. [Access Control](#access-control)
8. [Data Quality](#data-quality)
9. [Metadata Management](#metadata-management)
10. [Data Lineage](#data-lineage)
11. [Compliance and Audit](#compliance-and-audit)
12. [Operational Procedures](#operational-procedures)

---

## Overview

### Purpose

This data governance framework ensures that data assets are:
- **Discoverable**: Easy to find through the data catalog
- **Understandable**: Well-documented with clear definitions
- **Trustworthy**: Validated for quality and accuracy
- **Secure**: Protected with appropriate access controls
- **Compliant**: Managed according to regulatory requirements

### Scope

This framework applies to all data assets within the data platform:
- PostgreSQL database schemas (raw, staging, intermediate, marts)
- MinIO storage buckets (bronze, silver, gold layers)
- dbt transformation models
- Superset dashboards and reports
- API endpoints and data exports

### Governance Tools

| Tool | Purpose | Access |
|------|---------|--------|
| **OpenMetadata** | Data catalog, lineage, glossary | http://localhost:8585 |
| **Airflow** | Pipeline orchestration, metadata sync | http://localhost:8080 |
| **Great Expectations** | Data quality validation | Integrated with Airflow |
| **Superset** | Business intelligence, reporting | http://localhost:8088 |

---

## Governance Framework

### Principles

1. **Data as an Asset**: Treat data as a strategic organizational asset
2. **Accountability**: Clear ownership for all data assets
3. **Transparency**: Visible metadata and lineage
4. **Quality**: Continuous monitoring and improvement
5. **Security**: Protection appropriate to data sensitivity
6. **Compliance**: Adherence to regulations and policies

### Governance Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    STRATEGIC LAYER                          │
│  Data Governance Council, Policies, Standards               │
├─────────────────────────────────────────────────────────────┤
│                    TACTICAL LAYER                           │
│  Data Stewards, Domain Experts, Quality Rules               │
├─────────────────────────────────────────────────────────────┤
│                   OPERATIONAL LAYER                         │
│  Data Engineers, Automated Processes, Monitoring            │
└─────────────────────────────────────────────────────────────┘
```

---

## Roles and Responsibilities

### Data Governance Council

**Composition**: Senior stakeholders from each business domain

**Responsibilities**:
- Define and approve data governance policies
- Resolve cross-domain data issues
- Prioritize governance initiatives
- Review governance metrics quarterly

### Data Owner

**Definition**: Business stakeholder accountable for a data domain or dataset

**Responsibilities**:
- Define business requirements for data
- Approve access requests for their datasets
- Ensure data meets business needs
- Designate data stewards
- Make decisions about data retention and archival

**Selection Criteria**:
- Business domain expertise
- Authority to make decisions about the data
- Understanding of data consumers and use cases

### Data Steward

**Definition**: Individual responsible for day-to-day data governance activities

**Responsibilities**:
- Maintain metadata and documentation
- Define and monitor data quality rules
- Investigate and resolve data issues
- Coordinate with data engineers on technical implementation
- Train users on data usage best practices

**Selection Criteria**:
- Deep knowledge of data content and business context
- Technical understanding of data systems
- Strong communication skills

### Technical Owner

**Definition**: Engineer responsible for technical implementation and operations

**Responsibilities**:
- Implement data pipelines and transformations
- Maintain technical infrastructure
- Implement security controls
- Monitor system performance
- Execute technical changes requested by stewards

---

## Data Ownership

### Ownership Assignment

Every dataset must have an assigned owner. Ownership is tracked in:
- OpenMetadata (primary source of truth)
- `governance.data_owners` PostgreSQL table

### Ownership Matrix

| Data Domain | Schema | Owner Role | Steward Role |
|-------------|--------|------------|--------------|
| Raw Ingestion | `raw` | Data Platform Team | Data Engineering |
| Staging | `staging` | Data Platform Team | Data Engineering |
| Customer Data | `marts.mart_customer_*` | Customer Analytics Lead | Customer Data Steward |
| Product Data | `marts.mart_product_*` | Product Analytics Lead | Product Data Steward |
| Financial Data | `marts.mart_*_summary` | Finance Lead | Finance Data Steward |
| Operations | `intermediate` | Data Platform Team | Data Engineering |

### Ownership Transfer Process

1. Current owner submits transfer request
2. New owner accepts responsibility
3. Knowledge transfer session completed
4. Documentation updated in OpenMetadata
5. Access permissions transferred
6. Notification sent to stakeholders

### Orphaned Data Policy

Data without an owner for 30+ days:
1. Alert sent to Data Governance Council
2. Temporary ownership assigned to domain lead
3. Data assessed for retention or deprecation
4. Permanent owner assigned or data archived

---

## Data Stewardship

### Steward Responsibilities by Phase

#### Ingestion Phase
- Review source data documentation
- Define expected schema and data types
- Establish initial quality rules
- Document known data issues

#### Transformation Phase
- Validate business logic implementation
- Review dbt model documentation
- Ensure proper testing coverage
- Maintain column-level lineage

#### Consumption Phase
- Support data consumers with questions
- Monitor usage patterns
- Gather feedback for improvements
- Maintain glossary definitions

### Stewardship Activities

| Activity | Frequency | Output |
|----------|-----------|--------|
| Metadata review | Weekly | Updated documentation |
| Quality rule review | Monthly | Refined quality checks |
| Consumer feedback | Monthly | Improvement backlog |
| Lineage validation | Quarterly | Validated lineage graph |
| Policy compliance audit | Quarterly | Compliance report |

### Steward Communication

- **Slack Channel**: `#data-stewardship`
- **Office Hours**: Tuesdays 2-3 PM
- **Issue Tracker**: GitHub Issues with `data-governance` label

---

## Data Classification

### Classification Levels

| Level | Description | Examples | Controls |
|-------|-------------|----------|----------|
| **Public** | Can be shared externally | Product catalog, public reports | Standard access |
| **Internal** | For internal use only | Aggregated metrics, operational data | Authentication required |
| **Confidential** | Sensitive business data | Revenue data, customer segments | Role-based access |
| **Restricted** | Highly sensitive | PII, financial details | Approval required, audit logging |

### PII Classification

| Category | Examples | Handling Requirements |
|----------|----------|----------------------|
| **Direct PII** | Name, email, phone, SSN | Encryption, masking, audit |
| **Indirect PII** | IP address, device ID | Anonymization recommended |
| **Sensitive PII** | Health data, financial account | Strict access, encryption |

### Classification Process

1. Data steward assesses data content
2. Classification tag applied in OpenMetadata
3. Access controls configured based on classification
4. Handling procedures documented
5. Regular review (annually minimum)

---

## Access Control

### Role-Based Access Control (RBAC)

Roles are defined in `infrastructure/openmetadata/rbac/roles_config.yaml`

| Role | Read | Write | Admin | Description |
|------|------|-------|-------|-------------|
| DataConsumer | ✓ | - | - | Browse catalog, view metadata |
| DataAnalyst | ✓ | Query | - | Run queries, create reports |
| DataEngineer | ✓ | ✓ | - | Build pipelines, manage data |
| DataSteward | ✓ | Metadata | Governance | Manage metadata, quality |
| DataOwner | ✓ | ✓ | Own datasets | Full control of owned data |
| DataPlatformAdmin | ✓ | ✓ | ✓ | Platform administration |

### Access Request Process

1. **Request**: User submits access request via OpenMetadata
   - Dataset name
   - Access level needed
   - Business justification
   - Duration (if temporary)

2. **Review**: Data owner reviews request
   - Validates business need
   - Checks classification requirements
   - Considers compliance implications

3. **Approval/Denial**: Decision documented
   - If approved: Access granted, tracked in system
   - If denied: Reason provided to requester

4. **Provisioning**: Technical team implements access

5. **Audit**: Access logged and reviewed periodically

### Access Revocation

Access is revoked when:
- Employee leaves organization
- Role changes
- Access period expires
- Data owner requests revocation
- Security incident occurs

---

## Data Quality

### Quality Dimensions

| Dimension | Definition | Example Rules |
|-----------|------------|---------------|
| **Completeness** | Required fields are populated | Non-null checks |
| **Uniqueness** | No unwanted duplicates | Primary key uniqueness |
| **Validity** | Values within expected range | Email format validation |
| **Consistency** | Data matches across systems | Cross-table reconciliation |
| **Timeliness** | Data is sufficiently current | Freshness checks |
| **Accuracy** | Data reflects real-world truth | Business rule validation |

### Quality Framework

```
┌─────────────────────────────────────────────────────────────┐
│                  Great Expectations                         │
│  Expectation Suites → Checkpoints → Validation Results      │
├─────────────────────────────────────────────────────────────┤
│                  Quality Monitoring                         │
│  Metrics Collection → Alerting → Dashboards                 │
├─────────────────────────────────────────────────────────────┤
│                  Issue Resolution                           │
│  Detection → Investigation → Resolution → Prevention        │
└─────────────────────────────────────────────────────────────┘
```

### Quality SLAs

| Data Layer | Freshness SLA | Completeness SLA | Availability SLA |
|------------|---------------|------------------|------------------|
| Bronze (Raw) | < 4 hours | > 95% | 99% |
| Silver (Staging) | < 6 hours | > 99% | 99.5% |
| Gold (Marts) | < 8 hours | > 99.5% | 99.9% |

### Quality Issue Escalation

1. **P1 (Critical)**: Data completely unavailable or severely corrupted
   - Response: Immediate
   - Escalate to: Data Platform Team Lead

2. **P2 (High)**: Significant quality degradation affecting business
   - Response: Within 2 hours
   - Escalate to: Data Steward

3. **P3 (Medium)**: Quality below threshold but usable
   - Response: Within 1 business day
   - Escalate to: Technical Owner

4. **P4 (Low)**: Minor issues, documentation gaps
   - Response: Within 1 week
   - Escalate to: Data Steward

---

## Metadata Management

### Metadata Types

| Type | Description | Managed In |
|------|-------------|------------|
| **Technical** | Schema, types, constraints | OpenMetadata, dbt |
| **Business** | Descriptions, glossary terms | OpenMetadata |
| **Operational** | Lineage, freshness, quality | OpenMetadata, Airflow |
| **Administrative** | Ownership, classification | OpenMetadata |

### Metadata Sync Process

The `metadata_sync` DAG runs every 6 hours to:
1. Extract metadata from PostgreSQL schemas
2. Catalog MinIO storage objects
3. Parse dbt manifest for model metadata
4. Update lineage relationships
5. Validate metadata completeness
6. Generate governance reports

### Documentation Standards

#### Table Documentation
- Business description (required)
- Data source (required)
- Update frequency (required)
- Owner (required)
- Column descriptions (required for marts)

#### Column Documentation
- Business definition
- Data type and constraints
- Example values
- Related glossary terms
- PII classification if applicable

See templates in `docs/data-catalog/` for standardized documentation.

---

## Data Lineage

### Lineage Visualization

OpenMetadata provides interactive lineage visualization:
1. Navigate to asset in catalog
2. Click "Lineage" tab
3. View upstream (sources) and downstream (consumers)
4. Click nodes to explore further

### Lineage Sources

| Source | Type | Captured By |
|--------|------|-------------|
| dbt models | SQL transformations | dbt manifest parsing |
| Airflow DAGs | Pipeline orchestration | DAG task metadata |
| PostgreSQL | Table relationships | Schema introspection |
| Manual | External systems | Manual lineage API |

### Lineage Use Cases

1. **Impact Analysis**: Understand downstream effects of changes
2. **Root Cause Analysis**: Trace data issues to source
3. **Compliance**: Document data flows for auditors
4. **Documentation**: Auto-generate data flow diagrams

### Maintaining Lineage

- dbt: Lineage captured automatically from `ref()` and `source()`
- Custom SQL: Document in lineage_ingestion.yaml
- External sources: Use OpenMetadata API to register

---

## Compliance and Audit

### Audit Logging

All data access is logged:
- User identity
- Timestamp
- Action performed
- Data accessed
- Query executed (for SQL)

Logs retained for 2 years minimum.

### Compliance Checkpoints

| Checkpoint | Frequency | Owner |
|------------|-----------|-------|
| Access review | Quarterly | Data Owners |
| Classification audit | Annually | Data Stewards |
| Quality metrics review | Monthly | Data Stewards |
| Policy compliance | Annually | Governance Council |

### Regulatory Considerations

For regulated data:
- Document legal basis for processing
- Implement data subject rights procedures
- Maintain records of processing activities
- Conduct data protection impact assessments

---

## Operational Procedures

### Adding a New Dataset

1. **Request**: Submit dataset request form
2. **Design**: Define schema, quality rules, classification
3. **Implement**: Create ingestion pipeline, dbt model
4. **Document**: Add metadata in OpenMetadata
5. **Review**: Data steward validates documentation
6. **Publish**: Enable access, announce availability

### Deprecating a Dataset

1. **Announce**: 30-day deprecation notice to consumers
2. **Document**: Mark as deprecated in catalog
3. **Migrate**: Help consumers move to alternatives
4. **Archive**: Move to archive schema (retain 1 year)
5. **Delete**: Remove after retention period

### Schema Changes

1. **Propose**: Document change and impact analysis
2. **Review**: Data steward and consumers review
3. **Approve**: Owner approves change
4. **Implement**: Execute in non-production first
5. **Validate**: Verify quality and lineage
6. **Deploy**: Promote to production
7. **Notify**: Announce change to consumers

### Incident Response

1. **Detect**: Alert triggered or issue reported
2. **Triage**: Assess severity and impact
3. **Communicate**: Notify affected stakeholders
4. **Investigate**: Identify root cause
5. **Resolve**: Implement fix
6. **Validate**: Verify resolution
7. **Document**: Record incident and learnings
8. **Prevent**: Implement preventive measures

---

## Quick Reference

### Key Contacts

| Role | Contact |
|------|---------|
| Data Governance Lead | governance@example.com |
| Data Platform Team | data-platform@example.com |
| Security Team | security@example.com |

### Key Links

- Data Catalog: http://localhost:8585
- Pipeline Dashboard: http://localhost:8080
- BI Portal: http://localhost:8088
- Documentation: `docs/` directory

### Getting Help

1. Check documentation in `docs/` directory
2. Search the data catalog for existing answers
3. Post in `#data-help` Slack channel
4. Contact your domain's data steward
5. Escalate to Data Governance Lead if needed
