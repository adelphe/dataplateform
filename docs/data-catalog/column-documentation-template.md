# Column Documentation Template

Use this template for detailed column-level documentation. Useful for critical fields that require extensive explanation.

---

## Column Overview

| Attribute | Value |
|-----------|-------|
| **Column Name** | `[column_name]` |
| **Table** | `[schema.table_name]` |
| **Display Name** | [Human-readable name] |
| **Data Type** | [VARCHAR(255) / INTEGER / DECIMAL(10,2) / TIMESTAMP / etc.] |
| **Nullable** | [ ] Yes  [ ] No |

---

## Business Definition

### Description
[Detailed description of what this column represents from a business perspective]

### Glossary Term
- **Related Term**: [Link to glossary term if exists]

### Business Rules
1. [Rule 1: e.g., "Must be positive for revenue transactions"]
2. [Rule 2: e.g., "NULL indicates pending status"]

---

## Technical Details

### Source Mapping

| Source System | Source Table | Source Column | Transformation |
|---------------|--------------|---------------|----------------|
| | | | |

### Transformation Logic
```sql
-- Transformation applied to derive this column
[SQL or description of transformation]
```

### Dependencies
- [Other columns this depends on]

---

## Domain & Constraints

### Valid Values
| Value | Meaning | Notes |
|-------|---------|-------|
| | | |

### Value Range
- **Minimum**:
- **Maximum**:
- **Default**:

### Format / Pattern
- **Format**: [e.g., "YYYY-MM-DD", "XXX-NNNN"]
- **Regex**: [if applicable]
- **Example**:

---

## Data Quality

### Quality Rules

| Rule | Description | Threshold |
|------|-------------|-----------|
| Completeness | Non-null percentage | |
| Uniqueness | Unique value percentage | |
| Validity | Values within valid range | |
| Pattern Match | Matches expected format | |

### Common Issues
| Issue | Cause | Resolution |
|-------|-------|------------|
| | | |

---

## Classification & Security

### Data Classification
- [ ] Public
- [ ] Internal
- [ ] Confidential
- [ ] Restricted

### PII Classification
- [ ] Not PII
- [ ] Direct PII (e.g., name, email)
- [ ] Indirect PII (e.g., can identify with other data)
- [ ] Sensitive PII (e.g., SSN, health data)

### Handling Requirements
[Special handling requirements if PII or sensitive]

---

## Usage Guidelines

### When to Use
[Scenarios where this column should be used]

### When NOT to Use
[Scenarios where this column should be avoided]

### Common Query Patterns
```sql
-- Example 1: [Description]
SELECT [column_name]
FROM [table]
WHERE [condition];

-- Example 2: [Description]
SELECT [aggregation]
FROM [table]
GROUP BY [column_name];
```

### Performance Tips
- [Tip 1: e.g., "Index exists - filter performance is good"]
- [Tip 2: e.g., "Avoid LIKE patterns starting with %"]

---

## Related Columns

| Column | Table | Relationship |
|--------|-------|--------------|
| | | Same as (duplicate) |
| | | Derived from |
| | | Related to |

---

## Change History

| Date | Change | Reason | Author |
|------|--------|--------|--------|
| | Created | Initial implementation | |
| | | | |

---

## Owner

- **Data Owner**: [Name, email]
- **Technical Owner**: [Name, email]
