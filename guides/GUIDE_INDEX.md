# JSON Schema Guides Index

Complete documentation for JSON schema standards and implementation in the changes-worker scope.

---

## 📚 Guide Documents

### 1. **JSON_SCHEMA.md** — Format & Conventions Guide
**Status**: 📋 Active  
**Audience**: Developers, API designers  
**Length**: Comprehensive

**Contains**:
- ✅ Field naming conventions (snake_case)
- ✅ DateTime format standards (Unix epoch vs ISO-8601)
- ✅ Reserved fields reference (Couchbase/CouchDB/Sync Gateway)
- ✅ Data type standards (string, integer, boolean, array, object)
- ✅ Field organization standards
- ✅ Enum value conventions
- ✅ UUID format specifications
- ✅ Required vs optional field patterns
- ✅ Validation constraints
- ✅ Nesting & composition rules
- ✅ Common mistakes & fixes
- ✅ Checklist for document authors

**Use This Guide When**:
- Creating new JSON documents
- Adding fields to collections
- Defining API request/response schemas
- Writing validation code
- Training team members on standards

---

### 2. **SCHEMA_AUDIT_REPORT.md** — Compliance Verification
**Status**: ✅ Complete  
**Audience**: QA, Architects, Reviewers  
**Length**: Detailed

**Contains**:
- ✅ Audit results for all 16 collections
- ✅ Collection-by-collection review
- ✅ Standards compliance matrix
- ✅ System field exceptions (SGs_Seq)
- ✅ DateTime field audit
- ✅ Enum field audit
- ✅ Field naming audit (100% compliant)
- ✅ Reserved fields verification
- ✅ Structural audit
- ✅ Example document validation

**Findings**:
- 16/16 collections audited
- 0 critical issues
- 0 violations
- 100% compliant with format guide

**Use This Guide When**:
- Verifying schema compliance
- Understanding why schemas are structured as-is
- Reviewing changes to schemas
- Auditing new collections

---

## 🎯 Quick Start

### For New Developers

1. **Start Here**: Read [JSON_SCHEMA.md](#1-json_schemamd--format--conventions-guide)
2. **Field Naming**: Use snake_case (e.g., `job_id`, `created_at`)
3. **DateTime**: Use unix epoch for timestamps (e.g., `1705324200`)
4. **Enums**: Use lowercase (e.g., `"status": "running"`)
5. **Example**: See complete example at end of JSON_SCHEMA.md

### For Code Review

1. **Check**: JSON_SCHEMA.md checklist section
2. **Verify**: All required fields present
3. **Validate**: Against format guide standards
4. **Confirm**: Examples are valid

### For Architecture Review

1. **Reference**: SCHEMA_AUDIT_REPORT.md
2. **Understand**: Why collections are structured as-is
3. **Verify**: Compliance status
4. **Plan**: Future changes using standards

---

## 📋 Key Standards Summary

### Field Naming
```
✅ snake_case:      job_id, source_type, created_at
❌ camelCase:       jobId, sourceType, createdAt
❌ PascalCase:      JobId, SourceType, CreatedAt
```

### DateTime
```
✅ Unix epoch:      1705324200 (performance)
✅ ISO-8601:        "2024-01-15T10:30:00Z" (readability)
❌ Mixed:           Not in same field
```

### Reserved Fields
```
✅ Allowed:         Regular fields, _meta container
❌ Forbidden:       _id, _rev, _deleted, _attachments, etc.
```

### Enum Values
```
✅ Lowercase:       "status": "running"
✅ Snake_case:      "output_type": "sync_gateway"
✅ Uppercase HTTP:  "method": "POST"
❌ Mixed case:      "Status": "Running"
```

---

## 📊 Collection Reference

| Collection | Type | Fields | Status |
|-----------|------|--------|--------|
| inputs_changes | Input config | 4 | ✅ Compliant |
| outputs_rdbms | Output config | 12 | ✅ Compliant |
| outputs_http | Output config | 11 | ✅ Compliant |
| outputs_cloud | Output config | 11 | ✅ Compliant |
| jobs | Pipeline job | 14 | ✅ Compliant |
| checkpoints | Progress tracking | 7 | ✅ Compliant |
| dlq | Error queue | 13 | ✅ Compliant |
| config | System config | 28 | ✅ Compliant |
| data_quality | Metrics | 8 | ✅ Compliant |
| enrichments | Transformation | 11 | ✅ Compliant |
| users | User accounts | 10 | ✅ Compliant |
| sessions | Session tokens | 10 | ✅ Compliant |
| audit_log | Audit trail | 9 | ✅ Compliant |
| notifications | Alerts | 12 | ✅ Compliant |
| mappings | Deprecated | 6 | ✅ Compliant |

---

## 🔗 Related Documentation

### Schema Files Location
```
json_schema/
├── README.md                      (Schema overview)
├── COLLECTIONS_SUMMARY.md         (Collection details)
├── INDEX.md                       (File reference)
├── QUICK_REFERENCE.txt            (Quick lookup)
└── changes-worker/
    └── {collection}/schema.json   (16 files)
```

### Implementation Code
- **Python**: `cbl_store.py` (document creation/management)
- **API**: `rest/api_v2.py` (endpoint validation)
- **Validation**: Uses jsonschema library

### Parent Guides Directory
```
guides/
├── GUIDE_INDEX.md                 (This file)
├── GUIDE_LOGGING.md               (Logging standards)
├── JSON_SCHEMA.md                 (Format guide)
├── SCHEMA_AUDIT_REPORT.md         (Compliance report)
```

---

## ❓ FAQ

**Q: Should I use `jobId` or `job_id`?**  
A: Use `job_id` (snake_case). See JSON_SCHEMA.md field naming section.

**Q: When do I use unix timestamp vs ISO-8601?**  
A: Unix for performance (checkpoints, DLQ), ISO-8601 for readability (config, created_at). See DateTime section.

**Q: Can I use `_internal_flag` at the top level?**  
A: No. Use `_meta.internal_flag` instead. Reserved prefix forbidden. See Reserved Fields section.

**Q: Are the example documents valid?**  
A: Yes, all 20 examples validated. See SCHEMA_AUDIT_REPORT.md.

**Q: What if I need a field name with uppercase (like SGs_Seq)?**  
A: Only for system fields from external systems (Sync Gateway, etc). See exceptions in SCHEMA_AUDIT_REPORT.md.

**Q: How do I validate my document?**  
A: Use Python jsonschema library. See JSON_SCHEMA.md validation section.

---

## 📝 Checklist for New Fields

Before adding a field, verify:

- [ ] Field name is snake_case
- [ ] Field name doesn't start with underscore (unless system field)
- [ ] DataType is appropriate (string, integer, boolean, object, array)
- [ ] If enum: all values lowercase/snake_case
- [ ] If timestamp: unix epoch OR ISO-8601 (not both)
- [ ] Field has description
- [ ] Is it required or optional? Set properly
- [ ] Add example value
- [ ] Follows logical ordering (type → id → core → timestamps → objects)

---

## 📞 Support

**For Questions About**:
- Field naming → See JSON_SCHEMA.md
- Compliance → See SCHEMA_AUDIT_REPORT.md
- Specific fields → Check schema file directly
- New standards → Update JSON_SCHEMA.md and re-audit

---

## Version History

| Version | Date | Status | Changes |
|---------|------|--------|---------|
| 1.0 | 2024-04-20 | Current | Initial guides created |

---

## Document Information

- **Document**: GUIDE_INDEX.md
- **Location**: guides/
- **Date**: April 20, 2024
- **Status**: ✅ Active
- **Maintainer**: Project Team

