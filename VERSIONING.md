# Versioning & Compatibility

This document defines the versioning policy for the interspecies eventstream schema.

## Schema Version

Every eventstream file MUST include a top-level `schema_version` field using [Semantic Versioning 2.0.0](https://semver.org/):

```json
{
    "schema_version": "1.0.0",
    "id": "cleverpet.75",
    "provenance": "cleverpet",
    ...
}
```

The `schema_version` field indicates which version of the eventstream schema the file conforms to.

## Semver Policy

We follow Semantic Versioning (`MAJOR.MINOR.PATCH`):

- **PATCH** (e.g., 1.0.0 → 1.0.1): Documentation-only changes, clarifications, typo fixes. No changes to field definitions or validation behavior.
- **MINOR** (e.g., 1.0.0 → 1.1.0): Additive, backward-compatible changes. New optional fields, new event types, new extension registrations. Existing files remain valid without modification.
- **MAJOR** (e.g., 1.0.0 → 2.0.0): Breaking changes. Renamed or removed fields, changed types, new required fields, altered semantics of existing fields.

## Unknown Field Handling: Must-Ignore and Preserve

Implementations MUST follow a **must-ignore and preserve** policy for unknown fields:

1. **Must-Ignore**: Readers MUST NOT reject an eventstream file because it contains fields they do not recognize. Unknown fields should be silently accepted.
2. **Must-Preserve**: When reading and re-writing an eventstream file, implementations MUST preserve any unknown fields in their original form. Fields not recognized by a reader must be round-tripped without loss.

This policy enables forward compatibility: a file written with schema version 1.2.0 can be safely read by a tool that only understands 1.0.0. The tool will ignore the new fields but will not discard them.

## Backward Compatibility Guarantees

- Any file valid under schema version `X.Y.Z` will remain valid under all future versions `X.*.*` (same major version).
- New minor versions only add optional fields or new enum values for `type`-like fields.
- Tools targeting version `1.0.0` can safely read any `1.x.x` file.

## Forward Compatibility Guarantees

- A file written under a newer minor version (e.g., `1.3.0`) can be read by a tool targeting an older minor version (e.g., `1.0.0`) thanks to the must-ignore policy.
- A file written under a newer major version (e.g., `2.0.0`) may NOT be readable by tools targeting an older major version.
- Tools SHOULD check the major version number and warn (but not fail) when encountering a higher minor version than they support.

## Migration Guide Template

When a new major version is released, a migration guide MUST be provided following this template:

---

### Migration Guide: Version X.0.0 → Y.0.0

**Release date:** YYYY-MM-DD

**Summary:** Brief description of why a major version bump was necessary.

#### Breaking Changes

| Change | Old Behavior | New Behavior | Action Required |
|--------|-------------|-------------|-----------------|
| _description_ | _what it was_ | _what it is now_ | _what to do_ |

#### Removed Fields

- `field_name`: Reason for removal. Suggested alternative.

#### Changed Fields

- `field_name`: Description of type or semantic change.

#### New Required Fields

- `field_name`: Description and default value suggestion for migrating old files.

#### Migration Steps

1. Step-by-step instructions for updating files.
2. Include example before/after JSON snippets where helpful.

#### Tooling

- `migrate.py` (if provided): Usage instructions for automated migration.

---
