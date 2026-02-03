# PR Proposal: Evolve data_sharing_protocol toward interspecies communication standard

## Context
This repo defines a JSON eventstream format for interspecies AIC device interactions.
Currently it's practical and operational (button_press events from FluentPet devices),
but needs evolution toward a more robust, extensible, species-agnostic standard.

## Current Schema (from README.md)
- Eventstream: {id, provenance, start, end, agents[], events[]}
- Agent: {id, species}
- Event: {id, type, agent, start, end, content, other_data}

## What This PR Should Include

### 1. JSON Schema (schema/eventstream.schema.json)
- Formal JSON Schema for the eventstream format
- `additionalProperties: true` on all objects (allow extension without breaking)
- Required vs optional fields clearly defined
- Schema version field (`$schema` or `schema_version`) on every eventstream

### 2. Enhanced Agent Model (backward-compatible additions)
- Optional `metadata` object on Agent for: breed, age, sex, name, training_method
- Optional `group_id` for multi-agent scenarios (packs, households)
- Optional `role` field (e.g., "learner", "teacher", "observer")
- Keep existing fields unchanged

### 3. Enhanced Event Model (backward-compatible additions)
- Optional `context` object for environmental data (location, who_present, preceding_event_id)
- Optional `confidence` field (0-1) for automated/inferred events
- Optional `media_refs` array for linked video/audio recordings
- Optional `annotations` array for researcher-added metadata
- Scoped type namespacing: document the `provenance.schema.type` convention already hinted at

### 4. Versioning & Compatibility (VERSIONING.md)
- Explicit schema version in every file (add `schema_version: "1.1.0"` to eventstream)
- Semver policy: patch = docs, minor = additive fields, major = breaking changes
- Unknown field handling policy: "must-ignore and preserve" (readers MUST NOT reject unknown fields)
- Backward/forward compatibility guarantees

### 5. Extension Registry (EXTENSIONS.md)
- Document how to propose new event types, agent properties, or context fields
- Darwin Core-style: minimal universal core + domain-specific extensions
- Examples: ethogram extensions, video annotation extensions, multi-modal signal extensions

### 6. Updated README
- Reference the JSON Schema
- Add versioning section
- Link to EXTENSIONS.md and VERSIONING.md
- Keep existing content, add to it

### 7. Validation Script (validate.py)
- Simple Python script that validates an eventstream JSON against the schema
- Usage: `python validate.py data_sample/cleverpet.1.json`
- Reports errors clearly

## Design Principles (from Vint Cerf's interspecies vision)
- Store-and-forward resilience (intermittent connectivity)
- Namespace flexibility via scoped IDs
- Open extensibility via community contribution
- Species-agnosticism (no assumptions about sensory modality or temporal resolution)

## Rules
- ALL changes must be backward-compatible with existing data_sample/ files
- Existing files must validate against the new schema without modification
- No breaking changes to current field names or types
- Python 3.10+ only, no external dependencies for validate.py
- Keep it simple — this is an academic/research tool, not enterprise software
