# Extensions

This document describes the extension architecture for the interspecies eventstream schema, inspired by [Darwin Core](https://dwc.tdwg.org/)'s core-plus-extensions model.

## Architecture: Core Plus Extensions

The eventstream schema follows a **minimal universal core plus domain-specific extensions** pattern:

- **Core**: The fields defined in README.md (eventstream `id`, `provenance`, `start`, `end`, `agents`, `events`, and their sub-fields). These are stable, broadly applicable, and governed by the main schema.
- **Extensions**: Additional fields, event types, agent properties, or context objects defined for specific research domains or use cases. Extensions live in the `other_data` object on events, in agent `metadata`, or as namespaced `type` values.

This separation ensures that the core remains simple and species-agnostic while allowing rich domain-specific data to be attached without schema changes.

## Namespace Convention

Extensions use **dot-scoped namespaces** to avoid collisions:

```
{provider}.{domain}.{name}
```

Examples:
- `cleverpet.ethogram.lip_licking` — an event type from CleverPet's ethogram extension
- `ucdavis.video.frame_ref` — a field from UC Davis's video annotation extension
- `mit.multimodal.signal_type` — a field from MIT's multi-modal extension

Namespaces should use lowercase ASCII with dots as separators. The first segment identifies the organization or data producer; the second identifies the extension domain; the third identifies the specific type or field.

## Proposing New Event Types and Agent Properties

Anyone may propose new event types, agent properties, or context fields for the extension registry. Extensions do not require a schema version bump — they are additive by nature and governed by the must-ignore policy (see [VERSIONING.md](VERSIONING.md)).

### What Can Be Extended

- **Event types**: New values for the `type` field (e.g., `ethogram.tail_wag`, `vocalization.whistle`)
- **Agent properties**: New keys in the agent `metadata` object (e.g., `training_level`, `device_model`)
- **Context fields**: New keys in an event `context` object (e.g., `location`, `weather`)
- **Other data fields**: New keys in `other_data` for domain-specific payloads

## Example Extensions

### Ethogram Extension

For coding animal behaviors from standardized ethograms:

```json
{
    "id": "ucdavis.ethogram.0.1",
    "type": "ucdavis.evenson_ethogram.lip_licking",
    "agent": "ucdavis.dog.42",
    "start": "2024-03-15T14:22:01.000000",
    "content": "lip_licking",
    "other_data": {
        "ethogram_id": "evenson_2019",
        "behavior_category": "oral",
        "duration_ms": 1200,
        "coder_id": "researcher_A",
        "confidence": 0.95
    }
}
```

### Video Annotation Extension

For linking events to video frame ranges:

```json
{
    "id": "mit.video.0.5",
    "type": "button_press",
    "agent": "mit.dog.7",
    "start": "2024-06-01T09:15:30.000000",
    "end": "2024-06-01T09:15:31.500000",
    "content": "outside",
    "other_data": {
        "mit.video.source": "session_2024-06-01_cam1.mp4",
        "mit.video.frame_start": 13542,
        "mit.video.frame_end": 13587,
        "mit.video.bounding_box": [120, 340, 280, 510]
    }
}
```

### Multi-Modal Signal Extension

For capturing concurrent signals across sensory modalities:

```json
{
    "id": "caltech.multimodal.0.3",
    "type": "caltech.multimodal.composite_signal",
    "agent": "caltech.dolphin.12",
    "start": "2024-08-20T11:05:00.000000",
    "end": "2024-08-20T11:05:02.500000",
    "content": "attention_call",
    "other_data": {
        "caltech.multimodal.channels": [
            {
                "modality": "acoustic",
                "frequency_hz": 8200,
                "media_ref": "hydrophone_clip_0803.wav"
            },
            {
                "modality": "kinetic",
                "description": "pectoral_fin_slap",
                "media_ref": "underwater_cam_0803.mp4"
            }
        ]
    }
}
```

## Extension Proposal Template

To propose a new extension, open a pull request adding your extension to this file using the following template:

---

### Extension: {namespace}.{domain}

**Proposer:** Name / Organization

**Date:** YYYY-MM-DD

**Status:** Draft | Under Review | Accepted

#### Purpose

Brief description of what research question or data capture need this extension addresses.

#### Scope

- Event types introduced: (list)
- Agent properties introduced: (list)
- Other data fields introduced: (list)

#### Schema

```json
{
    "field_name": "type and description",
    "field_name_2": "type and description"
}
```

#### Example

```json
{
    "id": "example.0.1",
    "type": "namespace.domain.event_type",
    ...
}
```

#### Compatibility Notes

Any notes on interactions with existing fields or other extensions.

---
