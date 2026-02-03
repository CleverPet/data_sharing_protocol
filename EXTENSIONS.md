# Extensions

This document describes the extension architecture for the interspecies eventstream schema, inspired by [Darwin Core](https://dwc.tdwg.org/)'s core-plus-extensions model.

## Architecture: Core Plus Extensions

As of schema version 1.1.0, the core schema natively supports signal-level data for acoustic, electric, visual, and other communication modalities. The first-class `signal`, `recording`, and `classification` fields on events, along with `communication_profile` on agents, cover the most common bioacoustic and multi-modal data needs directly.

Extensions are for **domain-specific additions** that go beyond what the core provides:

- **Specialized classification taxonomies**: Custom call-type catalogs, ethogram behavior codes, or species-specific labeling systems that reference the core `classification.taxonomy` field.
- **Analysis metadata**: Outputs from specific analysis pipelines (e.g., deep learning feature embeddings, spectrogram parameters, source-separation results) that attach to events via `other_data`.
- **Novel modalities**: Sensory channels or data types not yet covered by the core `signal.modality` values (e.g., electroreception waveform data, bioluminescence patterns).
- **Video and spatial annotation**: Frame-level video references, bounding boxes, 3D tracking coordinates, and other spatial data.
- **Ethogram coding**: Standardized behavioral observations from ethogram frameworks.

The core handles the **what, when, who, and how** of communication events. Extensions add the **domain-specific why and details**.

## Namespace Convention

Extensions use **dot-scoped namespaces** to avoid collisions:

```
{provider}.{domain}.{name}
```

Examples:
- `cleverpet.ethogram.lip_licking` — an event type from CleverPet's ethogram extension
- `ucdavis.video.frame_ref` — a field from UC Davis's video annotation extension
- `whoi.dclde.detection_class` — a classification taxonomy from WHOI's DCLDE framework

Namespaces should use lowercase ASCII with dots as separators. The first segment identifies the organization or data producer; the second identifies the extension domain; the third identifies the specific type or field.

## Proposing New Event Types and Agent Properties

Anyone may propose new event types, agent properties, or context fields for the extension registry. Extensions do not require a schema version bump — they are additive by nature and governed by the must-ignore policy (see [VERSIONING.md](VERSIONING.md)).

### What Can Be Extended

- **Event types**: New values for the `type` field (e.g., `ethogram.tail_wag`, `vocalization.whistle`)
- **Agent properties**: New keys in the agent `metadata` object (e.g., `training_level`, `device_model`)
- **Context fields**: New keys in an event `context` object (e.g., `location`, `weather`)
- **Other data fields**: New keys in `other_data` for domain-specific payloads
- **Classification taxonomies**: Custom taxonomy references used in the core `classification.taxonomy` field
- **Signal metadata**: Additional keys in the `signal` object (it allows `additionalProperties`)

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
    "duration_ms": 1200,
    "other_data": {
        "ethogram_id": "evenson_2019",
        "behavior_category": "oral",
        "coder_id": "researcher_A"
    },
    "classification": {
        "method": "manual",
        "label": "lip_licking",
        "confidence": 0.95,
        "taxonomy": "evenson_ethogram_2019"
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

### Species-Specific Classification Taxonomy Extension

For registering a custom whistle catalog as a classification taxonomy:

```json
{
    "id": "sarasota.catalog.0.1",
    "type": "vocalization",
    "agent": "sarasota.dolphin.FB185",
    "start": "2024-07-15T09:23:14.337000",
    "content": "signature_whistle",
    "signal": {
        "call_type": "signature_whistle",
        "modality": "acoustic",
        "dominant_freq_hz": 8900
    },
    "classification": {
        "method": "template_matching",
        "label": "FB185_signature",
        "confidence": 0.92,
        "taxonomy": "sarasota_whistle_catalog_2024"
    },
    "other_data": {
        "sarasota.catalog.whistle_id": "FB185-SW-A",
        "sarasota.catalog.first_recorded": "2018-06-12",
        "sarasota.catalog.n_matches": 47
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
- Classification taxonomies introduced: (list)

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

Any notes on interactions with existing core fields (signal, recording, classification) or other extensions.

---
