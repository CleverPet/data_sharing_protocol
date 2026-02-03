# Extensions

This document describes the extension architecture for the interspecies eventstream schema, inspired by [Darwin Core](https://dwc.tdwg.org/)'s core-plus-extensions model.

## Architecture: Core Plus Extensions

The core schema (version 1.2.0) provides a modality-agnostic `measurements` array, a `source` object for sensor metadata, a `spatial` object for position/trajectory, and a `classification` object. These cover the structural **how** of attaching data to events. What remains open — and what extensions govern — is the **vocabulary**: what dimension names mean, how they relate to each other, and which taxonomies apply.

Extensions are **dimension vocabularies and taxonomies**, not new schema fields:

- **Dimension vocabularies**: Standardized `dimension` names for a domain. For example, a bioacoustics vocabulary defines `freq_min_hz`, `freq_max_hz`, `dominant_freq_hz`, `bandwidth_hz`, `call_type` as conventional dimension names for acoustic measurements. A waggle-dance vocabulary defines `waggle_angle_deg`, `waggle_duration_ms`, `indicated_distance_m`.
- **Classification taxonomies**: Custom call-type catalogs, ethogram behavior codes, or species-specific labeling systems referenced by `classification.taxonomy`.
- **Event type registries**: Namespaced event types like `ethogram.play_bow` or `waggle_run` that communities agree on.
- **Analysis metadata**: Outputs from specific analysis pipelines (e.g., deep learning feature embeddings, spectrogram parameters) stored in `other_data`.

The core schema handles the **structure**. Extensions standardize the **vocabulary**.

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

- **Event types**: New values for the `type` field (e.g., `ethogram.tail_wag`, `waggle_dance`)
- **Dimension vocabularies**: Standardized `dimension` names for use in `measurements` (e.g., `freq_min_hz`, `waggle_angle_deg`, `tail_wag_freq_hz`)
- **Agent properties**: New keys in the agent `metadata` object (e.g., `training_level`, `device_model`)
- **Context fields**: New keys in an event `context` object (e.g., `location`, `weather`)
- **Other data fields**: New keys in `other_data` for domain-specific payloads
- **Classification taxonomies**: Custom taxonomy references used in the core `classification.taxonomy` field

## Example Extensions

### Bioacoustics Dimension Vocabulary

Conventional dimension names for acoustic measurements:

| Dimension | Unit | Description |
|-----------|------|-------------|
| `freq_min_hz` | Hz | Minimum frequency |
| `freq_max_hz` | Hz | Maximum frequency |
| `dominant_freq_hz` | Hz | Dominant / peak frequency |
| `bandwidth_hz` | Hz | Signal bandwidth |
| `click_count` | — | Number of clicks in a train |
| `mean_ici_ms` | ms | Mean inter-click interval |

### Waggle Dance Dimension Vocabulary

Conventional dimension names for honeybee dance communication:

| Dimension | Unit | Description |
|-----------|------|-------------|
| `waggle_angle_deg` | deg | Angle of waggle run relative to vertical |
| `waggle_duration_ms` | ms | Duration of waggle phase |
| `indicated_distance_m` | m | Estimated distance to resource |
| `circuit_count` | — | Number of dance circuits |
| `return_phase_duration_ms` | ms | Duration of return phase |
| `abdomen_waggle_freq_hz` | Hz | Abdomen oscillation frequency |

### Canine Ethogram

Event types and dimension names for dog behavior observation:

**Event types**: `ethogram.play_bow`, `ethogram.tail_wag`, `ethogram.spin`, `ethogram.lip_licking`

| Dimension | Unit | Description |
|-----------|------|-------------|
| `tail_wag_freq_hz` | Hz | Tail wag frequency |
| `tail_height_deg` | deg | Tail angle above horizontal |
| `spin_rotations` | — | Number of full rotations |
| `spin_speed_rpm` | rpm | Spin angular velocity |

### Ethogram Extension Example

```json
{
    "id": "ucdavis.ethogram.0.1",
    "type": "ethogram.lip_licking",
    "agent": "ucdavis.dog.42",
    "start": "2024-03-15T14:22:01.000000",
    "content": "lip_licking",
    "duration_ms": 1200,
    "classification": {
        "method": "manual",
        "label": "lip_licking",
        "confidence": 0.95,
        "taxonomy": "evenson_ethogram_2019"
    }
}
```

### Video Annotation Extension

For linking events to video frame ranges via `other_data`:

```json
{
    "id": "mit.video.0.5",
    "type": "button_press",
    "agent": "mit.dog.7",
    "start": "2024-06-01T09:15:30.000000",
    "end": "2024-06-01T09:15:31.500000",
    "content": "outside",
    "source": {
        "type": "video",
        "sensor": "session_cam1",
        "fps": 30,
        "file": "session_2024-06-01_cam1.mp4",
        "format": "mp4"
    },
    "other_data": {
        "mit.video.frame_start": 13542,
        "mit.video.frame_end": 13587,
        "mit.video.bounding_box": [120, 340, 280, 510]
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
- Dimension vocabulary: (table of dimension names, units, descriptions)
- Agent properties introduced: (list)
- Other data fields introduced: (list)
- Classification taxonomies introduced: (list)

#### Example

```json
{
    "id": "example.0.1",
    "type": "namespace.domain.event_type",
    ...
}
```

#### Compatibility Notes

Any notes on interactions with existing core fields (measurements, source, classification, spatial) or other extensions.

---
