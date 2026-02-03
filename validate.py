#!/usr/bin/env python3
"""Validate an eventstream JSON file against the eventstream schema.

Usage: python validate.py path/to/file.json [path/to/another.json ...]

Uses the jsonschema library if available, otherwise falls back to manual validation.
"""

import json
import os
import sys

SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema", "eventstream.schema.json")


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Manual validator (fallback when jsonschema is not installed)
# ---------------------------------------------------------------------------

def _check_type(value, expected, path):
    """Return a list of error strings if *value* is not of *expected* type."""
    type_map = {
        "string": str,
        "number": (int, float),
        "integer": int,
        "boolean": bool,
        "object": dict,
        "array": list,
    }
    py_type = type_map.get(expected)
    if py_type and not isinstance(value, py_type):
        return [f"{path}: expected type '{expected}', got '{type(value).__name__}'"]
    return []


def _validate_object(data, schema, defs, path="$"):
    """Recursively validate *data* against *schema*. Returns list of errors."""
    errors = []

    # Resolve $ref
    ref = schema.get("$ref")
    if ref:
        ref_name = ref.split("/")[-1]
        schema = defs.get(ref_name, {})

    st = schema.get("type")
    if st:
        errors.extend(_check_type(data, st, path))
        if errors:
            return errors  # wrong type, no point going deeper

    if st == "object" and isinstance(data, dict):
        for req in schema.get("required", []):
            if req not in data:
                errors.append(f"{path}: missing required field '{req}'")
        props = schema.get("properties", {})
        for key, val in data.items():
            if key in props:
                errors.extend(_validate_object(val, props[key], defs, f"{path}.{key}"))

    if st == "array" and isinstance(data, list):
        items_schema = schema.get("items")
        if items_schema:
            for i, item in enumerate(data):
                errors.extend(_validate_object(item, items_schema, defs, f"{path}[{i}]"))

    if st == "number" or st == "integer":
        if "minimum" in schema and isinstance(data, (int, float)) and data < schema["minimum"]:
            errors.append(f"{path}: value {data} is less than minimum {schema['minimum']}")
        if "maximum" in schema and isinstance(data, (int, float)) and data > schema["maximum"]:
            errors.append(f"{path}: value {data} is greater than maximum {schema['maximum']}")

    return errors


def validate_manual(data, schema):
    defs = schema.get("$defs", {})
    return _validate_object(data, schema, defs)


# ---------------------------------------------------------------------------
# jsonschema-based validator
# ---------------------------------------------------------------------------

def validate_jsonschema(data, schema):
    from jsonschema import Draft202012Validator
    validator = Draft202012Validator(schema)
    errors = []
    for error in sorted(validator.iter_errors(data), key=lambda e: list(e.absolute_path)):
        path = ".".join(str(p) for p in error.absolute_path) or "$"
        errors.append(f"{path}: {error.message}")
    return errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def validate(filepath, schema):
    data = load_json(filepath)
    try:
        return validate_jsonschema(data, schema)
    except ImportError:
        return validate_manual(data, schema)


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <file.json> [file2.json ...]", file=sys.stderr)
        sys.exit(2)

    schema = load_json(SCHEMA_PATH)
    exit_code = 0

    for filepath in sys.argv[1:]:
        try:
            errors = validate(filepath, schema)
        except (json.JSONDecodeError, FileNotFoundError, IsADirectoryError) as exc:
            print(f"FAIL {filepath}: {exc}")
            exit_code = 1
            continue

        if errors:
            print(f"FAIL {filepath}:")
            for e in errors:
                print(f"  - {e}")
            exit_code = 1
        else:
            print(f"OK   {filepath}")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
