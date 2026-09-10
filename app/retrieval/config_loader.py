"""Fail-closed loaders and schemas for scoring profiles and synonym maps."""

from __future__ import annotations

from typing import Any, Mapping

from jsonschema import Draft202012Validator, ValidationError

from retrieval.scoring import (
    FreshnessParameters,
    ScoringFunction,
    ScoringProfile,
    ScoringProfileError,
    parse_boosting_duration,
)
from retrieval.synonyms import SynonymMap, SynonymMapError


class ConfigLoaderError(RuntimeError):
    """Raised for any fail-closed config load failure."""


SCORING_PROFILES_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["profiles"],
    "properties": {
        "profiles": {
            "type": "array",
            "maxItems": 100,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name"],
                "properties": {
                    "name": {"type": "string", "minLength": 1},
                    "textWeights": {
                        "type": "object",
                        "propertyNames": {
                            "enum": [
                                "content", "sourceName", "source_name", "sectionPath",
                                "section_path", "keyPhrases", "key_phrases",
                            ]
                        },
                        "additionalProperties": {"type": "number", "minimum": 0},
                    },
                    "functionAggregation": {
                        "enum": ["sum", "average", "minimum", "maximum"]
                    },
                    "synonymMap": {"type": "string", "minLength": 1},
                    "functions": {
                        "type": "array",
                        "maxItems": 8,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["type", "fieldName", "boost", "interpolation", "freshness"],
                            "properties": {
                                "type": {"const": "freshness"},
                                "fieldName": {
                                    "enum": [
                                        "sourceModifiedAt", "source_modified_at",
                                    ]
                                },
                                "boost": {"type": "number"},
                                "interpolation": {
                                    "enum": ["constant", "linear", "quadratic", "logarithmic"]
                                },
                                "freshness": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "required": ["boostingDuration"],
                                    "properties": {
                                        "boostingDuration": {
                                            "type": "string",
                                            "pattern": r"^P(?:\d+D)?(?:T(?:\d+H)?(?:\d+M)?(?:\d+(?:\.\d+)?S)?)?$",
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        }
    },
}


SYNONYM_MAPS_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["maps"],
    "properties": {
        "maps": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "format", "rules"],
                "properties": {
                    "name": {"type": "string", "minLength": 1},
                    "format": {"enum": ["solr"]},
                    "language": {"type": "string", "minLength": 1},
                    "rules": {
                        "type": "array",
                        "items": {"type": "string", "minLength": 1},
                        "maxItems": 20_000,
                    },
                },
            },
        }
    },
}


def load_scoring_profiles(payload: Mapping[str, Any]) -> dict[str, ScoringProfile]:
    try:
        Draft202012Validator(SCORING_PROFILES_SCHEMA).validate(payload)
    except ValidationError as error:
        raise ConfigLoaderError(
            f"scoring profiles config is invalid: {error.message}"
        ) from None
    return _build_profiles(payload["profiles"])


def load_synonym_maps(payload: Mapping[str, Any]) -> dict[str, SynonymMap]:
    try:
        Draft202012Validator(SYNONYM_MAPS_SCHEMA).validate(payload)
    except ValidationError as error:
        raise ConfigLoaderError(
            f"synonym maps config is invalid: {error.message}"
        ) from None
    built: dict[str, SynonymMap] = {}
    for entry in payload["maps"]:
        name = entry["name"]
        if name in built:
            raise ConfigLoaderError(f"duplicate synonym map name: {name}")
        try:
            built[name] = SynonymMap.parse(name, entry["rules"])
        except SynonymMapError as error:
            raise ConfigLoaderError(
                f"synonym map '{name}' rejected: {error}"
            ) from None
    return built


def validate_profile_synonym_map_references(
    profiles: dict[str, ScoringProfile],
    synonym_maps: dict[str, SynonymMap],
) -> None:
    """Fail closed if any profile references a synonym map that was not loaded."""
    for profile in profiles.values():
        if profile.synonym_map is None:
            continue
        if profile.synonym_map not in synonym_maps:
            raise ConfigLoaderError(
                f"scoring profile '{profile.name}' references unknown synonym map '{profile.synonym_map}'"
            )


def _build_profiles(entries: list[dict[str, Any]]) -> dict[str, ScoringProfile]:
    built: dict[str, ScoringProfile] = {}
    for entry in entries:
        name = entry["name"]
        if name in built:
            raise ConfigLoaderError(f"duplicate scoring profile name: {name}")
        try:
            functions = tuple(_build_function(function) for function in entry.get("functions", []))
            built[name] = ScoringProfile(
                name=name,
                text_weights=dict(entry.get("textWeights", {})),
                functions=functions,
                function_aggregation=entry.get("functionAggregation", "sum"),
                synonym_map=entry.get("synonymMap"),
            )
        except ScoringProfileError as error:
            raise ConfigLoaderError(
                f"scoring profile '{name}' rejected: {error}"
            ) from None
    return built


def _build_function(payload: dict[str, Any]) -> ScoringFunction:
    freshness_params: FreshnessParameters | None = None
    if payload["type"] == "freshness":
        freshness_payload = payload.get("freshness")
        if not isinstance(freshness_payload, dict):
            raise ScoringProfileError("freshness function requires 'freshness' parameters")
        seconds = parse_boosting_duration(freshness_payload["boostingDuration"])
        freshness_params = FreshnessParameters(boosting_duration_seconds=seconds)
    return ScoringFunction(
        type=payload["type"],
        field_name=payload["fieldName"],
        boost=float(payload["boost"]),
        interpolation=payload["interpolation"],
        freshness=freshness_params,
    )
