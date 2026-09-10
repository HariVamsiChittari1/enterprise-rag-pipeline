from __future__ import annotations

import pytest

from retrieval.config_loader import (
    ConfigLoaderError,
    load_scoring_profiles,
    load_synonym_maps,
    validate_profile_synonym_map_references,
)
from retrieval.synonyms import SynonymMap


def _valid_profile_payload() -> dict:
    return {
        "profiles": [
            {
                "name": "fresh",
                "textWeights": {"sourceName": 2.0},
                "functionAggregation": "sum",
                "functions": [
                    {
                        "type": "freshness",
                        "fieldName": "source_modified_at",
                        "boost": 5.0,
                        "interpolation": "linear",
                        "freshness": {"boostingDuration": "P30D"},
                    }
                ],
            }
        ]
    }


def _valid_synonym_payload() -> dict:
    return {
        "maps": [
            {
                "name": "geo",
                "format": "solr",
                "language": "en",
                "rules": [
                    "USA, United States, United States of America",
                    "Washington, Wash., WA => WA",
                ],
            }
        ]
    }


def test_valid_scoring_profile_config_loads() -> None:
    profiles = load_scoring_profiles(_valid_profile_payload())
    assert set(profiles.keys()) == {"fresh"}
    fresh = profiles["fresh"]
    assert fresh.text_weights == {"sourceName": 2.0}
    assert fresh.functions[0].type == "freshness"


def test_scoring_profile_loader_rejects_unknown_function_type() -> None:
    payload = _valid_profile_payload()
    payload["profiles"][0]["functions"][0]["type"] = "tag"
    with pytest.raises(ConfigLoaderError, match="invalid"):
        load_scoring_profiles(payload)


def test_scoring_profile_loader_rejects_bad_boosting_duration_pattern() -> None:
    payload = _valid_profile_payload()
    payload["profiles"][0]["functions"][0]["freshness"]["boostingDuration"] = "30 days"
    with pytest.raises(ConfigLoaderError, match="invalid"):
        load_scoring_profiles(payload)


def test_scoring_profile_loader_rejects_unknown_keys_fail_closed() -> None:
    payload = _valid_profile_payload()
    payload["profiles"][0]["unexpected"] = True
    with pytest.raises(ConfigLoaderError, match="invalid"):
        load_scoring_profiles(payload)


def test_scoring_profile_loader_rejects_unsupported_signal_and_nonfinite_weight() -> None:
    payload = _valid_profile_payload()
    payload["profiles"][0]["textWeights"] = {"authorityLevel": 1.0}
    with pytest.raises(ConfigLoaderError, match="invalid"):
        load_scoring_profiles(payload)

    payload = _valid_profile_payload()
    payload["profiles"][0]["textWeights"] = {"content": float("nan")}
    with pytest.raises(ConfigLoaderError, match="rejected"):
        load_scoring_profiles(payload)


def test_scoring_profile_loader_rejects_duplicate_profile_names() -> None:
    payload = _valid_profile_payload()
    payload["profiles"].append(dict(payload["profiles"][0]))
    with pytest.raises(ConfigLoaderError, match="duplicate"):
        load_scoring_profiles(payload)


def test_synonym_loader_accepts_valid_solr_payload() -> None:
    assert set(load_synonym_maps(_valid_synonym_payload()).keys()) == {"geo"}


def test_synonym_loader_rejects_wrong_format() -> None:
    payload = _valid_synonym_payload()
    payload["maps"][0]["format"] = "custom"
    with pytest.raises(ConfigLoaderError, match="invalid"):
        load_synonym_maps(payload)


def test_magnitude_scoring_profile_is_rejected() -> None:
    payload = _valid_profile_payload()
    payload["profiles"][0]["functions"][0] = {
        "type": "magnitude",
        "fieldName": "tokenCount",
        "boost": 3.0,
        "interpolation": "linear",
        "magnitude": {"boostingRangeStart": 100, "boostingRangeEnd": 800},
    }
    with pytest.raises(ConfigLoaderError, match="invalid"):
        load_scoring_profiles(payload)


def test_scoring_profile_accepts_synonym_map_reference_field() -> None:
    payload = _valid_profile_payload()
    payload["profiles"][0]["synonymMap"] = "geo"
    profiles = load_scoring_profiles(payload)
    assert profiles["fresh"].synonym_map == "geo"


def test_synonym_loader_returns_parsed_synonym_map_objects() -> None:
    geo = load_synonym_maps(_valid_synonym_payload())["geo"]
    assert isinstance(geo, SynonymMap)
    assert len(geo.rules) == 2


def test_cross_ref_validation_passes_when_profile_map_is_loaded() -> None:
    payload = _valid_profile_payload()
    payload["profiles"][0]["synonymMap"] = "geo"
    profiles = load_scoring_profiles(payload)
    maps = load_synonym_maps(_valid_synonym_payload())
    validate_profile_synonym_map_references(profiles, maps)


def test_cross_ref_validation_fails_closed_on_missing_synonym_map() -> None:
    payload = _valid_profile_payload()
    payload["profiles"][0]["synonymMap"] = "nonexistent"
    profiles = load_scoring_profiles(payload)
    maps = load_synonym_maps(_valid_synonym_payload())
    with pytest.raises(ConfigLoaderError, match="nonexistent"):
        validate_profile_synonym_map_references(profiles, maps)


@pytest.mark.parametrize("aggregation", ["sum", "average", "minimum", "maximum"])
def test_scoring_profile_loader_accepts_every_current_aggregation(aggregation: str) -> None:
    payload = _valid_profile_payload()
    payload["profiles"][0]["functionAggregation"] = aggregation
    profiles = load_scoring_profiles(payload)
    assert profiles["fresh"].function_aggregation == aggregation


def test_scoring_profile_loader_rejects_unsupported_max_aggregation() -> None:
    payload = _valid_profile_payload()
    payload["profiles"][0]["functionAggregation"] = "max"
    with pytest.raises(ConfigLoaderError, match="invalid"):
        load_scoring_profiles(payload)


def test_scoring_profile_loader_rejects_more_than_hundred_profiles() -> None:
    payload = _valid_profile_payload()
    template = payload["profiles"][0]
    payload["profiles"] = [dict(template, name=f"p{i}") for i in range(101)]
    with pytest.raises(ConfigLoaderError, match="invalid"):
        load_scoring_profiles(payload)


def test_scoring_profile_loader_rejects_more_than_eight_functions_per_profile() -> None:
    payload = _valid_profile_payload()
    template = payload["profiles"][0]["functions"][0]
    payload["profiles"][0]["functions"] = [dict(template) for _ in range(9)]
    with pytest.raises(ConfigLoaderError, match="invalid"):
        load_scoring_profiles(payload)


def test_scoring_profile_loader_rejects_freshness_type_missing_freshness_block() -> None:
    payload = _valid_profile_payload()
    del payload["profiles"][0]["functions"][0]["freshness"]
    with pytest.raises(ConfigLoaderError, match="invalid"):
        load_scoring_profiles(payload)


def test_scoring_profile_loader_rejects_freshness_type_with_irrelevant_magnitude_block() -> None:
    payload = _valid_profile_payload()
    payload["profiles"][0]["functions"][0]["magnitude"] = {
        "boostingRangeStart": 1,
        "boostingRangeEnd": 10,
    }
    with pytest.raises(ConfigLoaderError, match="invalid"):
        load_scoring_profiles(payload)


def test_scoring_profile_loader_rejects_boolean_boost() -> None:
    payload = _valid_profile_payload()
    payload["profiles"][0]["functions"][0]["boost"] = True
    with pytest.raises(ConfigLoaderError, match="invalid"):
        load_scoring_profiles(payload)


def test_scoring_profile_loader_rejects_empty_synonym_map_reference() -> None:
    payload = _valid_profile_payload()
    payload["profiles"][0]["synonymMap"] = ""
    with pytest.raises(ConfigLoaderError, match="invalid"):
        load_scoring_profiles(payload)