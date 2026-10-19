from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from retrieval.scoring import (
    FreshnessParameters,
    ScoringFunction,
    ScoringProfile,
    ScoringProfileError,
    ScoringProfileReranker,
    parse_boosting_duration,
)


def _fixed_now() -> datetime:
    return datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_parse_boosting_duration_days_hours_minutes_seconds() -> None:
    assert parse_boosting_duration("P1D") == 86400
    assert parse_boosting_duration("PT1H") == 3600
    assert parse_boosting_duration("PT30M") == 1800
    assert parse_boosting_duration("PT1.5S") == 1.5
    assert parse_boosting_duration("P2DT12H") == 2 * 86400 + 12 * 3600


def test_parse_boosting_duration_rejects_zero_or_malformed() -> None:
    with pytest.raises(ScoringProfileError):
        parse_boosting_duration("P")
    with pytest.raises(ScoringProfileError):
        parse_boosting_duration("banana")


def _profile_with_freshness(interpolation: str, boost: float, days: int) -> ScoringProfile:
    return ScoringProfile(
        name="p",
        functions=(
            ScoringFunction(
                type="freshness",
                field_name="source_modified_at",
                boost=boost,
                interpolation=interpolation,
                freshness=FreshnessParameters(boosting_duration_seconds=days * 86400),
            ),
        ),
    )


@pytest.mark.parametrize("interpolation,elapsed_days,expected_factor", [
    ("constant", 0, 1.0),
    ("constant", 100, 1.0),
    ("constant", 365, 0.0),
    ("linear", 0, 1.0),
    ("linear", 182, pytest.approx(0.5, abs=0.02)),
    ("linear", 365, 0.0),
    ("quadratic", 0, 1.0),
    ("quadratic", 182, pytest.approx(0.75, abs=0.02)),
    ("quadratic", 365, 0.0),
])
def test_freshness_interpolation_curves_hit_boundaries(interpolation, elapsed_days, expected_factor) -> None:
    now = _fixed_now()
    candidate = {"sourceModifiedAt": (now - timedelta(days=elapsed_days)).isoformat().replace("+00:00", "Z")}
    reranker = ScoringProfileReranker(_profile_with_freshness(interpolation, boost=1.0, days=365), now=now)
    scored = reranker.rerank([candidate, {"sourceModifiedAt": None}])
    # Contribution above the base rank score is exactly the freshness boost * factor.
    base_score = 1.0 / 1  # rank 0
    stale_base_score = 1.0 / 2
    fresh_score = scored[0]["_scoring_profile_score"] if scored[0] is scored[0] else 0.0
    del fresh_score
    # Score of the fresh candidate = base + boost*factor.
    fresh_score = next(item for item in scored if item.get("sourceModifiedAt"))["_scoring_profile_score"]
    factor = fresh_score - base_score  # boost=1 → factor
    if isinstance(expected_factor, float):
        assert factor == pytest.approx(expected_factor, abs=1e-6)
    else:
        assert factor == expected_factor
    stale_score = next(item for item in scored if item.get("sourceModifiedAt") is None)["_scoring_profile_score"]
    assert stale_score == pytest.approx(stale_base_score, abs=1e-6)


def test_missing_freshness_contributes_zero_and_never_raises() -> None:
    now = _fixed_now()
    reranker = ScoringProfileReranker(_profile_with_freshness("linear", boost=5.0, days=30), now=now)
    scored = reranker.rerank([
        {"id": "a"},
        {"id": "b", "sourceModifiedAt": ""},
        {"id": "c", "sourceModifiedAt": "not-a-timestamp"},
    ])
    assert all(item["_scoring_profile_score"] > 0 for item in scored)
    # Rank order preserved when all freshness contributions are equal (0).
    assert [item["id"] for item in scored] == ["a", "b", "c"]


def test_freshness_does_not_fall_back_or_boost_future_dates() -> None:
    now = _fixed_now()
    reranker = ScoringProfileReranker(
        _profile_with_freshness("linear", boost=5.0, days=30), now=now,
    )
    scored = reranker.rerank([
        {"id": "missing", "createdAt": now.isoformat().replace("+00:00", "Z")},
        {
            "id": "future",
            "sourceModifiedAt": (now + timedelta(days=1)).isoformat().replace("+00:00", "Z"),
        },
    ])

    assert scored[0]["_scoring_profile_score"] == pytest.approx(1.0)
    assert scored[1]["_scoring_profile_score"] == pytest.approx(0.5)


def test_text_weight_boost_requires_query_match_in_projected_fields() -> None:
    profile = ScoringProfile(
        name="w",
        text_weights={"source_name": 2.0, "sectionPath": 1.5, "keyPhrases": 1.0},
    )
    reranker = ScoringProfileReranker(profile, query_terms=["leave policy"], now=_fixed_now())
    scored = reranker.rerank([
        {"id": "irrelevant", "sourceName": "other.pdf", "sectionPath": ["Header"]},
        {"id": "matched", "sourceName": "leave-policy.pdf", "sectionPath": ["Header"]},
    ])
    ordered = [item["id"] for item in scored]
    assert ordered == ["matched", "irrelevant"]


def test_text_weight_without_query_terms_preserves_original_order() -> None:
    profile = ScoringProfile(name="w", text_weights={"sourceName": 5.0})
    scored = ScoringProfileReranker(profile).rerank([
        {"id": "first", "sourceName": "leave.pdf"},
        {"id": "second", "sourceName": "pension.pdf"},
    ])
    assert [item["id"] for item in scored] == ["first", "second"]


def test_function_aggregation_supports_sum_average_minimum_maximum() -> None:
    now = _fixed_now()
    fresh_function = ScoringFunction(
        type="freshness", field_name="source_modified_at", boost=1.0,
        interpolation="linear",
        freshness=FreshnessParameters(boosting_duration_seconds=365 * 86400),
    )
    stale_function = ScoringFunction(
        type="freshness", field_name="source_modified_at", boost=1.0,
        interpolation="linear",
        freshness=FreshnessParameters(boosting_duration_seconds=365 * 86400),
    )
    candidate = {"id": "x", "sourceModifiedAt": now.isoformat().replace("+00:00", "Z")}
    base = 1.0  # single candidate, original_rank=0 → 1/(0+1)

    def score(agg: str) -> float:
        profile = ScoringProfile(
            name=agg, functions=(fresh_function, stale_function), function_aggregation=agg,
        )
        return ScoringProfileReranker(profile, now=now).rerank([candidate])[0]["_scoring_profile_score"]

    # Both freshness contributions equal 1.0 for a candidate at `now`.
    assert score("sum") == pytest.approx(base + 2.0)
    assert score("average") == pytest.approx(base + 1.0)
    assert score("minimum") == pytest.approx(base + 1.0)
    assert score("maximum") == pytest.approx(base + 1.0)


def test_function_aggregation_average_uses_declared_function_count() -> None:
    now = _fixed_now()
    # Two declared freshness functions; only one has a matching candidate field value.
    active = ScoringFunction(
        type="freshness", field_name="source_modified_at", boost=2.0,
        interpolation="constant",
        freshness=FreshnessParameters(boosting_duration_seconds=86400),
    )
    inactive = ScoringFunction(
        type="freshness", field_name="source_modified_at", boost=2.0,
        interpolation="constant",
        freshness=FreshnessParameters(boosting_duration_seconds=86400),
    )
    profile = ScoringProfile(
        name="avg", functions=(active, inactive), function_aggregation="average",
    )
    # Candidate has no timestamp so both contributions are 0.0; both count toward the mean.
    score = ScoringProfileReranker(profile, now=now).rerank([{"id": "x"}])[0]["_scoring_profile_score"]
    assert score == pytest.approx(1.0)  # base + 0.0/2 == 1.0


def test_function_aggregation_minimum_includes_negative_boost() -> None:
    now = _fixed_now()
    # A negative boost freshness (penalty) is legal and must participate in `minimum`.
    penalty = ScoringFunction(
        type="freshness", field_name="source_modified_at", boost=-3.0,
        interpolation="constant",
        freshness=FreshnessParameters(boosting_duration_seconds=86400),
    )
    reward = ScoringFunction(
        type="freshness", field_name="source_modified_at", boost=1.0,
        interpolation="constant",
        freshness=FreshnessParameters(boosting_duration_seconds=86400),
    )
    candidate = {"id": "x", "sourceModifiedAt": now.isoformat().replace("+00:00", "Z")}
    minimum_profile = ScoringProfile(
        name="min", functions=(reward, penalty), function_aggregation="minimum",
    )
    maximum_profile = ScoringProfile(
        name="max", functions=(reward, penalty), function_aggregation="maximum",
    )
    min_score = ScoringProfileReranker(minimum_profile, now=now).rerank([candidate])[0]["_scoring_profile_score"]
    max_score = ScoringProfileReranker(maximum_profile, now=now).rerank([candidate])[0]["_scoring_profile_score"]
    assert min_score == pytest.approx(1.0 + (-3.0))
    assert max_score == pytest.approx(1.0 + 1.0)


def test_function_aggregation_empty_functions_contribute_zero_for_all_supported() -> None:
    for agg in ("sum", "average", "minimum", "maximum"):
        profile = ScoringProfile(name=agg, function_aggregation=agg)
        score = ScoringProfileReranker(profile).rerank([{"id": "x"}])[0]["_scoring_profile_score"]
        assert score == pytest.approx(1.0)


def test_scoring_profile_rejects_invalid_interpolation_and_aggregation() -> None:
    with pytest.raises(ScoringProfileError):
        ScoringProfile(name="x", function_aggregation="product")
    with pytest.raises(ScoringProfileError):
        ScoringFunction(
            type="freshness", field_name="ts", boost=1.0, interpolation="ninja",
            freshness=FreshnessParameters(boosting_duration_seconds=1),
        )


def test_scoring_function_rejects_unsupported_type_in_this_phase() -> None:
    """Only freshness functions are part of the active retrieval contract."""
    with pytest.raises(ScoringProfileError):
        ScoringFunction(type="tag", field_name="tags", boost=1.0, interpolation="linear")


def test_scoring_function_rejects_magnitude() -> None:
    with pytest.raises(ScoringProfileError, match="not supported"):
        ScoringFunction(
            type="magnitude", field_name="tokenCount", boost=1.0,
            interpolation="linear",
        )


# --- Direct-constructor invariants and vocabulary reset ------------------------


def test_scoring_profile_rejects_legacy_max_aggregation() -> None:
    with pytest.raises(ScoringProfileError, match="functionAggregation"):
        ScoringProfile(name="p", function_aggregation="max")


@pytest.mark.parametrize("agg", ["sum", "average", "minimum", "maximum"])
def test_scoring_profile_accepts_every_current_aggregation(agg: str) -> None:
    profile = ScoringProfile(name=f"p-{agg}", function_aggregation=agg)
    assert profile.function_aggregation == agg


def test_scoring_profile_rejects_duplicate_canonical_text_weights() -> None:
    with pytest.raises(ScoringProfileError, match="duplicate"):
        ScoringProfile(
            name="dup",
            text_weights={"sourceName": 1.0, "source_name": 2.0},
        )


def test_scoring_profile_rejects_boolean_text_weight() -> None:
    with pytest.raises(ScoringProfileError, match="non-negative"):
        ScoringProfile(name="b", text_weights={"sourceName": True})  # type: ignore[dict-item]


def test_scoring_profile_rejects_non_finite_text_weight() -> None:
    with pytest.raises(ScoringProfileError, match="non-negative"):
        ScoringProfile(name="nan", text_weights={"sourceName": float("nan")})


def test_scoring_profile_freezes_text_weights_after_construction() -> None:
    original = {"sourceName": 1.0}
    profile = ScoringProfile(name="frozen", text_weights=original)
    # Mutating the input dict does not change the profile.
    original["sourceName"] = 99.0
    assert profile.text_weights["sourceName"] == 1.0
    # The internal mapping is read-only.
    with pytest.raises(TypeError):
        profile.text_weights["sourceName"] = 5.0  # type: ignore[index]


def test_scoring_profile_rejects_more_than_eight_functions() -> None:
    function = ScoringFunction(
        type="freshness", field_name="source_modified_at", boost=0.1,
        interpolation="linear",
        freshness=FreshnessParameters(boosting_duration_seconds=86400),
    )
    with pytest.raises(ScoringProfileError, match="8 functions"):
        ScoringProfile(name="too-many", functions=(function,) * 9)


def test_scoring_profile_rejects_infinite_envelope() -> None:
    with pytest.raises(ScoringProfileError, match="envelope"):
        ScoringProfile(name="huge", text_weights={"sourceName": 1e308, "content": 1e308})


def test_scoring_function_rejects_boolean_boost() -> None:
    with pytest.raises(ScoringProfileError, match="boost"):
        ScoringFunction(
            type="freshness", field_name="source_modified_at", boost=True,  # type: ignore[arg-type]
            interpolation="linear",
            freshness=FreshnessParameters(boosting_duration_seconds=86400),
        )


def test_scoring_function_rejects_infinite_boost() -> None:
    with pytest.raises(ScoringProfileError, match="boost"):
        ScoringFunction(
            type="freshness", field_name="source_modified_at", boost=float("inf"),
            interpolation="linear",
            freshness=FreshnessParameters(boosting_duration_seconds=86400),
        )


# --- Strict invariants -----------------------------------------------------------


@pytest.mark.parametrize("bad", [True, False, float("nan"), float("inf"), -1.0, 0.0])
def test_freshness_parameters_reject_non_finite_zero_negative_and_boolean(bad) -> None:
    with pytest.raises(ScoringProfileError, match="boostingDuration"):
        FreshnessParameters(boosting_duration_seconds=bad)  # type: ignore[arg-type]


def test_scoring_profile_rejects_non_scoring_function_in_tuple() -> None:
    with pytest.raises(ScoringProfileError, match="ScoringFunction"):
        ScoringProfile(name="bad", functions=("not-a-function",))  # type: ignore[arg-type]


def test_scoring_profile_rejects_empty_or_non_string_synonym_map() -> None:
    with pytest.raises(ScoringProfileError, match="synonymMap"):
        ScoringProfile(name="p", synonym_map="   ")
    with pytest.raises(ScoringProfileError, match="synonymMap"):
        ScoringProfile(name="p", synonym_map=123)  # type: ignore[arg-type]


def test_reranker_rejects_naive_now() -> None:
    profile = ScoringProfile(name="p")
    with pytest.raises(ScoringProfileError, match="timezone-aware"):
        ScoringProfileReranker(profile, now=datetime(2026, 1, 1))


def test_freshness_treats_naive_candidate_timestamp_as_zero_signal() -> None:
    now = _fixed_now()
    reranker = ScoringProfileReranker(
        _profile_with_freshness("linear", boost=5.0, days=30), now=now,
    )
    # Naive ISO timestamp must not raise TypeError under the timezone-aware clock;
    # per plan, malformed candidate signals contribute exactly zero.
    scored = reranker.rerank([
        {"id": "naive", "sourceModifiedAt": "2026-01-01T00:00:00"},
        {"id": "aware", "sourceModifiedAt": now.isoformat().replace("+00:00", "Z")},
    ])
    naive_score = next(item for item in scored if item["id"] == "naive")["_scoring_profile_score"]
    aware_score = next(item for item in scored if item["id"] == "aware")["_scoring_profile_score"]
    # Naive at rank 0 keeps only base rank score.
    assert naive_score == pytest.approx(1.0)
    # Aware at rank 1 gets base 0.5 plus full boost (5.0) at elapsed=0.
    assert aware_score == pytest.approx(0.5 + 5.0)


def test_average_aggregation_uses_declared_count_with_mixed_contributions() -> None:
    now = _fixed_now()
    active_full = ScoringFunction(
        type="freshness", field_name="source_modified_at", boost=4.0,
        interpolation="constant",
        freshness=FreshnessParameters(boosting_duration_seconds=86400),
    )
    active_penalty = ScoringFunction(
        type="freshness", field_name="source_modified_at", boost=-2.0,
        interpolation="constant",
        freshness=FreshnessParameters(boosting_duration_seconds=86400),
    )
    profile = ScoringProfile(
        name="avg-mixed",
        functions=(active_full, active_penalty),
        function_aggregation="average",
    )
    candidate = {"id": "x", "sourceModifiedAt": now.isoformat().replace("+00:00", "Z")}
    # Mean of (+4.0, -2.0) divided by declared count 2 == 1.0; base is 1.0.
    score = ScoringProfileReranker(profile, now=now).rerank([candidate])[0]["_scoring_profile_score"]
    assert score == pytest.approx(1.0 + 1.0)
