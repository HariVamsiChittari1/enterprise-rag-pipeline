from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from retrieval.synonyms import (
    MAX_EXPANSIONS_PER_TERM,
    MAX_TERMS_PER_QUERY,
    SynonymExpander,
    SynonymMap,
    SynonymMapError,
    SynonymRule,
)


def _map(*rules: str) -> SynonymMap:
    return SynonymMap.parse("test", list(rules))


def test_given_expander_when_map_reassigned_then_state_is_unchanged() -> None:
    expander = SynonymExpander(_map("leave, vacation"))
    before = expander.expand("leave")

    with pytest.raises(FrozenInstanceError):
        expander._map = _map("leave => absence")

    assert expander.expand("leave") == before


def test_equivalency_rule_expands_query_with_synonyms() -> None:
    expander = SynonymExpander(_map("dog, puppy, canine"))
    terms = expander.expand("Did the dog run away?")
    assert set(terms) == {
        "Did the dog run away?",
        "Did the puppy run away?",
        "Did the canine run away?",
    }


def test_explicit_mapping_produces_only_rhs_terms() -> None:
    expander = SynonymExpander(_map("Washington, Wash., WA => WA"))
    terms = expander.expand("Live in Washington")
    assert terms == ["Live in WA"]


def test_explicit_mapping_preserves_rhs_input() -> None:
    expander = SynonymExpander(_map("Washington, Wash., WA => WA"))
    assert expander.expand("Live in WA") == ["Live in WA"]


def test_case_insensitive_word_boundary_matching() -> None:
    expander = SynonymExpander(_map("dog, puppy, canine"))
    matched = expander.expand("The Dog barked")
    assert set(matched) == {"The Dog barked", "The puppy barked", "The canine barked"}

    unmatched = expander.expand("dogfood policy")
    assert unmatched == ["dogfood policy"]


def test_no_matching_rule_returns_original_query_only() -> None:
    expander = SynonymExpander(_map("dog, puppy"))
    assert expander.expand("cats meow") == ["cats meow"]


def test_expansions_are_capped_per_input_term() -> None:
    rule = ", ".join(["target"] + [f"syn{i}" for i in range(10)])
    expander = SynonymExpander(_map(rule))
    terms = expander.expand("target")
    # 1 (original) + MAX_EXPANSIONS_PER_TERM new synonyms.
    assert len(terms) == 1 + MAX_EXPANSIONS_PER_TERM


def test_total_terms_are_capped_across_multiple_rules() -> None:
    rules = [
        "alpha, one, two, three, four, five",
        "beta, six, seven, eight, nine",
    ]
    expander = SynonymExpander(_map(*rules))
    terms = expander.expand("alpha beta")
    assert len(terms) <= MAX_TERMS_PER_QUERY


def test_duplicate_expansions_are_deduplicated_case_insensitive() -> None:
    expander = SynonymExpander(_map("dog, DOG, Dog"))
    terms = expander.expand("dog")
    # Only "dog" (input) remains — all outputs are the same term case-insensitively.
    assert terms == ["dog"]


def test_empty_query_returns_empty_list() -> None:
    expander = SynonymExpander(_map("dog, puppy"))
    assert expander.expand("") == []
    assert expander.expand("   ") == []


def test_solr_parser_rejects_empty_rule() -> None:
    with pytest.raises(SynonymMapError):
        SynonymMap.parse("test", [""])


def test_solr_parser_rejects_equivalency_with_single_term() -> None:
    with pytest.raises(SynonymMapError, match="at least two"):
        SynonymMap.parse("test", ["lonely"])


def test_solr_parser_rejects_explicit_rule_missing_rhs() -> None:
    with pytest.raises(SynonymMapError):
        SynonymMap.parse("test", ["a, b => "])


def test_solr_parser_deduplicates_identical_rules() -> None:
    parsed = SynonymMap.parse("test", ["dog, puppy", "dog, puppy"])
    assert len(parsed.rules) == 1


def test_multi_word_phrase_matches_phrase_in_query() -> None:
    expander = SynonymExpander(_map("New York, NYC, Big Apple"))
    # Multi-word input term matches by word-boundary substring.
    matched = expander.expand("I love New York")
    assert "I love NYC" in matched
    assert "I love Big Apple" in matched


def test_parser_supports_escaped_comma_and_backslash() -> None:
    parsed = _map(r"WA\, USA, Washington", r"path\\name, pathname")
    assert parsed.rules[0].inputs == ("WA, USA", "Washington")
    assert parsed.rules[1].inputs == (r"path\name", "pathname")


def test_parser_unescapes_reserved_special_characters() -> None:
    parsed = _map(r"C\+\+, cpp", r"high\-priority, urgent")
    assert parsed.rules[0].inputs == ("C++", "cpp")
    assert parsed.rules[1].inputs == ("high-priority", "urgent")
    expander = SynonymExpander(parsed)
    assert expander.expand("C++ guide") == ["C++ guide", "cpp guide"]


def test_punctuation_ending_terms_match_without_word_boundary_bug() -> None:
    expander = SynonymExpander(_map("C++, cpp", "Wash., Washington"))
    assert expander.expand("C++ guide") == ["C++ guide", "cpp guide"]
    assert expander.expand("Policy in Wash.") == ["Policy in Wash.", "Policy in Washington"]


def test_parser_rejects_trailing_escape_and_multiple_arrows() -> None:
    with pytest.raises(SynonymMapError, match="trailing escape"):
        _map("broken\\")
    with pytest.raises(SynonymMapError, match="multiple"):
        _map("a => b => c")
