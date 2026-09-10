"""Solr-format synonym expansion for the Cosmos DB retrieval pipeline.

Applied at query rewrite time as bound `@paramN` parameter values to multiple
FullTextScore calls fused via RRF (SDK-verified pattern). Never emits term
values into raw SQL text.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

# Caps enforce BM25 additivity guardrails; see plan Section 3, review R5.
MAX_EXPANSIONS_PER_TERM = 5
MAX_TERMS_PER_QUERY = 8


class SynonymMapError(ValueError):
    """Raised for invalid synonym map configuration."""


@dataclass(frozen=True)
class SynonymRule:
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    explicit: bool


@dataclass(frozen=True)
class SynonymMap:
    name: str
    rules: tuple[SynonymRule, ...]

    @classmethod
    def parse(cls, name: str, raw_rules: Iterable[str]) -> "SynonymMap":
        rules: list[SynonymRule] = []
        seen_ruleset: set[tuple[tuple[str, ...], tuple[str, ...], bool]] = set()
        for raw in raw_rules:
            rule = _parse_rule(raw)
            key = (rule.inputs, rule.outputs, rule.explicit)
            if key in seen_ruleset:
                continue
            seen_ruleset.add(key)
            rules.append(rule)
        return cls(name=name, rules=tuple(rules))


@dataclass(frozen=True, init=False)
class SynonymExpander:
    """Expand a free-text query into a bounded set of parameterizable terms."""

    _map: SynonymMap

    def __init__(self, synonym_map: SynonymMap) -> None:
        object.__setattr__(self, "_map", synonym_map)

    @property
    def map_name(self) -> str:
        return self._map.name

    def expand(self, query: str) -> list[str]:
        original = query.strip()
        if not original:
            return []
        variants: list[str] = [original]
        for rule in self._map.rules:
            rewritten: list[str] = []
            for variant in variants:
                matched_inputs = [token for token in rule.inputs if _contains_word(variant, token)]
                if not matched_inputs:
                    rewritten.append(variant)
                    continue
                if not rule.explicit:
                    rewritten.append(variant)
                for token in matched_inputs:
                    additions = 0
                    for output in rule.outputs:
                        if additions >= MAX_EXPANSIONS_PER_TERM:
                            break
                        if not rule.explicit and output.casefold() == token.casefold():
                            continue
                        rewritten.append(_replace_term(variant, token, output))
                        additions += 1
            variants = _deduplicate(rewritten)[:MAX_TERMS_PER_QUERY]
            if len(variants) >= MAX_TERMS_PER_QUERY:
                break
        return variants


def _parse_rule(raw: str) -> SynonymRule:
    stripped = raw.strip()
    if not stripped:
        raise SynonymMapError("synonym rule is empty")
    if stripped.count("=>") > 1:
        raise SynonymMapError(f"explicit rule '{raw}' contains multiple mappings")
    if "=>" in stripped:
        lhs, rhs = stripped.split("=>", 1)
        inputs = _split_terms(lhs)
        outputs = _split_terms(rhs)
        if not inputs or not outputs:
            raise SynonymMapError(f"explicit rule '{raw}' is malformed")
        return SynonymRule(inputs=inputs, outputs=outputs, explicit=True)
    inputs = _split_terms(stripped)
    if len(inputs) < 2:
        raise SynonymMapError(f"equivalency rule '{raw}' requires at least two terms")
    return SynonymRule(inputs=inputs, outputs=inputs, explicit=False)


def _split_terms(chunk: str) -> tuple[str, ...]:
    terms: list[str] = []
    current: list[str] = []
    escaped = False
    for character in chunk:
        if escaped:
            current.append(character)
            escaped = False
        elif character == "\\":
            escaped = True
        elif character == ",":
            value = "".join(current).strip()
            if value:
                terms.append(value)
            current = []
        else:
            current.append(character)
    if escaped:
        raise SynonymMapError("synonym term has a trailing escape")
    value = "".join(current).strip()
    if value:
        terms.append(value)
    return tuple(terms)


def _contains_word(query: str, token: str) -> bool:
    if not token:
        return False
    return _term_pattern(token).search(query) is not None


def _replace_term(query: str, token: str, replacement: str) -> str:
    return _term_pattern(token).sub(replacement, query)


def _term_pattern(token: str) -> re.Pattern[str]:
    return re.compile(rf"(?<!\w){re.escape(token)}(?!\w)", re.IGNORECASE)


def _deduplicate(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.casefold()
        if key not in seen:
            seen.add(key)
            result.append(value)
    return result
