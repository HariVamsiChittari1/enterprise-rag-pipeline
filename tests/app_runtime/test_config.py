from __future__ import annotations

import pytest

from config import ExtractionProvider, load_config


def _set_required_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {
        "EXTRACTION_ENABLED": "false",
        "DOCUMENT_INTELLIGENCE_ENABLED": "true",
        "CONTENT_UNDERSTANDING_ENABLED": "false",
        "SUMMARY_ENABLED": "false",
        "KEY_PHRASES_ENABLED": "false",
        "ENTITIES_ENABLED": "false",
        "INGESTION_SOURCE_ID": "source",
        "SHAREPOINT_ASSIGNED_DRIVE_ID": "drive",
        "SHAREPOINT_TENANT_ID": "tenant",
        "SHAREPOINT_APP_CLIENT_ID": "client",
        "KEY_VAULT_URI": "https://vault.example",
        "COSMOS_ENDPOINT": "https://cosmos.example",
        "COSMOS_DATABASE_NAME": "database",
        "OPENAI_ENDPOINT": "https://openai.example",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def test_load_config_requires_sharepoint_site_url(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.delenv("SHAREPOINT_SITE_URL", raising=False)

    with pytest.raises(EnvironmentError, match="SHAREPOINT_SITE_URL"):
        load_config()


def test_load_config_reads_required_sharepoint_site_url(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("SHAREPOINT_SITE_URL", " https://tenant.sharepoint.com/sites/site ")

    config = load_config()

    assert config.sharepoint_site_url == "https://tenant.sharepoint.com/sites/site"


def test_load_config_requires_vision_deployment_when_extraction_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("EXTRACTION_ENABLED", "true")
    monkeypatch.setenv("DOCUMENT_INTELLIGENCE_ENDPOINT", "https://di.example")
    monkeypatch.setenv("CONTENT_UNDERSTANDING_ENDPOINT", "https://cu.example")
    monkeypatch.setenv("CONTENT_UNDERSTANDING_ANALYZER_ID", "rag-document-search-v1")
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    monkeypatch.delenv("OPENAI_CHAT_DEPLOYMENT_NAME", raising=False)

    with pytest.raises(EnvironmentError, match="OPENAI_CHAT_DEPLOYMENT_NAME"):
        load_config()


def test_load_config_treats_content_understanding_endpoint_as_optional(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("EXTRACTION_ENABLED", "true")
    monkeypatch.setenv("DOCUMENT_INTELLIGENCE_ENDPOINT", "https://di.example")
    monkeypatch.setenv("OPENAI_CHAT_DEPLOYMENT_NAME", "gpt-5.4")
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    monkeypatch.delenv("CONTENT_UNDERSTANDING_ENDPOINT", raising=False)

    config = load_config()

    assert config.content_understanding_endpoint == ""


def test_load_config_treats_content_understanding_analyzer_as_optional(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("EXTRACTION_ENABLED", "true")
    monkeypatch.setenv("DOCUMENT_INTELLIGENCE_ENDPOINT", "https://di.example")
    monkeypatch.setenv("CONTENT_UNDERSTANDING_ENDPOINT", "https://cu.example")
    monkeypatch.setenv("OPENAI_CHAT_DEPLOYMENT_NAME", "gpt-5.4")
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    monkeypatch.delenv("CONTENT_UNDERSTANDING_ANALYZER_ID", raising=False)

    config = load_config()

    assert config.content_understanding_analyzer_id == ""


@pytest.mark.parametrize(
    ("content_understanding_enabled", "document_intelligence_enabled", "expected"),
    [
        (True, False, ExtractionProvider.CONTENT_UNDERSTANDING),
        (True, True, ExtractionProvider.CONTENT_UNDERSTANDING),
        (False, True, ExtractionProvider.DOCUMENT_INTELLIGENCE),
    ],
)
def test_load_config_selects_one_extraction_provider(
    monkeypatch: pytest.MonkeyPatch,
    content_understanding_enabled: bool,
    document_intelligence_enabled: bool,
    expected: ExtractionProvider,
) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("EXTRACTION_ENABLED", "true")
    monkeypatch.setenv(
        "CONTENT_UNDERSTANDING_ENABLED",
        str(content_understanding_enabled).lower(),
    )
    monkeypatch.setenv(
        "DOCUMENT_INTELLIGENCE_ENABLED",
        str(document_intelligence_enabled).lower(),
    )
    monkeypatch.setenv("DOCUMENT_INTELLIGENCE_ENDPOINT", "https://di.example")
    monkeypatch.setenv("CONTENT_UNDERSTANDING_ENDPOINT", "https://cu.example")
    monkeypatch.setenv("CONTENT_UNDERSTANDING_ANALYZER_ID", "prebuilt-documentSearch")
    monkeypatch.setenv("OPENAI_CHAT_DEPLOYMENT_NAME", "gpt-5.4")
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")

    config = load_config()

    assert config.extraction_provider is expected


def test_load_config_rejects_extraction_without_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("EXTRACTION_ENABLED", "true")
    monkeypatch.setenv("CONTENT_UNDERSTANDING_ENABLED", "false")
    monkeypatch.setenv("DOCUMENT_INTELLIGENCE_ENABLED", "false")
    monkeypatch.setenv("OPENAI_CHAT_DEPLOYMENT_NAME", "gpt-5.4")
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")

    with pytest.raises(EnvironmentError, match="extraction provider"):
        load_config()


def test_load_config_requires_only_selected_cu_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("EXTRACTION_ENABLED", "true")
    monkeypatch.setenv("CONTENT_UNDERSTANDING_ENABLED", "true")
    monkeypatch.setenv("DOCUMENT_INTELLIGENCE_ENABLED", "true")
    monkeypatch.setenv("CONTENT_UNDERSTANDING_ENDPOINT", "https://cu.example")
    monkeypatch.setenv("CONTENT_UNDERSTANDING_ANALYZER_ID", "prebuilt-documentSearch")
    monkeypatch.setenv("OPENAI_CHAT_DEPLOYMENT_NAME", "gpt-5.4")
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    monkeypatch.delenv("DOCUMENT_INTELLIGENCE_ENDPOINT", raising=False)

    config = load_config()

    assert config.extraction_provider is ExtractionProvider.CONTENT_UNDERSTANDING
    assert config.document_intelligence_endpoint == ""


@pytest.mark.parametrize(
    ("missing_name", "error_name"),
    [
        ("CONTENT_UNDERSTANDING_ENDPOINT", "CONTENT_UNDERSTANDING_ENDPOINT"),
        ("CONTENT_UNDERSTANDING_ANALYZER_ID", "CONTENT_UNDERSTANDING_ANALYZER_ID"),
    ],
)
def test_load_config_requires_selected_cu_prerequisites(
    monkeypatch: pytest.MonkeyPatch,
    missing_name: str,
    error_name: str,
) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("EXTRACTION_ENABLED", "true")
    monkeypatch.setenv("CONTENT_UNDERSTANDING_ENABLED", "true")
    monkeypatch.setenv("DOCUMENT_INTELLIGENCE_ENABLED", "false")
    monkeypatch.setenv("CONTENT_UNDERSTANDING_ENDPOINT", "https://cu.example")
    monkeypatch.setenv("CONTENT_UNDERSTANDING_ANALYZER_ID", "prebuilt-documentSearch")
    monkeypatch.setenv("OPENAI_CHAT_DEPLOYMENT_NAME", "gpt-5.4")
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    monkeypatch.delenv(missing_name, raising=False)

    with pytest.raises(EnvironmentError, match=error_name):
        load_config()