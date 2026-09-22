from __future__ import annotations

from dataclasses import replace

import pytest

from config import ExtractionProvider, load_config


def _set_required_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AUDIO_WRITER_ENABLED", raising=False)
    monkeypatch.delenv("AUDIO_TRANSCRIPTION_PROVIDER", raising=False)
    for name in ("SPEECH_ENDPOINT", "SPEECH_REGION", "AUDIO_DEPLOYMENT_REGION", "AUDIO_LOCALE"):
        monkeypatch.delenv(name, raising=False)
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


def test_audio_configuration_defaults_off_without_changing_documents(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    config = load_config()
    assert config.audio_writer_enabled is False
    assert config.audio_transcription_provider == "speech_fast"
    assert config.extraction_provider is None
    assert config.allowed_extensions == (".pdf",)
    assert (config.speech_endpoint, config.speech_region, config.audio_deployment_region, config.audio_locale) == (
        "", "", "", "",
    )


@pytest.mark.parametrize("value", ["yes", "1", "", "invalid"])
def test_audio_gate_rejects_non_boolean_environment_values(
    monkeypatch: pytest.MonkeyPatch, value: str,
) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("AUDIO_WRITER_ENABLED", value)
    with pytest.raises(ValueError, match="AUDIO_WRITER_ENABLED must be true or false"):
        load_config()


@pytest.mark.parametrize("value", ["fast", "enhanced", "batch", "", "translate"])
def test_audio_provider_rejects_unknown_values(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    monkeypatch.setenv("AUDIO_TRANSCRIPTION_PROVIDER", value)
    with pytest.raises(ValueError, match="AUDIO_TRANSCRIPTION_PROVIDER must be speech_fast"):
        load_config()


def test_audio_requires_master_extraction_switch(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    monkeypatch.setenv("AUDIO_WRITER_ENABLED", "true")
    with pytest.raises(ValueError, match="requires EXTRACTION_ENABLED"):
        load_config()


@pytest.mark.parametrize("cu_enabled", [False, True])
def test_audio_selector_does_not_change_document_provider(
    monkeypatch: pytest.MonkeyPatch, cu_enabled: bool,
) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    monkeypatch.setenv("EXTRACTION_ENABLED", "true")
    monkeypatch.setenv("AUDIO_WRITER_ENABLED", " TRUE ")
    monkeypatch.setenv("AUDIO_TRANSCRIPTION_PROVIDER", " SPEECH_FAST ")
    monkeypatch.setenv("SPEECH_ENDPOINT", " https://speech-fixture.cognitiveservices.azure.com/ ")
    monkeypatch.setenv("SPEECH_REGION", " CENTRALINDIA ")
    monkeypatch.setenv("AUDIO_DEPLOYMENT_REGION", " centralindia ")
    monkeypatch.setenv("AUDIO_LOCALE", " en-IN ")
    monkeypatch.setenv("CONTENT_UNDERSTANDING_ENABLED", str(cu_enabled))
    monkeypatch.setenv("CONTENT_UNDERSTANDING_ENDPOINT", "https://cu.example")
    monkeypatch.setenv("CONTENT_UNDERSTANDING_ANALYZER_ID", "test-analyzer")
    monkeypatch.setenv("DOCUMENT_INTELLIGENCE_ENDPOINT", "https://di.example")
    monkeypatch.setenv("OPENAI_CHAT_DEPLOYMENT_NAME", "test-vision")
    config = load_config()
    assert config.audio_writer_enabled is True
    assert config.audio_transcription_provider == "speech_fast"
    assert config.speech_region == config.audio_deployment_region == "centralindia"
    assert config.audio_locale == "en-IN"
    expected = ExtractionProvider.CONTENT_UNDERSTANDING if cu_enabled else ExtractionProvider.DOCUMENT_INTELLIGENCE
    assert config.extraction_provider is expected


@pytest.mark.parametrize("overrides", [
    {"audio_writer_enabled": 1}, {"audio_transcription_provider": None},
    {"audio_writer_enabled": True},
])
def test_programmatic_audio_config_cannot_bypass_validation(
    monkeypatch: pytest.MonkeyPatch, overrides: dict[str, object],
) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    with pytest.raises(ValueError, match="AUDIO_"):
        replace(load_config(), **overrides)


@pytest.mark.parametrize("field", ["speech_endpoint", "speech_region", "audio_deployment_region", "audio_locale"])
def test_enabled_audio_requires_explicit_speech_settings(monkeypatch: pytest.MonkeyPatch, field: str) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    settings = dict(
        extraction_enabled=True, audio_writer_enabled=True,
        speech_endpoint="https://speech-fixture.cognitiveservices.azure.com",
        speech_region="eastus2", audio_deployment_region="eastus2", audio_locale="en-US",
    )
    settings[field] = ""
    with pytest.raises(ValueError, match=field.upper()):
        replace(load_config(), **settings)


_BATCH_SETTINGS = dict(
    extraction_enabled=True,
    audio_writer_enabled=True,
    audio_transcription_provider="speech_batch",
    speech_endpoint="https://speech-fixture.cognitiveservices.azure.com",
    speech_region="eastus2",
    audio_deployment_region="eastus2",
    audio_locale="en-US",
    audio_staging_blob_endpoint="https://astg.blob.core.windows.net/",
    audio_staging_container="audio-staging",
)


def test_speech_batch_provider_accepted_with_staging(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    config = replace(load_config(), **_BATCH_SETTINGS)
    assert config.audio_transcription_provider == "speech_batch"
    assert config.audio_staging_container == "audio-staging"
    assert config.audio_batch_ttl_hours == 48


@pytest.mark.parametrize("field", ["audio_staging_blob_endpoint", "audio_staging_container"])
def test_speech_batch_requires_staging_settings(monkeypatch: pytest.MonkeyPatch, field: str) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    settings = dict(_BATCH_SETTINGS)
    settings[field] = ""
    with pytest.raises(ValueError, match=field.upper()):
        replace(load_config(), **settings)


@pytest.mark.parametrize("endpoint", [
    "http://astg.blob.core.windows.net",
    "https://astg.blob.core.windows.net.evil.example",
    "https://user:secret@astg.blob.core.windows.net",
    "https://astg.file.core.windows.net",
])
def test_speech_batch_rejects_non_blob_staging_endpoint(
    monkeypatch: pytest.MonkeyPatch, endpoint: str,
) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    settings = dict(_BATCH_SETTINGS)
    settings["audio_staging_blob_endpoint"] = endpoint
    with pytest.raises(ValueError, match="AUDIO_STAGING_BLOB_ENDPOINT"):
        replace(load_config(), **settings)


@pytest.mark.parametrize("ttl", [5, 745, 0, -1])
def test_audio_batch_ttl_out_of_range_is_rejected(monkeypatch: pytest.MonkeyPatch, ttl: int) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    with pytest.raises(ValueError, match="AUDIO_BATCH_TTL_HOURS"):
        replace(load_config(), audio_batch_ttl_hours=ttl)


@pytest.mark.parametrize("endpoint", [
    "http://speech-fixture.cognitiveservices.azure.com",
    "https://speech-fixture.cognitiveservices.azure.com.evil.example",
    "https://user:secret@fixture.cognitiveservices.azure.com",
    "https://fixture.cognitiveservices.azure.com:443",
    "https://fixture.cognitiveservices.azure.com/path",
    "https://fixture.cognitiveservices.azure.com?token=secret",
    "https://fixture.cognitiveservices.azure.com#fragment",
    "https://fixture.cognitiveservices.azure.com\n",
    "https://[invalid", "https://127.0.0.1", "https://-fixture.cognitiveservices.azure.com",
    "https://fixture-.cognitiveservices.azure.com", "https://eastus2.api.cognitive.microsoft.com",
])
def test_audio_endpoint_rejects_non_custom_base_urls(monkeypatch: pytest.MonkeyPatch, endpoint: str) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    with pytest.raises(ValueError, match="^SPEECH_ENDPOINT must be an HTTPS custom-domain base URL$"):
        replace(load_config(), speech_endpoint=endpoint)


@pytest.mark.parametrize("region,accepted", [
    ("eastus2", True), ("centralindia", True), ("southeastasia", True), ("unknown", False),
])
def test_audio_declared_region_matrix(
    monkeypatch: pytest.MonkeyPatch, region: str, accepted: bool,
) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    config = load_config()
    settings = dict(
        extraction_enabled=True, audio_writer_enabled=True,
        speech_endpoint="https://speech-fixture.cognitiveservices.azure.com",
        speech_region=region, audio_deployment_region=region, audio_locale="en-US",
    )
    if accepted:
        assert replace(config, **settings).speech_region == region
    else:
        with pytest.raises(ValueError):
            replace(config, **settings)


def test_audio_rejects_cross_region_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    with pytest.raises(ValueError, match="SPEECH_REGION must match AUDIO_DEPLOYMENT_REGION"):
        replace(load_config(), speech_region="eastus2", audio_deployment_region="centralindia")


@pytest.mark.parametrize("locale", ["en-US", "en-GB", "en-IN", "fr-FR", "en-XX", "en-us"])
def test_audio_locale_allowlist(monkeypatch: pytest.MonkeyPatch, locale: str) -> None:
    _set_required_environment(monkeypatch)
    monkeypatch.setenv("SHAREPOINT_SITE_URL", "https://tenant.sharepoint.com/sites/site")
    monkeypatch.setenv("AUDIO_LOCALE", locale)
    if locale in ("en-US", "en-GB", "en-IN"):
        assert load_config().audio_locale == locale
    else:
        with pytest.raises(ValueError, match="AUDIO_LOCALE"):
            load_config()


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