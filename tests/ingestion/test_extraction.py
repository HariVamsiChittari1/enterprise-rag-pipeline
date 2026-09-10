from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pytest
from azure.ai.documentintelligence.models import DocumentContentFormat

from ingestion.errors import TerminalDocumentError
from ingestion.extraction import (
    PNG_SIGNATURE,
    VisionExtractionConfig,
    extract_content_understanding,
    extract_markdown,
    extract_office_document_intelligence,
    extract_pdf,
    extract_rendered_pdf_visuals,
)
from ingestion.models import (
    ContentModality,
    ExtractionProvenance,
    LocatorKind,
    VisualCoverageStatus,
)


@dataclass
class _Span:
    offset: int
    length: int


@dataclass
class _Page:
    spans: list[_Span]
    page_number: int | None = None


@dataclass
class _Region:
    page_number: int


@dataclass
class _Figure:
    id: str
    bounding_regions: list[_Region]


@dataclass
class _Result:
    content: str
    pages: list[_Page]
    figures: list[_Figure] = field(default_factory=list)


class _Poller:
    def __init__(self, result: _Result) -> None:
        self.details = {"operation_id": "result-1"}
        self._result = result

    def result(self) -> _Result:
        return self._result


class _ContentUnderstandingPoller:
    def __init__(
        self,
        result: Any,
        operation_id: str | None = "cu-result-1",
        raw_payload: Any = None,
        callback: Any = None,
    ) -> None:
        self.operation_id = operation_id
        self._result = result
        self._raw_payload = raw_payload
        self._callback = callback

    def result(self) -> Any:
        if self._callback is not None:
            http_response = type(
                "RawHttpResponse",
                (),
                {"json": lambda _: self._raw_payload},
            )()
            pipeline_response = type(
                "PipelineResponse",
                (),
                {"http_response": http_response},
            )()
            return self._callback(pipeline_response, self._result, {})
        return self._result


class _ContentUnderstandingClient:
    def __init__(
        self,
        result: Any,
        *,
        operation_id: str | None = "cu-result-1",
        delete_error: Exception | None = None,
        raw_payload: Any = None,
    ) -> None:
        self.result = result
        self.operation_id = operation_id
        self.delete_error = delete_error
        self.raw_payload = raw_payload
        self.analyze_arguments: dict[str, Any] = {}
        self.deleted: list[str] = []

    def begin_analyze_binary(self, **kwargs: Any) -> _ContentUnderstandingPoller:
        self.analyze_arguments = kwargs
        raw_payload = self.raw_payload
        if raw_payload is None:
            raw_payload = {
                "result": {
                    "contents": list(getattr(self.result, "contents", None) or [])
                }
            }
        return _ContentUnderstandingPoller(
            self.result,
            self.operation_id,
            raw_payload,
            kwargs.get("cls"),
        )

    def delete_result(self, *, operation_id: str) -> None:
        self.deleted.append(operation_id)
        if self.delete_error is not None:
            raise self.delete_error


class _HttpError(Exception):
    def __init__(self, status_code: int) -> None:
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code


class _DocumentIntelligenceClient:
    def __init__(
        self,
        result: _Result,
        *,
        delete_error: Exception | None = None,
        figure_parts: list[bytes] | None = None,
    ) -> None:
        self.result = result
        self.delete_error = delete_error
        self.figure_parts = figure_parts or [PNG_SIGNATURE + b"image"]
        self.analyze_arguments: dict[str, Any] = {}
        self.deleted: list[str] = []

    def begin_analyze_document(self, **kwargs: Any) -> _Poller:
        self.analyze_arguments = kwargs
        return _Poller(self.result)

    def get_analyze_result_figure(
        self, *, model_id: str, result_id: str, figure_id: str
    ) -> list[bytes]:
        assert (model_id, result_id, figure_id) == (
            "prebuilt-layout",
            "result-1",
            "figure-1",
        )
        return self.figure_parts

    def delete_analyze_result(self, *, model_id: str, result_id: str) -> None:
        assert model_id == "prebuilt-layout"
        self.deleted.append(result_id)
        if self.delete_error is not None:
            raise self.delete_error


@dataclass
class _Message:
    content: str | None


@dataclass
class _Choice:
    finish_reason: str
    message: _Message


@dataclass
class _Response:
    choices: list[_Choice]


class _Completions:
    def __init__(self, response_content: str) -> None:
        self.response_content = response_content
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> _Response:
        self.calls.append(kwargs)
        return _Response(
            choices=[
                _Choice(
                    finish_reason="stop",
                    message=_Message(content=self.response_content),
                )
            ]
        )


class _Chat:
    def __init__(self, response_content: str) -> None:
        self.completions = _Completions(response_content)


class _OpenAIClient:
    def __init__(self, response_content: str) -> None:
        self.chat = _Chat(response_content)


def _result_with_figure() -> _Result:
    text = "Production extraction text long enough to satisfy the minimum character limit."
    return _Result(
        content=text,
        pages=[_Page(spans=[_Span(offset=0, length=len(text))])],
        figures=[_Figure(id="figure-1", bounding_regions=[_Region(page_number=1)])],
    )


def _vision_config() -> VisionExtractionConfig:
    return VisionExtractionConfig(deployment="gpt-5.4")


def test_given_cu_document_result_when_extracting_then_returns_page_markdown_and_deletes_result() -> None:
    markdown = (
        "First page content is long enough for extraction."
        "Second page content is also sufficiently detailed."
    )
    first_length = markdown.index("Second")
    result = type(
        "CuResult",
        (),
        {
            "contents": [
                type(
                    "CuDocument",
                    (),
                    {
                        "markdown": markdown,
                        "pages": [
                            type(
                                "CuPage",
                                (),
                                {
                                    "page_number": 2,
                                    "spans": [
                                        _Span(
                                            offset=first_length,
                                            length=len(markdown) - first_length,
                                        )
                                    ],
                                },
                            )(),
                            type(
                                "CuPage",
                                (),
                                {
                                    "page_number": 1,
                                    "spans": [_Span(offset=0, length=first_length)],
                                },
                            )(),
                        ],
                    },
                )()
            ]
        },
    )()
    client = _ContentUnderstandingClient(result)

    extraction = extract_content_understanding(
        client,
        b"PK\x03\x04office-content",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        analyzer_id="rag-document-search-v1",
    )

    assert [segment.ordinal for segment in extraction.segments] == [0, 1]
    assert extraction.segments[0].text == markdown[:first_length].strip()
    assert extraction.segments[1].text == markdown[first_length:].strip()
    assert extraction.segments[0].locator.kind is LocatorKind.SECTION
    assert extraction.segments[0].locator.label == "Section 1"
    assert extraction.segments[0].provenance == (ExtractionProvenance.DIRECT,)
    assert extraction.visual_coverage.status is VisualCoverageStatus.NOT_REQUIRED
    assert callable(client.analyze_arguments.pop("cls"))
    assert client.analyze_arguments == {
        "analyzer_id": "rag-document-search-v1",
        "binary_input": b"PK\x03\x04office-content",
        "content_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    }
    assert client.deleted == ["cu-result-1"]


def test_given_blank_cu_page_when_extracting_then_preserves_nonblank_page_locator() -> None:
    text = "Second page content is long enough to satisfy the extraction threshold."
    document = type(
        "CuDocument",
        (),
        {
            "markdown": text,
            "pages": [
                type("CuPage", (), {"page_number": 1, "spans": []})(),
                type(
                    "CuPage",
                    (),
                    {
                        "page_number": 2,
                        "spans": [_Span(offset=0, length=len(text))],
                    },
                )(),
            ],
        },
    )()
    client = _ContentUnderstandingClient(
        type("CuResult", (), {"contents": [document]})()
    )

    extraction = extract_content_understanding(
        client,
        b"%PDF-test",
        "application/pdf",
        analyzer_id="prebuilt-documentSearch",
    )

    assert len(extraction.segments) == 1
    assert extraction.segments[0].ordinal == 0
    assert extraction.segments[0].text == text
    assert extraction.segments[0].locator.label == "Page 2"
    assert extraction.segments[0].locator.ordinal_start == 2
    assert client.deleted == ["cu-result-1"]


def test_given_all_blank_cu_pages_when_extracting_then_fails_and_deletes_result() -> None:
    document = type(
        "CuDocument",
        (),
        {
            "markdown": "",
            "pages": [
                type("CuPage", (), {"page_number": 1, "spans": []})(),
                type("CuPage", (), {"page_number": 2, "spans": []})(),
            ],
        },
    )()
    client = _ContentUnderstandingClient(
        type("CuResult", (), {"contents": [document]})()
    )

    with pytest.raises(TerminalDocumentError, match="extraction_insufficient_text"):
        extract_content_understanding(
            client,
            b"%PDF-test",
            "application/pdf",
            analyzer_id="prebuilt-documentSearch",
        )

    assert client.deleted == ["cu-result-1"]


def test_given_cu_pdf_figure_description_then_closes_visual_coverage() -> None:
    markdown = (
        'Operations content is sufficiently detailed. '
        '![process](figures/1.1 "A labeled approval flow")'
    )
    document = type(
        "CuDocument",
        (),
        {
            "markdown": markdown,
            "figures": [
                type(
                    "CuFigure",
                    (),
                    {"id": "1.1", "description": None},
                )()
            ],
            "pages": [
                type(
                    "CuPage",
                    (),
                    {"page_number": 1, "spans": [_Span(0, len(markdown))]},
                )()
            ],
        },
    )()
    client = _ContentUnderstandingClient(
        type("CuResult", (), {"contents": [document]})()
    )

    extraction = extract_content_understanding(
        client,
        b"%PDF-test",
        "application/pdf",
        analyzer_id="prebuilt-documentSearch",
    )

    assert extraction.segments[0].locator.kind is LocatorKind.PAGE
    assert ContentModality.VISUAL_DESCRIPTION in extraction.segments[0].modalities
    assert extraction.visual_coverage.status is VisualCoverageStatus.COMPLETE
    assert extraction.visual_coverage.inventory_count == 1
    assert extraction.visual_coverage.described_count == 1
    assert client.analyze_arguments["content_type"] == "application/pdf"
    assert client.deleted == ["cu-result-1"]


def test_given_cu_typed_figure_description_then_closes_visual_coverage() -> None:
    markdown = (
        "Operations content is sufficiently detailed. "
        "![process](figures/1.1)"
    )
    document = type(
        "CuDocument",
        (),
        {
            "markdown": markdown,
            "figures": [
                type(
                    "CuFigure",
                    (),
                    {"id": "1.1", "description": "A labeled approval flow"},
                )()
            ],
            "pages": [
                type(
                    "CuPage",
                    (),
                    {"page_number": 1, "spans": [_Span(0, len(markdown))]},
                )()
            ],
        },
    )()
    client = _ContentUnderstandingClient(
        type("CuResult", (), {"contents": [document]})()
    )

    extraction = extract_content_understanding(
        client,
        b"%PDF-test",
        "application/pdf",
        analyzer_id="prebuilt-documentSearch",
    )

    assert extraction.visual_coverage.status is VisualCoverageStatus.COMPLETE
    assert extraction.visual_coverage.inventory_count == 1
    assert extraction.visual_coverage.described_count == 1
    assert "Figure description: A labeled approval flow" in extraction.segments[0].text
    assert extraction.visual_manifest_entries[0].description == "A labeled approval flow"
    assert extraction.visual_manifest_entries[0].source_reference == "figures/1.1"
    assert client.deleted == ["cu-result-1"]


def test_given_figure_only_cu_page_when_extracting_then_indexes_typed_description() -> None:
    markdown = "![process](figures/1.1)"
    description = "A labeled approval flow with decision points and final outcomes."
    document = type(
        "CuDocument",
        (),
        {
            "markdown": markdown,
            "figures": [
                type(
                    "CuFigure",
                    (),
                    {"id": "1.1", "description": description},
                )()
            ],
            "pages": [
                type(
                    "CuPage",
                    (),
                    {"page_number": 1, "spans": [_Span(0, len(markdown))]},
                )()
            ],
        },
    )()
    client = _ContentUnderstandingClient(
        type("CuResult", (), {"contents": [document]})()
    )

    extraction = extract_content_understanding(
        client,
        b"%PDF-test",
        "application/pdf",
        analyzer_id="prebuilt-documentSearch",
    )

    assert len(extraction.segments) == 1
    assert f"Figure description: {description}" in extraction.segments[0].text
    assert extraction.segments[0].modalities == (
        ContentModality.TEXT,
        ContentModality.VISUAL_DESCRIPTION,
    )
    assert extraction.visual_coverage.status is VisualCoverageStatus.COMPLETE
    assert extraction.visual_coverage.inventory_count == 1
    assert extraction.visual_coverage.described_count == 1
    assert client.deleted == ["cu-result-1"]


def test_given_cu_typed_figure_span_without_markdown_reference_then_closes_visual_coverage() -> None:
    markdown = "Operations content is sufficiently detailed for visual extraction."
    document = type(
        "CuDocument",
        (),
        {
            "markdown": markdown,
            "figures": [
                type(
                    "CuFigure",
                    (),
                    {
                        "id": "1.1",
                        "description": "A labeled approval flow",
                        "span": _Span(11, 7),
                    },
                )()
            ],
            "pages": [
                type(
                    "CuPage",
                    (),
                    {"page_number": 1, "spans": [_Span(0, len(markdown))]},
                )()
            ],
        },
    )()
    client = _ContentUnderstandingClient(
        type("CuResult", (), {"contents": [document]})()
    )

    extraction = extract_content_understanding(
        client,
        b"%PDF-test",
        "application/pdf",
        analyzer_id="prebuilt-documentSearch",
    )

    assert ContentModality.VISUAL_DESCRIPTION in extraction.segments[0].modalities
    assert "Figure description: A labeled approval flow" in extraction.segments[0].text
    assert extraction.visual_coverage.status is VisualCoverageStatus.COMPLETE
    assert extraction.visual_manifest_entries[0].source_reference == "figures/1.1"
    assert client.deleted == ["cu-result-1"]


def test_given_cu_blank_markdown_title_then_indexes_typed_description() -> None:
    markdown = (
        "Operations content is sufficiently detailed. "
        '![process](figures/1.1 "   ")'
    )
    document = type(
        "CuDocument",
        (),
        {
            "markdown": markdown,
            "figures": [
                type(
                    "CuFigure",
                    (),
                    {"id": "1.1", "description": "A labeled approval flow"},
                )()
            ],
            "pages": [
                type(
                    "CuPage",
                    (),
                    {"page_number": 1, "spans": [_Span(0, len(markdown))]},
                )()
            ],
        },
    )()
    client = _ContentUnderstandingClient(
        type("CuResult", (), {"contents": [document]})()
    )

    extraction = extract_content_understanding(
        client,
        b"%PDF-test",
        "application/pdf",
        analyzer_id="prebuilt-documentSearch",
    )

    assert "Figure description: A labeled approval flow" in extraction.segments[0].text
    assert extraction.visual_coverage.status is VisualCoverageStatus.COMPLETE
    assert client.deleted == ["cu-result-1"]


@pytest.mark.parametrize(
    ("figure_span", "page_spans"),
    [
        (_Span(-1, 3), [[_Span(0, 80)]]),
        (_Span(28, 10), [[_Span(0, 32)], [_Span(32, 48)]]),
        (_Span(10, 5), [[_Span(0, 80)], [_Span(0, 80)]]),
    ],
    ids=["invalid-typed-span", "cross-page-span", "ambiguous-page-span"],
)
def test_given_cu_figure_without_unique_page_then_fails_and_deletes_result(
    figure_span: _Span,
    page_spans: list[list[_Span]],
) -> None:
    markdown = (
        "Operations content is sufficiently detailed. "
        '![process](figures/1.1 "A labeled approval flow")'
    )
    document = type(
        "CuDocument",
        (),
        {
            "markdown": markdown,
            "figures": [
                type(
                    "CuFigure",
                    (),
                    {
                        "id": "1.1",
                        "description": "A labeled approval flow",
                        "span": figure_span,
                    },
                )()
            ],
            "pages": [
                type(
                    "CuPage",
                    (),
                    {"page_number": index, "spans": spans},
                )()
                for index, spans in enumerate(page_spans, start=1)
            ],
        },
    )()
    client = _ContentUnderstandingClient(
        type("CuResult", (), {"contents": [document]})()
    )

    with pytest.raises(
        TerminalDocumentError,
        match="content_understanding_figure_evidence_incomplete",
    ):
        extract_content_understanding(
            client,
            b"%PDF-test",
            "application/pdf",
            analyzer_id="prebuilt-documentSearch",
        )

    assert client.deleted == ["cu-result-1"]


def test_given_cu_figure_without_description_then_fails_and_deletes_result() -> None:
    markdown = (
        "Operations content is sufficiently detailed. "
        "![process](figures/1.1)"
    )
    document = type(
        "CuDocument",
        (),
        {
            "markdown": markdown,
            "figures": [
                type(
                    "CuFigure",
                    (),
                    {"id": "1.1", "description": None},
                )()
            ],
            "pages": [
                type(
                    "CuPage",
                    (),
                    {"page_number": 1, "spans": [_Span(0, len(markdown))]},
                )()
            ],
        },
    )()
    client = _ContentUnderstandingClient(
        type("CuResult", (), {"contents": [document]})()
    )

    with pytest.raises(
        TerminalDocumentError,
        match="content_understanding_figure_evidence_incomplete",
    ):
        extract_content_understanding(
            client,
            b"%PDF-test",
            "application/pdf",
            analyzer_id="prebuilt-documentSearch",
        )

    assert client.deleted == ["cu-result-1"]


@pytest.mark.parametrize("figure_id", ["2.1", " 1.1 "])
def test_given_cu_figure_reference_mismatch_then_fails_and_deletes_result(
    figure_id: str,
) -> None:
    markdown = (
        "Operations content is sufficiently detailed. "
        '![process](figures/1.1 "A labeled approval flow")'
    )
    document = type(
        "CuDocument",
        (),
        {
            "markdown": markdown,
            "figures": [
                type(
                    "CuFigure",
                    (),
                    {"id": figure_id, "description": None},
                )()
            ],
            "pages": [
                type(
                    "CuPage",
                    (),
                    {"page_number": 1, "spans": [_Span(0, len(markdown))]},
                )()
            ],
        },
    )()
    client = _ContentUnderstandingClient(
        type("CuResult", (), {"contents": [document]})()
    )

    with pytest.raises(
        TerminalDocumentError,
        match="content_understanding_figure_evidence_incomplete",
    ):
        extract_content_understanding(
            client,
            b"%PDF-test",
            "application/pdf",
            analyzer_id="prebuilt-documentSearch",
        )

    assert client.deleted == ["cu-result-1"]


def test_given_office_di_result_when_extracting_then_returns_native_units_and_deletes_result() -> None:
    markdown = (
        "First slide content is long enough for extraction."
        "Second slide content is also sufficiently detailed."
    )
    second_offset = markdown.index("Second")
    di_client = _DocumentIntelligenceClient(
        _Result(
            content=markdown,
            pages=[
                _Page(
                    spans=[
                        _Span(
                            offset=second_offset,
                            length=len(markdown) - second_offset,
                        )
                    ],
                    page_number=2,
                ),
                _Page(spans=[_Span(offset=0, length=second_offset)], page_number=1),
            ],
        )
    )

    extraction = extract_office_document_intelligence(
        di_client,
        b"PK\x03\x04presentation",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    )

    assert [segment.ordinal for segment in extraction.segments] == [0, 1]
    assert [segment.text for segment in extraction.segments] == [
        markdown[:second_offset].strip(),
        markdown[second_offset:].strip(),
    ]
    assert extraction.segments[0].locator.kind is LocatorKind.SLIDE
    assert extraction.segments[0].locator.label == "Slide 1"
    assert extraction.segments[0].provenance == (ExtractionProvenance.DIRECT,)
    assert extraction.visual_coverage.status is VisualCoverageStatus.NOT_REQUIRED
    assert di_client.analyze_arguments == {
        "model_id": "prebuilt-layout",
        "body": di_client.analyze_arguments["body"],
        "output_content_format": DocumentContentFormat.MARKDOWN,
    }
    assert "output" not in di_client.analyze_arguments
    assert di_client.deleted == ["result-1"]


def test_given_blank_office_di_unit_when_extracting_then_omits_blank_unit() -> None:
    markdown = "Second slide content is long enough for native Office extraction."
    di_client = _DocumentIntelligenceClient(
        _Result(
            content=markdown,
            pages=[
                _Page(spans=[], page_number=1),
                _Page(spans=[_Span(offset=0, length=len(markdown))], page_number=2),
            ],
        )
    )

    extraction = extract_office_document_intelligence(
        di_client,
        b"PK\x03\x04presentation",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    )

    assert len(extraction.segments) == 1
    assert extraction.segments[0].ordinal == 0
    assert extraction.segments[0].locator.label == "Slide 2"
    assert extraction.segments[0].text == markdown
    assert di_client.deleted == ["result-1"]


def test_given_unsupported_office_type_when_extracting_with_di_then_fails_with_domain_error() -> None:
    markdown = "Office content is long enough for native extraction."
    di_client = _DocumentIntelligenceClient(
        _Result(
            content=markdown,
            pages=[
                _Page(
                    spans=[_Span(offset=0, length=len(markdown))],
                    page_number=1,
                )
            ],
        )
    )

    with pytest.raises(
        TerminalDocumentError,
        match="office_content_type_unsupported",
    ):
        extract_office_document_intelligence(
            di_client,
            b"unsupported-office-content",
            "application/vnd.example.unsupported-office",
        )

    assert di_client.deleted == ["result-1"]


def test_given_old_cu_result_shape_when_extracting_then_logs_only_shape_and_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    old_shape_result = type("OldCuResult", (), {"content": object()})()
    client = _ContentUnderstandingClient(
        old_shape_result,
        raw_payload={
            "status": "Succeeded",
            "result": {
                "markdown": "sensitive document text",
                "contents": [],
            },
        },
    )

    with pytest.raises(
        TerminalDocumentError,
        match="content_understanding_contents_missing",
    ):
        extract_content_understanding(
            client,
            b"%PDF-test",
            "application/pdf",
            analyzer_id="rag-document-search-v1",
        )

    assert client.deleted == ["cu-result-1"]
    assert "top_level_keys=['result', 'status']" in caplog.text
    assert "result_keys=['contents', 'markdown']" in caplog.text
    assert "status='Succeeded'" in caplog.text
    assert "raw_contents_type=list raw_contents_count=0" in caplog.text
    assert "sensitive document text" not in caplog.text


def test_given_failed_cu_response_when_extracting_then_logs_only_codes_and_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    result = type("CuResult", (), {"contents": []})()
    client = _ContentUnderstandingClient(
        result,
        raw_payload={
            "status": "Failed",
            "error": {
                "code": "InvalidRequest",
                "message": "sensitive service message",
                "details": [
                    {"code": "InvalidContent", "message": "sensitive detail"},
                    {"code": "unsafe code with spaces"},
                ],
                "innererror": {
                    "code": "UnsupportedFormat",
                    "message": "sensitive inner error",
                },
            },
            "result": {"contents": []},
        },
    )

    with pytest.raises(
        TerminalDocumentError,
        match="content_understanding_analysis_failed",
    ):
        extract_content_understanding(
            client,
            b"%PDF-test",
            "application/pdf",
            analyzer_id="rag-document-search-v1",
        )

    assert client.deleted == ["cu-result-1"]
    assert "error_keys=['code', 'details', 'innererror', 'message']" in caplog.text
    assert "error_code='InvalidRequest'" in caplog.text
    assert "detail_codes=['InvalidContent']" in caplog.text
    assert "inner_error_code='UnsupportedFormat'" in caplog.text
    assert "sensitive" not in caplog.text
    assert "unsafe code with spaces" not in caplog.text


def test_given_raw_cu_contents_when_typed_contents_missing_then_fails_as_deserialization() -> None:
    result = type("CuResult", (), {"contents": []})()
    client = _ContentUnderstandingClient(
        result,
        raw_payload={"result": {"contents": [{"kind": "document"}]}},
    )

    with pytest.raises(
        TerminalDocumentError,
        match="content_understanding_deserialization_failed",
    ):
        extract_content_understanding(
            client,
            b"PK\x03\x04office-content",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            analyzer_id="rag-document-search-v1",
        )

    assert client.deleted == ["cu-result-1"]


def test_cu_keeps_gapped_source_locator_ordinals_but_rebases_segments() -> None:
    markdown = "First visible slide content.Third visible slide content is sufficiently detailed."
    third_offset = markdown.index("Third")
    document = type(
        "CuDocument",
        (),
        {
            "markdown": markdown,
            "pages": [
                type("CuPage", (), {"page_number": 1, "spans": [_Span(0, third_offset)]})(),
                type(
                    "CuPage",
                    (),
                    {
                        "page_number": 3,
                        "spans": [_Span(third_offset, len(markdown) - third_offset)],
                    },
                )(),
            ],
        },
    )()
    client = _ContentUnderstandingClient(
        type("CuResult", (), {"contents": [document]})()
    )

    extraction = extract_content_understanding(
        client,
        b"PK\x03\x04presentation",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        analyzer_id="rag-document-search-v1",
    )

    assert [segment.ordinal for segment in extraction.segments] == [0, 1]
    assert [segment.locator.ordinal_start for segment in extraction.segments] == [1, 3]


def test_given_multiple_cu_contents_when_extracting_then_uses_each_markdown_scope() -> None:
    first_text = "First slide content is long enough for canonical extraction."
    second_text = "Second slide has different content and an independent span offset."
    first_document = type(
        "CuDocument",
        (),
        {
            "markdown": first_text,
            "pages": [
                type(
                    "CuPage",
                    (),
                    {"page_number": 1, "spans": [_Span(0, len(first_text))]},
                )()
            ],
        },
    )()
    second_document = type(
        "CuDocument",
        (),
        {
            "markdown": second_text,
            "pages": [
                type(
                    "CuPage",
                    (),
                    {"page_number": 2, "spans": [_Span(0, len(second_text))]},
                )()
            ],
        },
    )()
    client = _ContentUnderstandingClient(
        type("CuResult", (), {"contents": [second_document, first_document]})()
    )

    extraction = extract_content_understanding(
        client,
        b"PK\x03\x04presentation",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        analyzer_id="rag-document-search-v1",
    )

    assert [segment.text for segment in extraction.segments] == [
        first_text,
        second_text,
    ]
    assert [segment.ordinal for segment in extraction.segments] == [0, 1]
    assert [segment.locator.ordinal_start for segment in extraction.segments] == [1, 2]
    assert client.deleted == ["cu-result-1"]


def test_given_duplicate_pages_across_cu_contents_when_extracting_then_fails() -> None:
    text = "Duplicate page content is long enough for extraction."
    documents = [
        type(
            "CuDocument",
            (),
            {
                "markdown": text,
                "pages": [
                    type(
                        "CuPage",
                        (),
                        {"page_number": 1, "spans": [_Span(0, len(text))]},
                    )()
                ],
            },
        )()
        for _ in range(2)
    ]
    client = _ContentUnderstandingClient(
        type("CuResult", (), {"contents": documents})()
    )

    with pytest.raises(
        TerminalDocumentError,
        match="content_understanding_page_number_invalid",
    ):
        extract_content_understanding(
            client,
            b"PK\x03\x04presentation",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            analyzer_id="rag-document-search-v1",
        )

    assert client.deleted == ["cu-result-1"]


def test_given_inline_and_reference_images_when_extracting_markdown_then_preserves_text_and_describes_in_order() -> None:
    content = (
        b"# Operations guide\n\n"
        b"Review ![first](images/chart.png) and ![second][diagram].\n\n"
        b"[diagram]: <images/process flow.png>\n"
    )
    loaded_paths: list[str] = []

    def image_loader(path: str) -> bytes:
        loaded_paths.append(path)
        return PNG_SIGNATURE + path.encode("utf-8")

    openai_client = _OpenAIClient(
        json.dumps({"description": "A labeled process flow.", "elements": ["Start"]})
    )

    extraction = extract_markdown(
        content,
        image_loader=image_loader,
        openai_client=openai_client,
        vision_config=_vision_config(),
    )

    assert extraction.segments[0].text.startswith(content.decode("utf-8"))
    assert loaded_paths == ["images/chart.png", "images/process%20flow.png"]
    assert extraction.segments[0].text.count("A labeled process flow.") == 2
    assert extraction.segments[0].locator.kind is LocatorKind.SECTION
    assert extraction.segments[0].modalities == (
        ContentModality.TEXT,
        ContentModality.VISUAL_DESCRIPTION,
    )
    assert extraction.visual_coverage.status is VisualCoverageStatus.COMPLETE
    assert extraction.visual_coverage.described_count == 2
    assert len(openai_client.chat.completions.calls) == 2


def test_given_repeated_markdown_image_when_extracting_then_describes_bytes_once() -> None:
    content = (
        b"# Operations guide with enough direct text for extraction\n\n"
        b"![first](chart.png)\n\n![again](chart.png)\n"
    )
    loaded_paths: list[str] = []

    def image_loader(path: str) -> bytes:
        loaded_paths.append(path)
        return PNG_SIGNATURE + b"image"

    openai_client = _OpenAIClient(
        json.dumps({"description": "A chart.", "elements": []})
    )

    extraction = extract_markdown(
        content,
        image_loader=image_loader,
        openai_client=openai_client,
        vision_config=_vision_config(),
    )

    assert loaded_paths == ["chart.png"]
    assert len(openai_client.chat.completions.calls) == 1
    assert extraction.segments[0].text.count("A chart.") == 2
    assert extraction.visual_coverage.required_count == 2


def test_given_non_png_markdown_image_bytes_when_extracting_then_fails_before_vision() -> None:
    content = b"# Operations guide with enough text\n\n![chart](chart.png)\n"
    openai_client = _OpenAIClient(
        json.dumps({"description": "Not used.", "elements": []})
    )

    with pytest.raises(TerminalDocumentError, match="vision_image_not_png"):
        extract_markdown(
            content,
            image_loader=lambda _path: b"not-a-png",
            openai_client=openai_client,
            vision_config=_vision_config(),
        )

    assert openai_client.chat.completions.calls == []


def test_given_figure_when_extracting_pdf_then_appends_description_and_deletes_result() -> None:
    di_client = _DocumentIntelligenceClient(_result_with_figure())
    openai_client = _OpenAIClient(
        json.dumps({"description": "A labeled process flow.", "elements": ["Start", "End"]})
    )

    extraction = extract_pdf(
        di_client,
        b"%PDF-test",
        openai_client=openai_client,
        vision_config=_vision_config(),
    )

    assert "[Figure 1] A labeled process flow." in extraction.segments[0].text
    assert "Elements: Start; End" in extraction.segments[0].text
    assert extraction.segments[0].locator.kind is LocatorKind.PAGE
    assert ContentModality.VISUAL_DESCRIPTION in extraction.segments[0].modalities
    assert extraction.visual_coverage.status is VisualCoverageStatus.COMPLETE
    assert extraction.visual_coverage.described_count == 1
    assert di_client.deleted == ["result-1"]
    assert di_client.analyze_arguments["output"]
    request = openai_client.chat.completions.calls[0]
    assert request["model"] == "gpt-5.4"
    assert request["response_format"]["json_schema"]["strict"] is True
    assert request["messages"][1]["content"][1]["image_url"]["url"].startswith(
        "data:image/png;base64,"
    )


def test_given_blank_pdf_page_when_extracting_then_preserves_nonblank_page_locator() -> None:
    text = "Second page content is long enough to satisfy the extraction threshold."
    di_client = _DocumentIntelligenceClient(
        _Result(
            content=text,
            pages=[
                _Page(spans=[]),
                _Page(spans=[_Span(offset=0, length=len(text))]),
            ],
        )
    )

    extraction = extract_pdf(di_client, b"%PDF-test")

    assert len(extraction.segments) == 1
    assert extraction.segments[0].ordinal == 0
    assert extraction.segments[0].text == text
    assert extraction.segments[0].locator.label == "Page 2"
    assert extraction.segments[0].locator.ordinal_start == 2
    assert di_client.deleted == ["result-1"]


def test_given_all_blank_pdf_pages_when_extracting_then_fails_and_deletes_result() -> None:
    di_client = _DocumentIntelligenceClient(
        _Result(content="", pages=[_Page(spans=[]), _Page(spans=[])])
    )

    with pytest.raises(TerminalDocumentError, match="extraction_insufficient_text"):
        extract_pdf(di_client, b"%PDF-test")

    assert di_client.deleted == ["result-1"]


def test_rendered_pdf_visual_extraction_discards_derivative_page_text() -> None:
    di_client = _DocumentIntelligenceClient(_result_with_figure())
    openai_client = _OpenAIClient(
        json.dumps({"description": "A labeled process flow.", "elements": ["Start"]})
    )

    visuals = extract_rendered_pdf_visuals(
        di_client,
        b"%PDF-rendered",
        openai_client=openai_client,
        vision_config=_vision_config(),
    )

    assert len(visuals) == 1
    assert visuals[0].rendered_page == 1
    assert "A labeled process flow" in visuals[0].text
    assert "Production extraction text" not in visuals[0].text
    assert di_client.deleted == ["result-1"]


def test_given_malformed_vision_response_when_extracting_then_deletes_result() -> None:
    di_client = _DocumentIntelligenceClient(_result_with_figure())
    openai_client = _OpenAIClient('{"description":"missing elements"}')

    with pytest.raises(TerminalDocumentError, match="vision_response_shape_invalid"):
        extract_pdf(
            di_client,
            b"%PDF-test",
            openai_client=openai_client,
            vision_config=_vision_config(),
        )

    assert di_client.deleted == ["result-1"]


def test_given_figure_stream_exceeds_limit_when_extracting_then_stops_before_vision() -> None:
    di_client = _DocumentIntelligenceClient(
        _result_with_figure(),
        figure_parts=[PNG_SIGNATURE, b"1234", b"5678"],
    )
    openai_client = _OpenAIClient(
        json.dumps({"description": "Not used.", "elements": []})
    )

    with pytest.raises(TerminalDocumentError, match="vision_image_too_large"):
        extract_pdf(
            di_client,
            b"%PDF-test",
            openai_client=openai_client,
            vision_config=VisionExtractionConfig(
                deployment="gpt-5.4",
                max_image_bytes=len(PNG_SIGNATURE) + 4,
            ),
        )

    assert openai_client.chat.completions.calls == []
    assert di_client.deleted == ["result-1"]


def test_given_cleanup_failure_when_extracting_then_fails_closed() -> None:
    result = _result_with_figure()
    result.figures = []
    di_client = _DocumentIntelligenceClient(result, delete_error=_HttpError(400))

    with pytest.raises(
        TerminalDocumentError, match="document_intelligence_cleanup_rejected"
    ):
        extract_pdf(di_client, b"%PDF-test")

    assert di_client.deleted == ["result-1"]