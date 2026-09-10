"""Bounded OOXML visual inventory for Office visual-coverage accounting."""

from __future__ import annotations

import io
import posixpath
import re
import zipfile
from dataclasses import dataclass, replace
from pathlib import PurePosixPath
from xml.etree import ElementTree

from ingestion.errors import TerminalDocumentError
from ingestion.models import (
    CanonicalExtractionResult,
    CanonicalSegment,
    ContentModality,
    ExtractionProvenance,
    LocatorKind,
    SourceLocator,
    VisualCoverage,
    VisualCoverageStatus,
    VisualDisposition,
    VisualManifestEntry,
    VisualRelevance,
    content_sha256,
)

MAX_ARCHIVE_ENTRIES = 4_096
MAX_ARCHIVE_UNCOMPRESSED_BYTES = 100 * 1024 * 1024
MAX_XML_PART_BYTES = 8 * 1024 * 1024
MAX_INVENTORY_OBJECTS = 2_000

DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
PPTX_MIME = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

_RELATIONSHIP_NAMESPACE = "http://schemas.openxmlformats.org/package/2006/relationships"
_OFFICE_RELATIONSHIP_NAMESPACE = (
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
)
_VISUAL_RELATIONSHIPS = {
    "chart": "chart",
    "diagramData": "diagram",
    "drawing": "drawing",
    "image": "image",
    "vmlDrawing": "drawing",
}
_VISUAL_CONTAINERS = {"diagramData", "drawing", "vmlDrawing"}
_UNSUPPORTED_RELATIONSHIPS = {
    "audio": "audio",
    "comments": "comments",
    "media": "media",
    "notesSlide": "speaker_notes",
    "oleObject": "ole_object",
    "video": "video",
}
_SLIDE_PART_RE = re.compile(r"^ppt/slides/slide(\d+)\.xml$")


@dataclass(frozen=True)
class OfficeVisualObject:
    identity: str
    kind: str
    owner: SourceLocator
    package_part: str


@dataclass(frozen=True)
class OfficeOmission:
    identity: str
    kind: str
    owner: SourceLocator
    reason: str


@dataclass(frozen=True)
class OfficeContentUnit:
    part: str
    locator: SourceLocator
    hidden: bool


@dataclass(frozen=True)
class RenderedVisualDescription:
    ordinal: int
    rendered_page: int
    text: str

    def __post_init__(self) -> None:
        if self.ordinal < 0 or self.rendered_page < 1 or not self.text.strip():
            raise ValueError("rendered visual description is invalid")


@dataclass(frozen=True)
class OfficeVisualInventory:
    content_units: tuple[OfficeContentUnit, ...]
    required: tuple[OfficeVisualObject, ...]
    excluded: tuple[OfficeOmission, ...]
    unsupported: tuple[OfficeOmission, ...]

    @property
    def inventory_count(self) -> int:
        return len(self.required) + len(self.excluded) + len(self.unsupported)


@dataclass(frozen=True)
class _Relationship:
    relationship_id: str
    kind: str
    target: str
    external: bool


class _Package:
    def __init__(
        self,
        content: bytes,
        *,
        max_entries: int,
        max_uncompressed_bytes: int,
    ) -> None:
        try:
            self.archive = zipfile.ZipFile(io.BytesIO(content))
        except (zipfile.BadZipFile, OSError) as error:
            raise TerminalDocumentError("office_package_invalid") from error

        infos = self.archive.infolist()
        if len(infos) > max_entries:
            self.archive.close()
            raise TerminalDocumentError("office_archive_entry_limit_exceeded")
        if sum(info.file_size for info in infos) > max_uncompressed_bytes:
            self.archive.close()
            raise TerminalDocumentError("office_archive_size_limit_exceeded")

        self.infos: dict[str, zipfile.ZipInfo] = {}
        for info in infos:
            name = _validate_package_path(info.filename)
            if name in self.infos:
                self.archive.close()
                raise TerminalDocumentError("office_archive_duplicate_path")
            if info.flag_bits & 0x1:
                self.archive.close()
                raise TerminalDocumentError("office_archive_encrypted")
            self.infos[name] = info

    def close(self) -> None:
        self.archive.close()

    def read_xml(self, part: str, *, required: bool = True) -> ElementTree.Element | None:
        info = self.infos.get(part)
        if info is None:
            if required:
                raise TerminalDocumentError("office_package_part_missing")
            return None
        if info.file_size > MAX_XML_PART_BYTES:
            raise TerminalDocumentError("office_xml_part_limit_exceeded")
        try:
            value = self.archive.read(info)
        except (OSError, RuntimeError, zipfile.BadZipFile) as error:
            raise TerminalDocumentError("office_package_read_failed") from error
        upper_value = value.upper()
        if b"<!DOCTYPE" in upper_value or b"<!ENTITY" in upper_value:
            raise TerminalDocumentError("office_xml_declaration_unsupported")
        try:
            return ElementTree.fromstring(value)
        except ElementTree.ParseError as error:
            raise TerminalDocumentError("office_package_xml_invalid") from error


def inventory_office_visuals(
    content: bytes,
    content_type: str,
    *,
    max_entries: int = MAX_ARCHIVE_ENTRIES,
    max_uncompressed_bytes: int = MAX_ARCHIVE_UNCOMPRESSED_BYTES,
    max_objects: int = MAX_INVENTORY_OBJECTS,
    max_content_units: int = 300,
) -> OfficeVisualInventory:
    """Inventory OOXML visual relationships without extracting semantic text."""
    if (
        max_entries < 1
        or max_uncompressed_bytes < 1
        or max_objects < 1
        or max_content_units < 1
    ):
        raise ValueError("Office inventory limits must be positive")
    package = _Package(
        content,
        max_entries=max_entries,
        max_uncompressed_bytes=max_uncompressed_bytes,
    )
    try:
        owners = _owners(package, content_type)
        if len(owners) > max_content_units:
            raise TerminalDocumentError("office_content_unit_limit_exceeded")
        required: list[OfficeVisualObject] = []
        excluded: list[OfficeOmission] = []
        unsupported: list[OfficeOmission] = []

        for owner in owners:
            visual_objects, owner_unsupported = _owner_inventory(package, owner)
            if owner.hidden:
                excluded.extend(
                    OfficeOmission(
                        identity=visual.identity,
                        kind=visual.kind,
                        owner=visual.owner,
                        reason="hidden_owner",
                    )
                    for visual in visual_objects
                )
            else:
                required.extend(visual_objects)
            unsupported.extend(owner_unsupported)
            _enforce_object_limit(required, excluded, unsupported, max_objects)

        return OfficeVisualInventory(
            content_units=owners,
            required=tuple(_unique_by_identity(required)),
            excluded=tuple(_unique_by_identity(excluded)),
            unsupported=tuple(_unique_by_identity(unsupported)),
        )
    finally:
        package.close()


def merge_office_visuals(
    native: CanonicalExtractionResult,
    inventory: OfficeVisualInventory,
    rendered: tuple[RenderedVisualDescription, ...],
    content_type: str,
) -> CanonicalExtractionResult:
    """Merge rendered descriptions into native Office semantics and discard rendered text."""
    segments = _visible_native_segments(native.segments, inventory, content_type)
    pairings = _pair_required_visuals(inventory.required, rendered, content_type)
    described_keys: set[tuple[str, str]] = set()

    if content_type == DOCX_MIME:
        rendered_by_page: dict[int, list[str]] = {}
        for visual, description in pairings:
            key = _description_key(visual, description)
            if key in described_keys:
                continue
            described_keys.add(key)
            rendered_by_page.setdefault(description.rendered_page, []).append(
                description.text.strip()
            )
        for rendered_page, descriptions in sorted(rendered_by_page.items()):
            segments.append(
                CanonicalSegment(
                    ordinal=len(segments),
                    text="\n\n".join(descriptions),
                    locator=SourceLocator(
                        LocatorKind.PAGE,
                        f"Rendered page {rendered_page}",
                        rendered_page,
                        rendered_page,
                    ),
                    modalities=(
                        ContentModality.TEXT,
                        ContentModality.VISUAL_DESCRIPTION,
                    ),
                    provenance=(ExtractionProvenance.RENDERED,),
                )
            )
    else:
        segment_indexes = {
            segment.locator.ordinal_start: index
            for index, segment in enumerate(segments)
        }
        additions: dict[int, list[str]] = {}
        rendered_segments: list[CanonicalSegment] = []
        for visual, description in pairings:
            key = _description_key(visual, description)
            if key in described_keys:
                continue
            described_keys.add(key)
            segment_index = segment_indexes.get(visual.owner.ordinal_start)
            if segment_index is None:
                rendered_segments.append(
                    CanonicalSegment(
                        ordinal=len(segments) + len(rendered_segments),
                        text=description.text.strip(),
                        locator=visual.owner,
                        modalities=(
                            ContentModality.TEXT,
                            ContentModality.VISUAL_DESCRIPTION,
                        ),
                        provenance=(ExtractionProvenance.RENDERED,),
                    )
                )
                continue
            additions.setdefault(segment_index, []).append(description.text.strip())
        for segment_index, descriptions in additions.items():
            segment = segments[segment_index]
            segments[segment_index] = replace(
                segment,
                text=segment.text + "\n\n" + "\n\n".join(descriptions),
                modalities=tuple(
                    dict.fromkeys(
                        (*segment.modalities, ContentModality.VISUAL_DESCRIPTION)
                    )
                ),
                provenance=tuple(
                    dict.fromkeys(
                        (*segment.provenance, ExtractionProvenance.RENDERED)
                    )
                ),
            )
        segments.extend(rendered_segments)

    if len(described_keys) != len(inventory.required):
        raise TerminalDocumentError("office_visual_coverage_incomplete")
    manifest_entries = _office_manifest_entries(inventory, pairings)
    status = (
        VisualCoverageStatus.COMPLETE
        if inventory.required
        else VisualCoverageStatus.NOT_REQUIRED
    )
    return CanonicalExtractionResult(
        segments=tuple(
            replace(segment, ordinal=ordinal)
            for ordinal, segment in enumerate(segments)
        ),
        visual_coverage=VisualCoverage(
            status=status,
            inventory_count=inventory.inventory_count,
            required_count=len(inventory.required),
            described_count=len(described_keys),
            excluded_count=len(inventory.excluded),
            unsupported_count=len(inventory.unsupported),
            uncovered_count=0,
        ),
        visual_manifest_entries=manifest_entries,
    )


def bind_office_content_understanding_visuals(
    extraction: CanonicalExtractionResult,
    inventory: OfficeVisualInventory,
    content_type: str,
) -> CanonicalExtractionResult:
    """Bind rendered CU evidence to source-native Office visual identities."""
    described_entries = tuple(
        entry
        for entry in extraction.visual_manifest_entries
        if entry.disposition is VisualDisposition.DESCRIBED
    )
    if len(described_entries) != len(inventory.required):
        raise TerminalDocumentError("office_visual_coverage_incomplete")
    rendered = tuple(
        RenderedVisualDescription(
            ordinal=ordinal,
            rendered_page=entry.source_locator.ordinal_start,
            text=entry.description or "",
        )
        for ordinal, entry in enumerate(described_entries)
    )
    pairings = _pair_required_visuals(inventory.required, rendered, content_type)
    return replace(
        extraction,
        segments=tuple(
            replace(segment, provenance=(ExtractionProvenance.RENDERED,))
            for segment in extraction.segments
        ),
        visual_coverage=VisualCoverage(
            status=(
                VisualCoverageStatus.COMPLETE
                if inventory.required
                else VisualCoverageStatus.NOT_REQUIRED
            ),
            inventory_count=inventory.inventory_count,
            required_count=len(inventory.required),
            described_count=len(pairings),
            excluded_count=len(inventory.excluded),
            unsupported_count=len(inventory.unsupported),
            uncovered_count=0,
        ),
        visual_manifest_entries=_office_manifest_entries(
            inventory,
            pairings,
            derivative_locators={
                ordinal: entry.source_locator
                for ordinal, entry in enumerate(described_entries)
            },
        ),
    )


def _office_manifest_entries(
    inventory: OfficeVisualInventory,
    pairings: tuple[tuple[OfficeVisualObject, RenderedVisualDescription], ...],
    *,
    derivative_locators: dict[int, SourceLocator] | None = None,
) -> tuple[VisualManifestEntry, ...]:
    manifest_entries: list[VisualManifestEntry] = []
    for visual, description in pairings:
        manifest_entries.append(
            VisualManifestEntry(
                ordinal=len(manifest_entries),
                visual_id=visual.identity,
                object_type=visual.kind,
                source_locator=visual.owner,
                relevance=VisualRelevance.REQUIRED,
                disposition=VisualDisposition.DESCRIBED,
                description=description.text.strip(),
                provenance=(ExtractionProvenance.RENDERED,),
                derivative_locator=(
                    derivative_locators[description.ordinal]
                    if derivative_locators is not None
                    else SourceLocator(
                        LocatorKind.PAGE,
                        f"Rendered page {description.rendered_page}",
                        description.rendered_page,
                        description.rendered_page,
                    )
                ),
                source_reference=visual.package_part,
            )
        )
    for omission in inventory.excluded:
        manifest_entries.append(
            VisualManifestEntry(
                ordinal=len(manifest_entries),
                visual_id=omission.identity,
                object_type=omission.kind,
                source_locator=omission.owner,
                relevance=VisualRelevance.DECORATIVE,
                disposition=VisualDisposition.EXCLUDED,
                reason=omission.reason,
            )
        )
    for omission in inventory.unsupported:
        manifest_entries.append(
            VisualManifestEntry(
                ordinal=len(manifest_entries),
                visual_id=omission.identity,
                object_type=omission.kind,
                source_locator=omission.owner,
                relevance=VisualRelevance.UNRESOLVED,
                disposition=VisualDisposition.UNSUPPORTED,
                reason=omission.reason,
            )
        )
    return tuple(manifest_entries)


def _owners(package: _Package, content_type: str) -> tuple[OfficeContentUnit, ...]:
    if content_type == DOCX_MIME:
        return _word_owners(package)
    if content_type == PPTX_MIME:
        return _presentation_owners(package)
    if content_type == XLSX_MIME:
        return _workbook_owners(package)
    raise TerminalDocumentError("office_content_type_unsupported")


def _word_owners(package: _Package) -> tuple[OfficeContentUnit, ...]:
    parts = ["word/document.xml"]
    parts.extend(
        sorted(
            part
            for part in package.infos
            if re.fullmatch(r"word/(?:header|footer)\d+\.xml", part)
        )
    )
    owners: list[OfficeContentUnit] = []
    for ordinal, part in enumerate(parts, start=1):
        package.read_xml(part)
        label = "Document" if ordinal == 1 else PurePosixPath(part).stem.title()
        owners.append(
            OfficeContentUnit(
                part=part,
                locator=SourceLocator(LocatorKind.SECTION, label, ordinal, ordinal),
                hidden=False,
            )
        )
    return tuple(owners)


def _presentation_owners(package: _Package) -> tuple[OfficeContentUnit, ...]:
    matched_parts = sorted(
        (
            (int(match.group(1)), part)
            for part in package.infos
            if (match := _SLIDE_PART_RE.fullmatch(part))
        ),
        key=lambda item: item[0],
    )
    if not matched_parts:
        raise TerminalDocumentError("office_package_part_missing")
    owners: list[OfficeContentUnit] = []
    for ordinal, part in matched_parts:
        root = package.read_xml(part)
        assert root is not None
        hidden = root.attrib.get("show", "1").lower() in {"0", "false", "off"}
        owners.append(
            OfficeContentUnit(
                part=part,
                locator=SourceLocator(
                    LocatorKind.SLIDE,
                    f"Slide {ordinal}",
                    ordinal,
                    ordinal,
                ),
                hidden=hidden,
            )
        )
    return tuple(owners)


def _workbook_owners(package: _Package) -> tuple[OfficeContentUnit, ...]:
    workbook = package.read_xml("xl/workbook.xml")
    assert workbook is not None
    relationships = {
        relationship.relationship_id: relationship
        for relationship in _relationships(package, "xl/workbook.xml")
    }
    owners: list[OfficeContentUnit] = []
    for ordinal, sheet in enumerate(
        (element for element in workbook.iter() if _local_name(element.tag) == "sheet"),
        start=1,
    ):
        relationship_id = sheet.attrib.get(f"{{{_OFFICE_RELATIONSHIP_NAMESPACE}}}id")
        relationship = relationships.get(relationship_id or "")
        if relationship is None or relationship.external:
            raise TerminalDocumentError("office_worksheet_relationship_invalid")
        part = _resolve_target("xl/workbook.xml", relationship.target)
        package.read_xml(part)
        label = sheet.attrib.get("name", "").strip()
        if not label:
            raise TerminalDocumentError("office_worksheet_name_invalid")
        owners.append(
            OfficeContentUnit(
                part=part,
                locator=SourceLocator(
                    LocatorKind.WORKSHEET,
                    label,
                    ordinal,
                    ordinal,
                ),
                hidden=sheet.attrib.get("state", "visible") != "visible",
            )
        )
    if not owners:
        raise TerminalDocumentError("office_package_part_missing")
    return tuple(owners)


def _owner_inventory(
    package: _Package,
    owner: OfficeContentUnit,
) -> tuple[list[OfficeVisualObject], list[OfficeOmission]]:
    visual_objects: list[OfficeVisualObject] = []
    unsupported: list[OfficeOmission] = []
    relationships = _relationships(package, owner.part)
    for relationship in relationships:
        if relationship.kind in _UNSUPPORTED_RELATIONSHIPS:
            unsupported.append(
                OfficeOmission(
                    identity=f"{owner.part}#{relationship.relationship_id}",
                    kind=_UNSUPPORTED_RELATIONSHIPS[relationship.kind],
                    owner=owner.locator,
                    reason="unsupported_office_feature",
                )
            )
            continue
        if relationship.kind not in _VISUAL_RELATIONSHIPS:
            continue
        if relationship.external:
            unsupported.append(
                OfficeOmission(
                    identity=f"{owner.part}#{relationship.relationship_id}",
                    kind=_VISUAL_RELATIONSHIPS[relationship.kind],
                    owner=owner.locator,
                    reason="external_relationship",
                )
            )
            continue
        visual_objects.extend(
            _expand_visual_relationship(package, owner, relationship)
        )

    root = package.read_xml(owner.part)
    assert root is not None
    if _contains_unsupported_markup(root, owner.part):
        unsupported.append(
            OfficeOmission(
                identity=f"{owner.part}#unsupported-markup",
                kind="advanced_markup",
                owner=owner.locator,
                reason="unsupported_office_feature",
            )
        )
    if _contains_native_drawing(root) and not any(
        visual.package_part == owner.part for visual in visual_objects
    ):
        visual_objects.append(
            OfficeVisualObject(
                identity=f"{owner.part}#native-drawing",
                kind="drawing",
                owner=owner.locator,
                package_part=owner.part,
            )
        )
    return visual_objects, unsupported


def _expand_visual_relationship(
    package: _Package,
    owner: OfficeContentUnit,
    relationship: _Relationship,
) -> list[OfficeVisualObject]:
    target = _resolve_target(owner.part, relationship.target)
    if target not in package.infos:
        raise TerminalDocumentError("office_visual_relationship_target_missing")
    if relationship.kind in _VISUAL_CONTAINERS:
        nested = [
            nested_relationship
            for nested_relationship in _relationships(package, target)
            if nested_relationship.kind in _VISUAL_RELATIONSHIPS
            and not nested_relationship.external
        ]
        if nested:
            return [
                OfficeVisualObject(
                    identity=(
                        f"{owner.part}#{relationship.relationship_id}/"
                        f"{nested_relationship.relationship_id}"
                    ),
                    kind=_VISUAL_RELATIONSHIPS[nested_relationship.kind],
                    owner=owner.locator,
                    package_part=_resolve_target(target, nested_relationship.target),
                )
                for nested_relationship in nested
            ]
    return [
        OfficeVisualObject(
            identity=f"{owner.part}#{relationship.relationship_id}",
            kind=_VISUAL_RELATIONSHIPS[relationship.kind],
            owner=owner.locator,
            package_part=target,
        )
    ]


def _relationships(package: _Package, source_part: str) -> tuple[_Relationship, ...]:
    source_path = PurePosixPath(source_part)
    relationships_part = str(
        source_path.parent / "_rels" / f"{source_path.name}.rels"
    )
    root = package.read_xml(relationships_part, required=False)
    if root is None:
        return ()
    relationships: list[_Relationship] = []
    for element in root.findall(f"{{{_RELATIONSHIP_NAMESPACE}}}Relationship"):
        relationship_id = element.attrib.get("Id", "").strip()
        relationship_type = element.attrib.get("Type", "").strip()
        target = element.attrib.get("Target", "").strip()
        if not relationship_id or not relationship_type or not target:
            raise TerminalDocumentError("office_relationship_invalid")
        relationships.append(
            _Relationship(
                relationship_id=relationship_id,
                kind=relationship_type.rsplit("/", 1)[-1],
                target=target,
                external=element.attrib.get("TargetMode", "").lower() == "external",
            )
        )
    return tuple(relationships)


def _resolve_target(source_part: str, target: str) -> str:
    if "\\" in target:
        raise TerminalDocumentError("office_relationship_path_invalid")
    if target.startswith("/"):
        candidate = target.lstrip("/")
    else:
        candidate = posixpath.join(posixpath.dirname(source_part), target)
    normalized = posixpath.normpath(candidate)
    return _validate_package_path(normalized)


def _validate_package_path(path: str) -> str:
    if not path or "\\" in path or path.startswith("/"):
        raise TerminalDocumentError("office_archive_path_invalid")
    parts = path.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise TerminalDocumentError("office_archive_path_invalid")
    return path


def _contains_native_drawing(root: ElementTree.Element) -> bool:
    local_names = {_local_name(element.tag) for element in root.iter()}
    if local_names.intersection({"cxnSp", "grpSp"}):
        return True
    return any(
        _local_name(element.tag) == "prstGeom"
        and element.attrib.get("prst", "").startswith("flowChart")
        for element in root.iter()
    )


def _contains_unsupported_markup(root: ElementTree.Element, part: str) -> bool:
    local_names = {_local_name(element.tag) for element in root.iter()}
    if part.startswith("word/") and local_names.intersection(
        {"commentRangeStart", "del", "ins", "moveFrom", "moveTo"}
    ):
        return True
    return part.startswith("ppt/") and "timing" in local_names


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _visible_native_segments(
    native_segments: tuple[CanonicalSegment, ...],
    inventory: OfficeVisualInventory,
    content_type: str,
) -> list[CanonicalSegment]:
    if content_type == DOCX_MIME:
        return list(native_segments)
    content_units = {
        unit.locator.ordinal_start: unit for unit in inventory.content_units
    }
    visible_segments: list[CanonicalSegment] = []
    seen_ordinals: set[int] = set()
    for segment in native_segments:
        ordinal = segment.locator.ordinal_start
        content_unit = content_units.get(ordinal)
        if content_unit is None:
            raise TerminalDocumentError("office_native_unit_unmapped")
        seen_ordinals.add(ordinal)
        if content_unit.hidden:
            continue
        visible_segments.append(replace(segment, locator=content_unit.locator))
    required_ordinals = {
        unit.locator.ordinal_start
        for unit in inventory.content_units
        if not unit.hidden
    }
    visual_ordinals = {visual.owner.ordinal_start for visual in inventory.required}
    uncovered_ordinals = required_ordinals - seen_ordinals - visual_ordinals
    if uncovered_ordinals or (not visible_segments and not visual_ordinals):
        raise TerminalDocumentError("office_native_unit_coverage_incomplete")
    return visible_segments


def _pair_required_visuals(
    required: tuple[OfficeVisualObject, ...],
    rendered: tuple[RenderedVisualDescription, ...],
    content_type: str,
) -> tuple[tuple[OfficeVisualObject, RenderedVisualDescription], ...]:
    ordered_rendered = tuple(
        sorted(rendered, key=lambda value: (value.rendered_page, value.ordinal))
    )
    if content_type == PPTX_MIME:
        pairings: list[tuple[OfficeVisualObject, RenderedVisualDescription]] = []
        pages = sorted(
            {visual.owner.ordinal_start for visual in required}
            | {description.rendered_page for description in ordered_rendered}
        )
        for page in pages:
            page_required = [
                visual for visual in required if visual.owner.ordinal_start == page
            ]
            page_rendered = [
                description
                for description in ordered_rendered
                if description.rendered_page == page
            ]
            if len(page_required) != len(page_rendered):
                raise TerminalDocumentError("office_visual_coverage_incomplete")
            pairings.extend(zip(page_required, page_rendered, strict=True))
        return tuple(pairings)
    if len(required) != len(ordered_rendered):
        raise TerminalDocumentError("office_visual_coverage_incomplete")
    return tuple(zip(required, ordered_rendered, strict=True))


def _description_key(
    visual: OfficeVisualObject,
    description: RenderedVisualDescription,
) -> tuple[str, str]:
    normalized = " ".join(description.text.split()).casefold()
    return visual.identity, content_sha256(normalized)


def _enforce_object_limit(
    required: list[OfficeVisualObject],
    excluded: list[OfficeOmission],
    unsupported: list[OfficeOmission],
    max_objects: int,
) -> None:
    if len(required) + len(excluded) + len(unsupported) > max_objects:
        raise TerminalDocumentError("office_visual_object_limit_exceeded")


def _unique_by_identity(values: list) -> list:
    unique: dict[str, object] = {}
    for value in values:
        unique.setdefault(value.identity, value)
    return list(unique.values())