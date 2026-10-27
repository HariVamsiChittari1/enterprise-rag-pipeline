"""Typed failure categories for ingestion outcome decisions."""


class TerminalDocumentError(ValueError):
    """The current source change is permanently unsupported and may be quarantined."""


class DependencyAccessError(RuntimeError):
    """A dependency rejected our credentials (401/403); replay once access is restored."""


class StaleFenceError(RuntimeError):
    """A predecessor worker attempted to mutate successor-owned state."""