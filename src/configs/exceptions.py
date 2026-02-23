"""
Custom exceptions for the Salesforce → Oracle ingestion pipeline.

Hierarchy:
    IngestionError
    ├── QuarantineError       File/batch must be moved to error folder; pipeline halts.
    │   ├── AlignmentError    Row field count doesn't match header count.
    │   └── SizeBreachError   A column value exceeds Oracle's 4000 CHAR VARCHAR2 limit.
    └── DDLError              CREATE TABLE or ALTER TABLE failed or produced invalid DDL.
"""


class IngestionError(Exception):
    """Base class for all pipeline errors."""


class QuarantineError(IngestionError):
    """
    Raised when a file or batch must be quarantined.

    Args:
        message: Human-readable description of the failure.
        source_path: Path of the file being processed when the error occurred.
    """

    def __init__(self, message: str, source_path: str | None = None) -> None:
        super().__init__(message)
        self.source_path = source_path

    def __str__(self) -> str:
        base = super().__str__()
        if self.source_path:
            return f"{base} | source={self.source_path}"
        return base


class AlignmentError(QuarantineError):
    """
    Raised when a CSV row has a different number of fields than the header row.

    Args:
        message: Human-readable description.
        source_path: Path of the CSV file.
        row_number: 1-based row number where the misalignment was detected.
        expected: Number of fields expected (from header).
        got: Number of fields actually found in the row.
    """

    def __init__(
        self,
        message: str,
        source_path: str | None = None,
        row_number: int | None = None,
        expected: int | None = None,
        got: int | None = None,
    ) -> None:
        super().__init__(message, source_path)
        self.row_number = row_number
        self.expected = expected
        self.got = got

    def __str__(self) -> str:
        base = super().__str__()
        parts = []
        if self.row_number is not None:
            parts.append(f"row={self.row_number}")
        if self.expected is not None:
            parts.append(f"expected={self.expected}")
        if self.got is not None:
            parts.append(f"got={self.got}")
        if parts:
            return f"{base} | {' '.join(parts)}"
        return base


class SizeBreachError(QuarantineError):
    """
    Raised when a column value exceeds Oracle's VARCHAR2(4000 CHAR) hard limit.

    Args:
        message: Human-readable description.
        source_path: Path of the file being processed.
        column_name: The offending column's oracle_name or target_name.
        char_length: The actual character length that breached the limit.
        limit: The limit that was breached (default 4000).
    """

    def __init__(
        self,
        message: str,
        source_path: str | None = None,
        column_name: str | None = None,
        char_length: int | None = None,
        limit: int = 4000,
    ) -> None:
        super().__init__(message, source_path)
        self.column_name = column_name
        self.char_length = char_length
        self.limit = limit

    def __str__(self) -> str:
        base = super().__str__()
        parts = []
        if self.column_name:
            parts.append(f"column={self.column_name}")
        if self.char_length is not None:
            parts.append(f"char_length={self.char_length}")
        parts.append(f"limit={self.limit}")
        return f"{base} | {' '.join(parts)}"


class DDLError(IngestionError):
    """
    Raised when DDL generation or execution fails.

    Args:
        message: Human-readable description.
        ddl: The DDL statement that caused the failure, if available.
    """

    def __init__(self, message: str, ddl: str | None = None) -> None:
        super().__init__(message)
        self.ddl = ddl

    def __str__(self) -> str:
        base = super().__str__()
        if self.ddl:
            return f"{base} | ddl={self.ddl!r}"
        return base
