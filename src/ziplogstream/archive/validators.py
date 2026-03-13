"""
ziplogstream.archive.validators
==============================

Validation helpers for ZIP archive inputs.

Overview
--------
This module contains validation logic for archive-related inputs used by
the ziplogstream package. Its purpose is to keep path and archive checks
separate from streaming and member resolution logic.

Design goals
------------
- Fail early on invalid archive inputs
- Keep validation rules explicit and testable
- Raise package-specific exceptions where possible
- Avoid mixing validation with I/O-heavy streaming code

Current scope
-------------
This module validates:
- archive path existence
- archive path type
- expected `.zip` suffix

Notes
-----
Validation in this module is intentionally lightweight. It does not fully
parse or inspect the archive contents. It only verifies that the provided
input is suitable to attempt ZIP access.
"""

from __future__ import annotations

from pathlib import Path

from ziplogstream.errors import ZipValidationError


def normalize_zip_path(zip_path: Path | str) -> Path:
    """
    Normalize a user-provided ZIP path into a `Path` object.

    Args:
        zip_path:
            ZIP archive path as a `Path` or string.

    Returns:
        Normalized `Path` instance.

    Raises:
        ZipValidationError:
            If the provided path value is invalid or empty.
    """
    if isinstance(zip_path, Path):
        return zip_path

    if isinstance(zip_path, str):
        if not zip_path.strip():
            raise ZipValidationError("ZIP path must be a non-empty string")
        return Path(zip_path)

    raise ZipValidationError("ZIP path must be a pathlib.Path or string")


def validate_zip_path(zip_path: Path) -> None:
    """
    Validate that a path is suitable for ZIP archive access.

    Validation rules:
    - the path must exist
    - the path must refer to a file
    - the path suffix must be `.zip` (case-insensitive)

    Args:
        zip_path:
            Normalized archive path.

    Raises:
        FileNotFoundError:
            If the path does not exist.

        ZipValidationError:
            If the path is not a file or does not have a `.zip` suffix.
    """
    if not zip_path.exists():
        raise FileNotFoundError(f"ZIP not found: {zip_path}")

    if not zip_path.is_file():
        raise ZipValidationError(f"ZIP path is not a file: {zip_path}")

    if zip_path.suffix.lower() != ".zip":
        raise ZipValidationError(
            f"Expected a '.zip' archive, got suffix: {zip_path.suffix or '<none>'}"
        )