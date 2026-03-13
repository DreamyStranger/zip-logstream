"""
ziplogstream.archive.openers
===========================

Archive-opening helpers for ZIP-backed streaming.

Overview
--------
This module provides small orchestration helpers that connect archive
validation and member resolution. Its purpose is to keep archive-opening
concerns separate from line decoding and streaming behavior.

Design goals
------------
- Centralize ZIP archive opening logic
- Reuse validation and resolution building blocks
- Keep the high-level streamer implementation thin
- Preserve deterministic member selection behavior

Notes
-----
This module does not implement line parsing or buffered text streaming.
It is responsible only for preparing archive resources and resolving the
member that should be opened.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

from ziplogstream.archive.validators import normalize_zip_path, validate_zip_path
from ziplogstream.types.protocols import ZipMemberResolver


def resolve_zip_member_name(
    zip_path: Path | str,
    target: str,
    *,
    resolver: ZipMemberResolver,
) -> tuple[Path, str]:
    """
    Validate a ZIP path and resolve the exact target member name.

    This helper is useful when the caller needs the normalized archive path
    together with the resolved member name, but does not want to duplicate
    archive validation and resolution logic.

    Args:
        zip_path:
            Path to the ZIP archive.

        target:
            User-provided member selector string.

        resolver:
            Callable used to resolve `target` to one exact member name.

    Returns:
        A tuple of:
        - normalized ZIP path
        - resolved member name

    Raises:
        FileNotFoundError:
            If the ZIP path does not exist.

        ZipValidationError:
            If the archive path is invalid.

        ZipMemberNotFoundError:
            If the resolver cannot find a matching member.

        ZipMemberAmbiguityError:
            If the resolver finds more than one matching member.
    """
    normalized_path = normalize_zip_path(zip_path)
    validate_zip_path(normalized_path)

    with zipfile.ZipFile(normalized_path, "r") as zf:
        member_name = resolver(zf, target)

    return normalized_path, member_name