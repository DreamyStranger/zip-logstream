"""
ziplogstream.archive.member_resolution
=====================================

Deterministic ZIP member resolution strategies.

Overview
--------
This module provides logic for resolving a single target member from an
open ZIP archive. Resolution is intentionally separated from archive
opening and line streaming so that member selection remains easy to test,
replace, and reason about.

Public contract
---------------
Resolvers in this module operate on an already-open `zipfile.ZipFile`
instance and return the exact member name to read.

The default resolver follows a deterministic strategy:
    1. If the target contains no '/' characters, prefer exact basename
       matches across all members.
    2. If no basename match is found, fall back to suffix matching via
       `member_name.endswith(target)`.
    3. If no members match, raise `ZipMemberNotFoundError`.
    4. If multiple members match, raise `ZipMemberAmbiguityError`.

Design goals
------------
- Deterministic behavior
- Explicit ambiguity failures
- Minimal policy surface
- Easy replacement with custom resolvers

This module does not open archives or stream bytes.
"""

from __future__ import annotations

import zipfile
from typing import Sequence

from ziplogstream.errors import ZipMemberAmbiguityError, ZipMemberNotFoundError


def default_zip_member_resolver(zf: zipfile.ZipFile, target: str) -> str:
    """
    Resolve a unique target member inside a ZIP archive.

    Resolution strategy:
        1. If `target` contains no '/' characters, prefer exact basename
           matches. For example, target="app.log" matches members whose
           basename is exactly "app.log", such as "logs/app.log".
        2. If no basename matches are found, fall back to suffix matching
           using `member_name.endswith(target)`.
        3. If no matches exist, raise `ZipMemberNotFoundError`.
        4. If multiple matches exist, raise `ZipMemberAmbiguityError`.

    Args:
        zf:
            Open ZIP archive instance.

        target:
            Filename or suffix identifying the desired member.

    Returns:
        The resolved archive member name.

    Raises:
        ZipMemberNotFoundError:
            If no archive member matches the target.

        ZipMemberAmbiguityError:
            If the target matches more than one archive member.
    """
    if not isinstance(target, str) or not target:
        raise ZipMemberNotFoundError("Target member selector must be a non-empty string")

    names: Sequence[str] = zf.namelist()
    matches: list[str] = []

    if "/" not in target:
        basename_matches = [name for name in names if name.rsplit("/", 1)[-1] == target]
        if basename_matches:
            matches = basename_matches

    if not matches:
        matches = [name for name in names if name.endswith(target)]

    if not matches:
        zip_name = getattr(zf, "filename", "<zip>")
        raise ZipMemberNotFoundError(f"'{target}' not found in ZIP: {zip_name}")

    if len(matches) > 1:
        raise ZipMemberAmbiguityError(
            f"Ambiguous target '{target}'. Matches: {matches}"
        )

    return matches[0]