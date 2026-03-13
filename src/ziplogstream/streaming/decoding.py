"""
ziplogstream.streaming.decoding
==============================

Low-level byte decoding helpers for streaming text lines.

Overview
--------
This module contains small, focused helpers for transforming raw byte
slices into decoded text while preserving the streaming semantics defined
by the package.

Design goals
------------
- Keep decoding helpers isolated and easily testable
- Avoid repeating byte-to-text logic across streaming components
- Preserve explicit handling of CRLF line endings
- Keep policy small and predictable

Notes
-----
This module does not perform any I/O and does not manage buffering. It is
used by higher-level streaming components that handle chunked reads and
line boundary detection.
"""

from __future__ import annotations


def strip_trailing_cr(data: bytes | bytearray) -> bytes:
    """
    Remove a single trailing carriage return byte, if present.

    This helper is primarily used to normalize CRLF line endings after
    splitting on the newline byte (``b"\\n"``). It removes only one
    trailing ``b"\\r"`` and leaves all other content unchanged.

    Args:
        data:
            Raw byte sequence representing a line or partial line.

    Returns:
        A ``bytes`` object with one trailing carriage return removed if
        present, otherwise the original content converted to ``bytes``.
    """
    normalized = bytes(data)
    if normalized.endswith(b"\r"):
        return normalized[:-1]
    return normalized


def decode_text(data: bytes | bytearray, *, encoding: str, errors: str) -> str:
    """
    Decode raw bytes into text using the configured codec settings.

    Args:
        data:
            Raw bytes to decode.

        encoding:
            Text encoding name, such as `"utf-8"`.

        errors:
            Decode error handling policy, such as `"strict"`,
            `"replace"`, or `"ignore"`.

    Returns:
        Decoded text string.
    """
    return bytes(data).decode(encoding, errors=errors)