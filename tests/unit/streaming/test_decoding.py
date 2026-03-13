"""
tests.unit.streaming.test_decoding
========================

Unit tests for :mod:`zipstreamer.streaming.decoding`.

Overview
--------
This module verifies the small low-level decoding helpers used by the
streaming layer.

Although these helpers are simple, they sit on the hot path of line
processing and define part of the package's text normalization behavior.
These tests ensure that byte decoding remains explicit and that CRLF
normalization behaves exactly as intended.

Behavior under test
-------------------
The decoding helpers are expected to:

- remove a single trailing carriage return byte when present
- leave non-CR-terminated data unchanged
- decode both ``bytes`` and ``bytearray`` inputs
- respect the configured encoding and error-handling policy

Test philosophy
---------------
These are pure unit tests. They perform no I/O and validate only the
behavior of the helper functions in isolation.
"""

from __future__ import annotations

from ziplogstream.streaming.decoding import decode_text, strip_trailing_cr


def test_strip_trailing_cr_removes_single_terminal_carriage_return() -> None:
    """
    Ensure a trailing ``b"\\r"`` is removed exactly once.

    This matches the package's CRLF normalization behavior after splitting
    on the newline byte.
    """
    assert strip_trailing_cr(b"hello\r") == b"hello"


def test_strip_trailing_cr_leaves_non_terminated_bytes_unchanged() -> None:
    """
    Ensure byte data without a trailing carriage return is returned
    unchanged.
    """
    assert strip_trailing_cr(b"hello") == b"hello"


def test_strip_trailing_cr_returns_empty_bytes_unchanged() -> None:
    """
    Ensure empty byte input is handled safely and remains unchanged.
    """
    assert strip_trailing_cr(b"") == b""


def test_strip_trailing_cr_handles_bytearray_input() -> None:
    """
    Ensure the helper also works correctly for ``bytearray`` input, which
    is used in the streaming buffer implementation.
    """
    assert strip_trailing_cr(bytearray(b"hello\r")) == bytearray(b"hello")


def test_decode_text_decodes_bytes_using_configured_encoding() -> None:
    """
    Ensure raw ``bytes`` are decoded using the provided codec settings.
    """
    assert decode_text(b"hello", encoding="utf-8", errors="strict") == "hello"


def test_decode_text_decodes_bytearray_using_configured_encoding() -> None:
    """
    Ensure ``bytearray`` input is decoded the same way as ``bytes``.
    """
    assert (
        decode_text(bytearray("héllo".encode("utf-8")), encoding="utf-8", errors="strict")
        == "héllo"
    )


def test_decode_text_honors_replace_error_policy() -> None:
    """
    Ensure invalid byte sequences are handled according to the configured
    decode error policy.

    Here, the ``replace`` policy should yield the Unicode replacement
    character rather than raising an exception.
    """
    assert decode_text(b"\xff", encoding="utf-8", errors="replace") == "�"


def test_decode_text_honors_ignore_error_policy() -> None:
    """
    Ensure the ``ignore`` decode policy drops invalid byte sequences.
    """
    assert decode_text(b"a\xffb", encoding="utf-8", errors="ignore") == "ab"