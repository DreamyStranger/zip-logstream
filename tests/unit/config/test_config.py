"""
tests.unit.config.test_config
======================

Unit tests for :mod:`zipstreamer.config.line_streamer`.

Overview
--------
This module verifies the behavior of the public
:class:`zipstreamer.config.line_streamer.LineStreamerConfig` dataclass.

The configuration object is intentionally small, but it is a critical part
of the package contract because it defines how the streaming layer reads,
buffers, and decodes text from ZIP members. These tests ensure that:

- default values remain stable and intentional
- valid custom values are accepted unchanged
- invalid numeric values are rejected early
- invalid codec settings fail fast during configuration construction

Test philosophy
---------------
These are pure unit tests. They do not perform any ZIP I/O and do not
exercise the streaming pipeline itself. Their goal is to validate only the
configuration model and its input validation rules.
"""

from __future__ import annotations

import pytest

from ziplogstream.config import LineStreamerConfig
from ziplogstream.errors import ConfigurationError


def test_config_defaults_are_stable() -> None:
    """
    Ensure the default configuration matches the documented package
    defaults.

    This test protects the public contract for callers who instantiate
    ``LineStreamerConfig()`` without overrides and expect predictable
    baseline behavior.
    """
    cfg = LineStreamerConfig()

    assert cfg.chunk_size == 1 << 20
    assert cfg.encoding == "utf-8"
    assert cfg.errors == "replace"
    assert cfg.max_line_bytes == 32 * (1 << 20)


def test_config_accepts_valid_custom_values() -> None:
    """
    Ensure valid caller-provided values are stored unchanged.

    This verifies the happy-path construction case for consumers who want
    to tune chunk size, decoding behavior, or maximum buffered line size.
    """
    cfg = LineStreamerConfig(
        chunk_size=4096,
        encoding="utf-8",
        errors="strict",
        max_line_bytes=8192,
    )

    assert cfg.chunk_size == 4096
    assert cfg.encoding == "utf-8"
    assert cfg.errors == "strict"
    assert cfg.max_line_bytes == 8192


@pytest.mark.parametrize("chunk_size", [0, -1])
def test_config_rejects_non_positive_chunk_size(chunk_size: int) -> None:
    """
    Ensure ``chunk_size`` must be strictly positive.

    A zero or negative read size would make the streaming contract invalid,
    so configuration should fail immediately rather than later at runtime.
    """
    with pytest.raises(ConfigurationError, match="chunk_size must be greater than 0"):
        LineStreamerConfig(chunk_size=chunk_size)


@pytest.mark.parametrize("max_line_bytes", [0, -1])
def test_config_rejects_non_positive_max_line_bytes(max_line_bytes: int) -> None:
    """
    Ensure ``max_line_bytes`` must be strictly positive.

    The package uses this value as a hard upper bound for buffered content
    in the oversized-line protection path, so invalid values must be
    rejected at construction time.
    """
    with pytest.raises(
        ConfigurationError,
        match="max_line_bytes must be greater than 0",
    ):
        LineStreamerConfig(max_line_bytes=max_line_bytes)


def test_config_rejects_max_line_bytes_smaller_than_chunk_size() -> None:
    """
    Ensure ``max_line_bytes`` cannot be smaller than ``chunk_size``.

    This protects against contradictory configuration where a single read
    operation could exceed the allowed buffered line size immediately.
    """
    with pytest.raises(
        ConfigurationError,
        match="max_line_bytes must be greater than or equal to chunk_size",
    ):
        LineStreamerConfig(chunk_size=8192, max_line_bytes=4096)


def test_config_rejects_empty_encoding() -> None:
    """
    Ensure the encoding name must be a non-empty string.

    Empty encoding values should fail fast instead of being deferred to
    later decode operations.
    """
    with pytest.raises(ConfigurationError, match="encoding must be a non-empty string"):
        LineStreamerConfig(encoding="")


def test_config_rejects_unknown_encoding() -> None:
    """
    Ensure invalid codec names are rejected during configuration
    validation.

    This keeps codec misconfiguration close to object construction and
    avoids surprising failures during streaming.
    """
    with pytest.raises(ConfigurationError, match="Unknown encoding"):
        LineStreamerConfig(encoding="definitely-not-a-real-encoding")


def test_config_rejects_empty_errors_handler() -> None:
    """
    Ensure the decode error policy name must be a non-empty string.

    As with encoding validation, this should fail during configuration
    construction rather than later when decoding occurs.
    """
    with pytest.raises(ConfigurationError, match="errors must be a non-empty string"):
        LineStreamerConfig(errors="")


def test_config_rejects_unknown_errors_handler() -> None:
    """
    Ensure invalid decode error handler names are rejected early.

    This confirms that codec error-policy validation is performed eagerly
    and consistently.
    """
    with pytest.raises(ConfigurationError, match="Unknown decoding error handler"):
        LineStreamerConfig(errors="definitely-not-a-real-error-handler")

def test_config_rejects_non_integer_chunk_size() -> None:
    """
    Ensure ``chunk_size`` must be an integer value.

    This protects the configuration model against invalid non-integer
    inputs that would make chunked binary reads ill-defined.
    """
    with pytest.raises(ConfigurationError, match="chunk_size must be an integer"):
        LineStreamerConfig(chunk_size="4096")  # type: ignore[arg-type]


def test_config_rejects_non_integer_max_line_bytes() -> None:
    """
    Ensure ``max_line_bytes`` must be an integer value.

    This protects the oversized-line buffering contract from invalid
    non-integer threshold values.
    """
    with pytest.raises(ConfigurationError, match="max_line_bytes must be an integer"):
        LineStreamerConfig(max_line_bytes="8192")  # type: ignore[arg-type]