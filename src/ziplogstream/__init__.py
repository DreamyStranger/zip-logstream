"""
ziplogstream
===========

Streaming UTF-8 and text lines directly from files inside ZIP archives.

Overview
--------
`ziplogstream` is a focused library for bounded-memory, streaming reads of
text files stored inside ZIP archives. It is designed for large log-like
inputs where full extraction is undesirable and a single-pass iteration
model is preferred.

Public API
----------
The package exposes:
- `LineStreamer` as the primary high-level entry point
- `LineStreamerConfig` for streaming configuration
- `default_zip_member_resolver` for deterministic member selection
- package-specific exception types for explicit error handling
"""

from __future__ import annotations

from .archive.member_resolution import default_zip_member_resolver
from .config.streamer_config import LineStreamerConfig
from .errors import (
    ConfigurationError,
    ZipLogStreamError,
    ZipMemberAmbiguityError,
    ZipMemberNotFoundError,
    ZipValidationError,
)
from .streaming.line_streamer import LineStreamer
from .version import __version__

__all__ = [
    "__version__",
    "LineStreamer",
    "LineStreamerConfig",
    "default_zip_member_resolver",
    "ZipLogStreamError",
    "ConfigurationError",
    "ZipValidationError",
    "ZipMemberNotFoundError",
    "ZipMemberAmbiguityError",
]