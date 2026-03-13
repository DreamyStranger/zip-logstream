"""
ziplogstream.streaming.line_streamer
===================================

High-level ZIP member line streaming interface.

Overview
--------
This module provides the public `LineStreamer` class, which streams
decoded text lines from a target file stored inside a ZIP archive.

The class is intentionally thin and orchestration-focused. It delegates:
- path normalization and validation to `ziplogstream.archive.validators`
- member selection to a resolver
- chunked binary line reading to `BufferedLineReader`

Public contract
---------------
- The target member is streamed directly from the ZIP archive.
- The member is never extracted to disk.
- Decoded lines are yielded one at a time without trailing newline.
- Memory use remains bounded by chunked reads and forced buffer flushes.

Design goals
------------
- Small and predictable public API
- Clear separation between archive concerns and streaming concerns
- Support for custom member resolution strategies
- Bounded-memory operation for very large inputs
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Iterator, cast

from ziplogstream.archive import (
    default_zip_member_resolver,
    normalize_zip_path,
    validate_zip_path,
)
from ziplogstream.config import LineStreamerConfig
from ziplogstream.logging import get_logger
from ziplogstream.streaming import BufferedLineReader
from ziplogstream.types import ZipMemberResolver

logger = get_logger(__name__)


class LineStreamer:
    """
    Stream decoded text lines from a file inside a ZIP archive.

    Args:
        zip_path:
            Path to the ZIP archive.

        target:
            Filename or suffix identifying the desired member inside
            the archive.

        config:
            Optional streaming configuration. If omitted, defaults from
            `LineStreamerConfig` are used.

        resolver:
            Optional ZIP member resolver. If omitted, the package default
            deterministic resolver is used.

    Notes
    -----
    `LineStreamer` is the main public entry point for the library. It
    focuses on orchestration and delegates low-level buffered line reading
    to `BufferedLineReader`.
    """

    def __init__(
        self,
        zip_path: Path | str,
        target: str,
        *,
        config: LineStreamerConfig | None = None,
        resolver: ZipMemberResolver | None = None,
    ) -> None:
        self.zip_path = normalize_zip_path(zip_path)
        self.target = target
        self.config = config or LineStreamerConfig()
        self.resolver = resolver or default_zip_member_resolver

    def stream(self) -> Iterator[str]:
        """
        Stream decoded lines from the resolved ZIP member.

        Yields:
            One decoded line at a time, without the trailing newline
            character. If the input uses CRLF line endings, the trailing
            carriage return is also removed.

        Raises:
            FileNotFoundError:
                If the ZIP archive path does not exist.

            ZipValidationError:
                If the archive path is invalid.

            ZipMemberNotFoundError:
                If the target member cannot be resolved.

            ZipMemberAmbiguityError:
                If the target selector matches multiple members.
        """
        validate_zip_path(self.zip_path)

        with zipfile.ZipFile(self.zip_path, "r") as zf:
            member_name = self.resolver(zf, self.target)
            logger.debug(
                "Streaming ZIP member '%s' from archive '%s'",
                member_name,
                self.zip_path.name,
            )

            with zf.open(member_name, "r") as raw:
                buffered_raw = cast(io.BufferedIOBase, raw)
                reader = BufferedLineReader(buffered_raw, self.config)
                yield from reader.iter_lines()