"""
tests.conftest
==============

Shared pytest fixtures and helpers for zipstreamer tests.

Overview
--------
This module provides reusable helpers for building temporary ZIP archives
and in-memory binary streams used across both unit and integration tests.

Design goals
------------
- avoid committed binary fixture files
- keep test setup explicit and readable
- centralize common archive construction helpers
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Callable

import pytest


@pytest.fixture
def make_zip(tmp_path: Path) -> Callable[[str, dict[str, bytes]], Path]:
    """
    Build a temporary ZIP archive from an in-memory member mapping.

    Args:
        tmp_path:
            Pytest temporary directory fixture.

    Returns:
        A helper function that creates a ZIP file and returns its path.

    The returned helper accepts:
    - zip_name: output archive filename
    - members: mapping of archive member name -> raw bytes content
    """

    def _make_zip(zip_name: str, members: dict[str, bytes]) -> Path:
        zip_path = tmp_path / zip_name
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for member_name, payload in members.items():
                zf.writestr(member_name, payload)
        return zip_path

    return _make_zip


@pytest.fixture
def make_text_zip(
    tmp_path: Path,
) -> Callable[[str, dict[str, str], str], Path]:
    """
    Build a temporary ZIP archive from text members.

    Returns:
        A helper function that creates a ZIP file and returns its path.

    The returned helper accepts:
    - zip_name: output archive filename
    - members: mapping of archive member name -> text content
    - encoding: text encoding used to encode member content
    """

    def _make_text_zip(
        zip_name: str,
        members: dict[str, str],
        encoding: str = "utf-8",
    ) -> Path:
        zip_path = tmp_path / zip_name
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for member_name, text in members.items():
                zf.writestr(member_name, text.encode(encoding))
        return zip_path

    return _make_text_zip


@pytest.fixture
def make_bytes_stream() -> Callable[[bytes], io.BytesIO]:
    """
    Return a helper that creates an in-memory binary stream.
    """

    def _make_bytes_stream(payload: bytes) -> io.BytesIO:
        return io.BytesIO(payload)

    return _make_bytes_stream