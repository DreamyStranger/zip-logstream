"""
tests.unit.package.test_logging
===============================

Unit tests for :mod:`zipstreamer.logging`.
"""

from __future__ import annotations

import logging

from ziplogstream.logging import PACKAGE_LOGGER_NAME, get_logger


def test_get_logger_returns_package_logger_by_default() -> None:
    """
    Ensure the default logger helper returns the package root logger.
    """
    logger = get_logger()

    assert isinstance(logger, logging.Logger)
    assert logger.name == PACKAGE_LOGGER_NAME


def test_get_logger_returns_named_logger_when_name_is_provided() -> None:
    """
    Ensure an explicit logger name is returned unchanged.
    """
    logger = get_logger("zipstreamer.streaming.line_streamer")

    assert isinstance(logger, logging.Logger)
    assert logger.name == "zipstreamer.streaming.line_streamer"