"""
ziplogstream.logging
===================

Logging helpers for the ziplogstream package.

Overview
--------
This module provides package-level logger access without imposing logging
configuration on library consumers.

Library policy
--------------
`ziplogstream` does not configure logging handlers itself. Applications
using the library are expected to configure Python logging according to
their own needs.

This module exists mainly to provide a single canonical package logger
name and a small helper for internal use.
"""

from __future__ import annotations

import logging

PACKAGE_LOGGER_NAME = "ziplogstream"


def get_logger(name: str | None = None) -> logging.Logger:
    """
    Return a logger for the package or one of its submodules.

    Args:
        name:
            Optional fully qualified logger name. If omitted, the package
            root logger is returned.

    Returns:
        A standard library `logging.Logger` instance.
    """
    return logging.getLogger(name or PACKAGE_LOGGER_NAME)