"""
ziplogstream.errors
==================

Custom exception hierarchy for the ziplogstream package.

Overview
--------
This module defines all package-specific exceptions used across the
ziplogstream codebase. Exposing a dedicated exception hierarchy allows
callers to reliably catch errors originating from this library without
interfering with unrelated exceptions.

Design principles
-----------------
- All package exceptions inherit from :class:`ZipLogStreamError`
- Specialized errors represent distinct failure categories
- Error classes are stable parts of the public API
"""


class ZipLogStreamError(Exception):
    """
    Base exception for all errors raised by the ziplogstream package.

    Users who want to catch *any* error originating from this library
    can catch this base class.
    """


class ConfigurationError(ZipLogStreamError):
    """
    Raised when configuration validation fails.

    This typically occurs during initialization of configuration models
    such as :class:`LineStreamerConfig`.
    """


class ZipValidationError(ZipLogStreamError):
    """
    Raised when a ZIP archive fails structural validation.

    Examples include:
    - archive file does not exist
    - file does not have a `.zip` suffix
    - archive cannot be opened
    """


class ZipMemberNotFoundError(ZipLogStreamError):
    """
    Raised when no archive member matches the requested target selector.
    """


class ZipMemberAmbiguityError(ZipLogStreamError):
    """
    Raised when multiple archive members match the requested selector
    and the resolver cannot determine a unique result.
    """


__all__ = [
    "ZipLogStreamError",
    "ConfigurationError",
    "ZipValidationError",
    "ZipMemberNotFoundError",
    "ZipMemberAmbiguityError",
]