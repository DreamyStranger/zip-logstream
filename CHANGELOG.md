# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog:
https://keepachangelog.com/

## [0.1.0] - 2026-03-17

### Added
- Initial release of `ziplogstream`
- Streaming line reader for ZIP archive members
- Deterministic ZIP member resolution
- Bounded memory buffering with oversized-line protection
- Configurable decoding and chunk size
- CRLF normalization
- Full unit and integration test suite
- Benchmark and stress testing scripts