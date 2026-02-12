# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- `CHANGELOG.md`!

## [0.2.2](https://github.com/franciscoabsampaio/spark-connect/releases/tag/v0.2.2) - 2025-02-12

### Changed

- Build script now uses vendored protobuf-compiler, no longer requiring users to manually install it.

## [0.2.1](https://github.com/franciscoabsampaio/spark-connect/releases/tag/v0.2.1) - 2025-01-22

### Added

- Databricks example.

### Changed

- Fix TLS.
- Enforce SSL if `token` header is used.
- Enforce lower case in HTTP headings.

## [0.2.0](https://github.com/franciscoabsampaio/spark-connect/releases/tag/v0.2.0) - 2025-01-10

### Changed

- Standardize protobuf directory to support different versions of Spark.

## [0.1.1](https://github.com/franciscoabsampaio/spark-connect/releases/tag/v0.1.1) - 2025-09-27

### Changed

- Remove explicit dependency on arrow_ipc (use feature instead).

## [0.1.0](https://github.com/franciscoabsampaio/swellow/releases/tag/v0.1.0) - 2025-09-26

- Initial release.
