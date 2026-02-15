# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

- Cloudflare R2 example.

## [0.2.0](https://github.com/franciscoabsampaio/swellow/releases/tag/v0.2.0) - 2025-02-14

### Added

- `CHANGELOG.md`!
- Support for multiple engines and catalogs, namely Apache Spark with Delta and Iceberg catalogs.
- JSON interface with custom errors.
- Secondary lock for concurrency management, and CLI flag `--ignore-locks`.
- Log timestamps.
- Publish Rust binary crate, with API and docs.
- Non-affiliation disclaimer to project README.
- Examples for PostgreSQL and Databricks with Delta.

### Changed

- Extensive refactoring.
- Remove dependency on `testcontainers`, use `docker` package instead.
- Pad migration version directories with 0s.
- Update license to MIT license.
- Switch from SLSA3 provenance to GitHub's native attestation.

### Removed

- `anyhow` in favour of explicit errors and better error handling.
- All [panicking](https://doc.rust-lang.org/rust-by-example/std/panic.html) code.

## [0.1.2](https://github.com/franciscoabsampaio/swellow/releases/tag/v0.1.2) - 2025-09-27

### Changed

- Compile Rust binary with msvc instead of mingw.

## [0.1.1](https://github.com/franciscoabsampaio/swellow/releases/tag/v0.1.1) - 2025-09-26

- Initial release.
