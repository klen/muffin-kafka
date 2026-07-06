# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.1.0] - 2026-07-06

### Added

- Test setup infrastructure.

## [3.0.0] - 2026-06-24

### Changed

- **Breaking:** Drop Python 3.10 support, require Python 3.11+.

## [2.3.0] - 2026-06-15

### Added

- `setup_commands` config option to control management command registration (`KafkaPlugin`).

## [2.2.0] - 2026-05-13

### Documentation

- Add `kafka-listen` command section and fix CLI examples in README.

## [2.1.7] - 2026-05-13

### Fixed

- Use `monitor_interval` config for logger interval instead of hardcoded value.

## [2.1.6] - 2026-05-13

### Added

- Test verifying `batch_size` coercion from string in management command.

## [2.1.5] - 2026-05-13

### Added

- Test verifying pool starts before creating consumer tasks.

## [2.1.2] - 2026-05-13

### Added

- Internal improvements.

## [2.1.1] - 2026-05-13

### Fixed

- Merge setup params into `init_consumer` in pool.

## [2.1.0] - 2026-05-13

### Added

- Manual offset commit support in runner when `auto_commit` is disabled.

## [2.0.0] - 2026-05-13

### Changed

- **Breaking:** Extract consumers into submodules and introduce `PoolRunner`.
- Update API documentation for new consumers package.

## [1.0.3] - 2026-03-26

### Fixed

- Fallback to configured consumer `group_id` on listen.

## [1.0.2] - 2026-03-25

### Fixed

- `kafka-listen` command.

## [1.0.1] - 2026-03-25

### Fixed

- GitHub Actions CI configuration.

## [1.0.0] - 2026-03-25

### Changed

- Migrate tooling to uv.
- Expand Kafka plugin test coverage.

## [0.9.3] - 2026-03-25

### Fixed

- Ensure consumers start asynchronously.

## [0.9.2] - 2026-03-25

### Changed

- Convert `init_consumers` to asynchronous method.
- Add logging to consumer initialization.

## [0.9.1] - 2026-03-25

### Changed

- Update `init_consumers` to accept additional keyword arguments.

## [0.9.0] - 2026-03-25

### Changed

- Update `KafkaPlugin` to use `defaultdict` for handlers.

## [0.8.1] - 2026-03-25

### Added

- Kafka healthcheck management command.

## [0.8.0] - 2026-03-25

### Added

- Kafka healthcheck integration in plugin.

## [0.7.4] - 2026-03-25

### Changed

- Update ASGI Tools dependency to version 1.3.2.
- Update pre-commit hooks and dependencies.

## [0.7.3] - 2026-03-25

### Changed

- Internal tooling improvements (Makefile, pre-commit).

## [0.7.2] - 2026-03-25

### Documentation

- Update README with detailed usage and configuration.

## [0.7.0] - 2026-03-25

### Changed

- Update Python version support to 3.10 and later.

## [0.6.0] - 2025-05-07

### Added

- Python 3.13 support.

### Fixed

- Compatibility fixes for Python 3.13.

## [0.5.2] - 2024-10-18

### Fixed

- Message `send` method.

## [0.5.1] - 2024-08-01

### Added

- Enable LZ4 compression by default.

## [0.5.0] - 2024-07-31

### Added

- Python 3.12 support.

## [0.4.0] - 2024-07-05

### Added

- Internal improvements and features.

## [0.3.3] - 2024-06-21

### Changed

- Build and tooling updates.

## [0.3.2] - 2024-06-21

### Fixed

- Bug fixes and improvements.

## [0.3.1] - 2024-06-21

### Fixed

- Bug fixes and improvements.

## [0.3.0] - 2024-06-21

### Added

- New features and improvements.

## [0.2.8] - 2024-06-21

### Changed

- Tune logging.

## [0.2.7] - 2024-05-28

### Fixed

- Dependencies.

## [0.2.6] - 2024-03-27

### Fixed

- Bug fixes and improvements.

## [0.2.5] - 2024-03-27

### Added

- New features and improvements.

## [0.2.4] - 2024-01-29

### Changed

- Update monitor settings.

## [0.2.3] - 2023-12-13

### Fixed

- Plugin fixes.

## [0.2.2] - 2023-12-13

### Added

- More logging.

## [0.2.1] - 2023-12-11

### Added

- Features and improvements.

## [0.2.0] - 2023-12-08

### Added

- Monitor feature.

## [0.1.2] - 2023-12-08

### Fixed

- Bug fixes and improvements.

## [0.1.1] - 2023-12-08

### Added

- Features and improvements.

## [0.1.0] - 2023-11-28

### Added

- Features and improvements.

## [0.0.22] - 2023-11-28

### Fixed

- Bug fixes and improvements.

## [0.0.21] - 2023-11-28

### Added

- `send_and_wait` method.

## [0.0.20] - 2023-11-27

### Fixed

- Default values.

## [0.0.19] - 2023-11-24

### Added

- Features and improvements.

## [0.0.18] - 2023-11-16

### Fixed

- Bug fixes and improvements.

## [0.0.17] - 2023-11-15

### Fixed

- Bug fixes and improvements.

## [0.0.16] - 2023-11-15

### Fixed

- Bug fixes and improvements.

## [0.0.15] - 2023-11-15

### Fixed

- Type hints.

## [0.0.14] - 2023-11-15

### Fixed

- Bug fixes and improvements.

## [0.0.13] - 2023-11-07

### Added

- Features and improvements.

## [0.0.12] - 2023-11-07

### Fixed

- Bug fixes and improvements.

## [0.0.11] - 2023-11-02

### Fixed

- Bug fixes and improvements.

## [0.0.10] - 2023-11-02

### Fixed

- Bug fixes and improvements.

## [0.0.9] - 2023-11-02

### Added

- Features and improvements.

## [0.0.8] - 2023-11-02

### Added

- Features and improvements.

## [0.0.7] - 2023-11-02

### Added

- Features and improvements.

## [0.0.6] - 2023-11-02

### Fixed

- Package configuration.

## [0.0.5] - 2023-11-02

### Fixed

- README.

## [0.0.4] - 2023-11-02

### Changed

- Build tuning.

## [0.0.3] - 2023-11-02

### Changed

- Build improvements.

## [0.0.2] - 2023-11-02

### Added

- Initial release.

[3.1.0]: https://github.com/klen/muffin-kafka/compare/3.0.0...3.1.0
[3.0.0]: https://github.com/klen/muffin-kafka/compare/2.3.0...3.0.0
[2.3.0]: https://github.com/klen/muffin-kafka/compare/2.2.0...2.3.0
[2.2.0]: https://github.com/klen/muffin-kafka/compare/2.1.7...2.2.0
[2.1.7]: https://github.com/klen/muffin-kafka/compare/2.1.6...2.1.7
[2.1.6]: https://github.com/klen/muffin-kafka/compare/2.1.5...2.1.6
[2.1.5]: https://github.com/klen/muffin-kafka/compare/2.1.2...2.1.5
[2.1.2]: https://github.com/klen/muffin-kafka/compare/2.1.1...2.1.2
[2.1.1]: https://github.com/klen/muffin-kafka/compare/2.0.0...2.1.1
[2.1.0]: https://github.com/klen/muffin-kafka/compare/2.0.0...2.1.0
[2.0.0]: https://github.com/klen/muffin-kafka/compare/1.0.3...2.0.0
[1.0.3]: https://github.com/klen/muffin-kafka/compare/1.0.2...1.0.3
[1.0.2]: https://github.com/klen/muffin-kafka/compare/1.0.1...1.0.2
[1.0.1]: https://github.com/klen/muffin-kafka/compare/1.0.0...1.0.1
[1.0.0]: https://github.com/klen/muffin-kafka/compare/0.6.0...1.0.0
[0.9.3]: https://github.com/klen/muffin-kafka/compare/0.9.2...0.9.3
[0.9.2]: https://github.com/klen/muffin-kafka/compare/0.9.1...0.9.2
[0.9.1]: https://github.com/klen/muffin-kafka/compare/0.9.0...0.9.1
[0.9.0]: https://github.com/klen/muffin-kafka/compare/0.8.1...0.9.0
[0.8.1]: https://github.com/klen/muffin-kafka/compare/0.8.0...0.8.1
[0.8.0]: https://github.com/klen/muffin-kafka/compare/0.7.4...0.8.0
[0.7.4]: https://github.com/klen/muffin-kafka/compare/0.7.3...0.7.4
[0.7.3]: https://github.com/klen/muffin-kafka/compare/0.7.2...0.7.3
[0.7.2]: https://github.com/klen/muffin-kafka/compare/0.7.0...0.7.2
[0.7.0]: https://github.com/klen/muffin-kafka/compare/0.6.0...0.7.0
[0.6.0]: https://github.com/klen/muffin-kafka/compare/0.5.2...0.6.0
[0.5.2]: https://github.com/klen/muffin-kafka/compare/0.5.1...0.5.2
[0.5.1]: https://github.com/klen/muffin-kafka/compare/0.5.0...0.5.1
[0.5.0]: https://github.com/klen/muffin-kafka/compare/0.4.0...0.5.0
[0.4.0]: https://github.com/klen/muffin-kafka/compare/0.3.3...0.4.0
[0.3.3]: https://github.com/klen/muffin-kafka/compare/0.3.2...0.3.3
[0.3.2]: https://github.com/klen/muffin-kafka/compare/0.3.1...0.3.2
[0.3.1]: https://github.com/klen/muffin-kafka/compare/0.3.0...0.3.1
[0.3.0]: https://github.com/klen/muffin-kafka/compare/0.2.8...0.3.0
[0.2.8]: https://github.com/klen/muffin-kafka/compare/0.2.7...0.2.8
[0.2.7]: https://github.com/klen/muffin-kafka/compare/0.2.6...0.2.7
[0.2.6]: https://github.com/klen/muffin-kafka/compare/0.2.5...0.2.6
[0.2.5]: https://github.com/klen/muffin-kafka/compare/0.2.4...0.2.5
[0.2.4]: https://github.com/klen/muffin-kafka/compare/0.2.3...0.2.4
[0.2.3]: https://github.com/klen/muffin-kafka/compare/0.2.2...0.2.3
[0.2.2]: https://github.com/klen/muffin-kafka/compare/0.2.1...0.2.2
[0.2.1]: https://github.com/klen/muffin-kafka/compare/0.2.0...0.2.1
[0.2.0]: https://github.com/klen/muffin-kafka/compare/0.1.2...0.2.0
[0.1.2]: https://github.com/klen/muffin-kafka/compare/0.1.1...0.1.2
[0.1.1]: https://github.com/klen/muffin-kafka/compare/0.1.0...0.1.1
[0.1.0]: https://github.com/klen/muffin-kafka/compare/0.0.22...0.1.0
[0.0.22]: https://github.com/klen/muffin-kafka/compare/0.0.21...0.0.22
[0.0.21]: https://github.com/klen/muffin-kafka/compare/0.0.20...0.0.21
[0.0.20]: https://github.com/klen/muffin-kafka/compare/0.0.19...0.0.20
[0.0.19]: https://github.com/klen/muffin-kafka/compare/0.0.18...0.0.19
[0.0.18]: https://github.com/klen/muffin-kafka/compare/0.0.17...0.0.18
[0.0.17]: https://github.com/klen/muffin-kafka/compare/0.0.16...0.0.17
[0.0.16]: https://github.com/klen/muffin-kafka/compare/0.0.15...0.0.16
[0.0.15]: https://github.com/klen/muffin-kafka/compare/0.0.14...0.0.15
[0.0.14]: https://github.com/klen/muffin-kafka/compare/0.0.13...0.0.14
[0.0.13]: https://github.com/klen/muffin-kafka/compare/0.0.12...0.0.13
[0.0.12]: https://github.com/klen/muffin-kafka/compare/0.0.11...0.0.12
[0.0.11]: https://github.com/klen/muffin-kafka/compare/0.0.10...0.0.11
[0.0.10]: https://github.com/klen/muffin-kafka/compare/0.0.9...0.0.10
[0.0.9]: https://github.com/klen/muffin-kafka/compare/0.0.8...0.0.9
[0.0.8]: https://github.com/klen/muffin-kafka/compare/0.0.7...0.0.8
[0.0.7]: https://github.com/klen/muffin-kafka/compare/0.0.6...0.0.7
[0.0.6]: https://github.com/klen/muffin-kafka/compare/0.0.5...0.0.6
[0.0.5]: https://github.com/klen/muffin-kafka/compare/0.0.4...0.0.5
[0.0.4]: https://github.com/klen/muffin-kafka/compare/0.0.3...0.0.4
[0.0.3]: https://github.com/klen/muffin-kafka/compare/0.0.2...0.0.3
[0.0.2]: https://github.com/klen/muffin-kafka/releases/tag/0.0.2
