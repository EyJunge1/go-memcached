# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

- Benchmark competitor client: `github.com/yeqown/memcached` 1.5.0→1.5.1 in `/benchmark` (#2). Patch fixes invalid meta-set mode tokens (`Madd`/`Mset` → protocol singles `E`/`A`/`P`/`R`/`S`).

### Notes

- Library modules (`memcached`, `memcachedotel`) were unchanged; no new module release tag is needed for this bump.
