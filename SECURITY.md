# Security Policy

## Supported Versions

| Version | Supported |
| --- | --- |
| 0.1.x | Yes |
| < 0.1.0 | No |

The first public release is **v0.1.0**. Treat earlier commits as pre-release.

## Reporting a Vulnerability

Please report security issues privately. Do not open a public GitHub issue for
vulnerabilities that could affect cache confidentiality, integrity, or
availability.

Preferred channel: open a private [GitHub Security Advisory](https://github.com/EyJunge1/go-memcached/security/advisories/new)
against this repository, or email the maintainer listed in the repository profile.

Include:

- affected version / commit
- reproduction steps or a minimal PoC
- impact (e.g. connection desync, credential exposure, DoS)

We aim to acknowledge reports within 7 days and to ship a fix or mitigation
advisory for confirmed issues in supported versions as quickly as practical.

## Scope Notes

- Default TCP is plaintext. Use `Transport.TLS` / `RequireTLS` for confidentiality.
- UDP provides no authenticity or integrity; IDs are correlation only.
- A custom `Transport.Dial` is a caller trust boundary for Auth plaintext policy.
- Unix socket mode checks are best-effort before connect.
