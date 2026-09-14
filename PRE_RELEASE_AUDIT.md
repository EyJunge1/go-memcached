# Pre-Release Audit: gomemcached

## 1. Executive Summary

**Gesamturteil:** Der Pre-Release-Audit und die erneute adversariale Nachprüfung
haben einen P1-Lifecycle-Blocker, zwei P2-Defekte sowie sekundäre Pool-, Parser-,
Input-, API-, Security- und Release-Lücken gefunden. Alle bestätigten Defekte
wurden im Working Tree behoben und mit Regressionstests abgesichert. Restrisiken
(UDP-Spoofing, Plaintext-Default und dokumentierter Close-Vertrag für
uncancellable Caller-I/O) bleiben bewusst akzeptiert.

**Release-Empfehlung:** **RELEASE WITH ACCEPTED RISKS** für **v0.1.0**
(nicht v1.0.0), sobald dieses Working Tree getaggt wird.

| Severity | Offen | Behoben in diesem Audit |
|---|---:|---:|
| P0 | 0 | 0 |
| P1 | 0 | 1 |
| P2 | 0 | 2 |
| P3 | 0 | Doku/Hardening/Coverage geschlossen |

Wichtigste behobene Blocker:

1. **LIFE-001:** Pool-Cleaner/`ensureMinIdle` Dial bei `TimeoutNone` hängt nicht mehr nach Close.
2. **POOL-001:** `warmup` respektiert `MaxOpen` unter parallelem `get`.
3. **UDP-001:** Request-ID-Wrap rotiert jeden UDP-Pfad vor Send/Recv auf einen
   frischen Socket.

## 2. Scope and Methodology

- Repository: `/Users/arneleder/Development/gomemcached`
- Basis-Commit: `66c06f09ee2b2296d29956f437660436136b93ca`
- Working Tree: enthält die Remediations dieses Audits (uncommitted)
- Go: `go1.27.1 darwin/arm64`; Modul `github.com/EyJunge1/go-memcached`
- Methodik: Baseline → acht parallele Spezialaudits → zwei adversariale Reviewer →
  gezielte Verifikation → Fixes → erneute Verifikation
- Agents: `cursor-grok-4.5-high-fast`, read-only Audits; Fixes nur durch den
  koordinierenden Agenten
- Livetest gegen lokale Memcached **1.6.45** (TCP, UDP und Unix)

### Ausgeführte Kommandos (Nach-Fix)

| Kommando | Exit | Ergebnis |
|---|---:|---|
| `go test ./...` | 0 | OK |
| `go test -race ./memcached` | 0 | OK |
| `go vet ./...` | 0 | clean |
| `golangci-lint run ./...` | 0 | 0 issues |
| `govulncheck ./...` | 0 | No vulnerabilities found |
| kritische Race-Gruppe `-count=20` | 0 | OK |
| Fuzz-Smokes je 30s (4 Targets) | 0 | PASS |
| lokale Livetests `-race -count=1` | 0 | TCP, UDP und Unix OK |
| ausgewählte Hotpath-Benchmarks `-benchmem -count=3` | 0 | 0 B/op, 0 allocs/op |

## 3. Existing Strengths

- Getrennte Security-Gates (`AllowAdmin` / `AllowFlushAll` / `AllowSetServers`)
- TLS wrappt auch custom Dial; `RequireTLS` / `AllowInsecureTLS` / TLS 1.2 Default
- Length-Framing für Values; Text-Key- und Auth-Control-Gates
- Trailing-Byte-, Line-, Stats- und UDP-Assembly-Budgets
- Persistent-Fanout Close ohne Enqueue-under-lock-Deadlock
- Pool-Waiter-Wakeup nach `discard` / no-idle `put`
- MIT, SECURITY.md, Changelog, SHA-gepinnte CI

## 4. Release Blockers

Keine offenen P0/P1 nach Remediation.

### Behoben: LIFE-001 — Cleaner-Dial bei TimeoutNone

- **Status:** Fixed
- **Ort:** `memcached/pool.go` (`lifeCtx`/`lifeCancel`, `mergeDialContext`,
  `ensureMinIdle`, `connectionsCleaner`)
- **Fix:** Pool besitzt Lifecycle-Context; Close cancelt ihn; Cleaner/Warmup/get
  dialen über den gemergten Context.
- **Test:** `TestConnPoolCloseCancelsCleanerDial`, `TestWarmupStopsWhenPoolClosed`

## 5. Findings

### Lifecycle and Goroutines

#### LIFE-001 — Cleaner/MinIdle-Dial hängt nach Close (P1) — **Fixed**
Siehe Abschnitt 4.

#### LIFE-002/LIFE-004 — Post-Close-Aktivität und Session-Ownership (P3) — **Documented/Tested**
Dedicated Dial recheckt `c.closed` nach Connect; Warmup/MinIdle sind an den
Pool-Lifecycle gebunden. Uncancellable Single-Node-, Ephemeral-Fanout-,
Broadcast-, Session- und Watch/Dump-I/O bleiben dokumentierter Caller-Vertrag.
`Session.Close` nach `Client.Close` gibt den Checkout exakt einmal frei.

#### LIFE-003 — Pool-Close-Race falsch klassifiziert (P3) — **Fixed**
`errPoolClosed` während Client-Shutdown wird als `ErrClosed`, nicht als
`ErrTransport`, veröffentlicht.

### Connection Pool and Ownership

#### POOL-001 — Warmup überschreitet MaxOpen (P2) — **Fixed**
Gemeinsame `reserveOpenSlotLocked()` für `get`/`ensureMinIdle`/`warmup`;
`MinIdle > MaxOpen` wird in `validateConfig` abgelehnt.
Test: `TestWarmupRespectsMaxOpenUnderParallelGet`.

`MinIdle > 0` zusammen mit deaktiviertem Idle-Pool (`MaxIdle < 0`) wird
abgelehnt; `warmup`/Cleaner haben zusätzlich einen defensiven No-op-Guard.

#### POOL-002/POOL-005 — Checkout-Ownership und Put/Close-Race (P3) — **Fixed**
Checkout-Pfade enden genau einmal in `put` oder `discard`. Ein paralleles
`put`/`close` drainiert nach dem Enqueue erneut, sodass kein FD im geschlossenen
Idle-Channel verbleibt. Tests: `TestConcurrentPutAndCloseDrainsIdle`,
`TestSessionCloseAfterClientCloseReleasesCheckout`.

#### POOL-003 — Staler `addr.pool` nach Remove (P3) — **Fixed**
`applyServers` cleared Pool-Pointer entfernter Nodes; `getConn` fällt bei
geschlossenem Pool auf Membership-Check zurück.

#### POOL-004 — `release` ohne Pool (P3) — **Fixed**
Nil-Pool → `closeSilent`.

### Parser and Wire Desynchronization

#### PARSE-001 — Meta-Wave Teilerfolg nach Fehler (P3/API) — **Fixed**
`ErrMalformedResponse` und Transportfehler invalidieren die gesamte Node-Wave;
CLIENT_ERROR/SERVER_ERROR dürfen bereits zugeordnete Siblings behalten. Tests:
`TestMetaPipelineMalformedClearsSiblingSuccess`,
`TestMetaPipelineTransportFailureClearsSiblingSuccess`,
`TestMetaPipelineChargeKeepsSiblingHD`.

#### PARSE-002 — Duplicate VALUE Last-Wins (P3 Hardening) — **Fixed**
Zweite VALUE für denselben bereits gefüllten Slot → Malformed + Invalidate.
Ungeordnete Antworten und bewusst doppelt angefragte Keys bleiben unterstützt.
Tests: `TestGetRejectsDuplicateVALUE`,
`TestRetrievalSupportsUnorderedAndRepeatedRequestedKeys`.

#### PARSE-003 — Unbekanntes/fehlendes Meta-Opaque (P3) — **Fixed**
Reguläre Antwort mit fremdem oder fehlendem Opaque → Malformed. Nur die reine
`MN`-Barrier bleibt Quiet-Erfolg. Tests:
`TestMetaPipelineRejectsUnknownResponseOpaque`,
`TestMetaPipelineRejectsMissingResponseOpaque`.

#### PARSE-004/PARSE-005/PARSE-006 — Sekundäres Parser-Hardening — **Closed**
Unbekannte Meta-Flags bleiben dokumentiert forward-compatible; bekannte
ungültige numerische Debug-Felder sind malformed. Discard-Pfade dürfen den Body
bei ohnehin zu verwerfender Verbindung bewusst nicht drainieren.

### Request Injection

#### INJ-001..INJ-007 — Input- und Framing-Hardening — **Fixed/Documented**
Namespaces lassen mindestens ein Byte Key-Budget frei; Auth und Namespaces
weisen ASCII-Controls ab; P/L-Hints sind auf 4 KiB begrenzt; Serverfehlertext
ist auf 4 KiB begrenzt; High-Bit-Keybytes sind dokumentiert opaque; Auth-Probes
schlagen ohne CSPRNG fail-closed fehl. Adversariale CRLF-/Command-ähnliche
Values wurden über Store, PreparedBatch und PreparedMetaBatch roundtrip-getestet.

### UDP

#### UDP-001 — Epoch erst nach Wrap-Request (P2) — **Fixed**
Nach `enableUDPFraming` rotiert `rotateUDPConnIfStale` vor Send/Recv sowohl im
normalen Dispatch als auch in `VersionAll`/Broadcast-Pfaden. Der Checkout-
Ownership-Zeiger verhindert Doppel-Release beim Austausch.
Tests: `TestUDPWrapRequestRotatesBeforeRecv`,
`TestVersionAllRotatesUDPBeforeWrappedIDReceive`.

#### UDP-002..UDP-005 — Datagramm-Hardening — **Closed**
Fremde/stale IDs werden verworfen, Fragmentgrenzen sind geprüft, Multi-Fragment-
Reassembly (inklusive Reihenfolge und widersprüchlichem `total`) wird gefuzzt,
und Meta-Side-Effect-Flags N/R/T/E werden auf UDP abgewiesen.

Prinzipbedingte UDP-Limits (keine Authentizität/Integrität) bleiben akzeptiert.

### Security Gates and Transport

#### SEC-001 Unix-Symlink Mode-Gate (P3) — **Fixed**
`checkUnixSocketMode` lehnt Symlinks auch bei `UnixSocketAnyMode` ab.
Direkte World-writable-Sockets benötigen den expliziten Opt-in; der lokale
TOCTOU-Rest bleibt dokumentiert.

#### SEC-002/SEC-003/SEC-004 — Auth/TLS/API-Gates (P3) — **Fixed**
Der Standard-Dialer verifiziert vor Plaintext-Auth den tatsächlich verbundenen
Loopback-Peer; Custom Dial bleibt dokumentierte Trust Boundary. Auth-Fehler
leaken das Passwort nicht. Broadcast-Admin exponiert kein `Shutdown`.
Explizite TLS-Versionen unter TLS 1.2 werden abgelehnt.

### Public API and Documentation

#### API-001 Quiet GoDoc / API-002 PreparedMetaBatch.Close — **Fixed**
Quiet dokumentiert CLIENT_ERROR und SERVER_ERROR; Meta-Close dokumentiert Running.

#### API-003..API-008 — Sekundäre API-Verträge — **Fixed/Documented**
Meta-Transportfehler löschen die Node-Wave; nil/zero `MetaClient` liefert
`ErrClosed`; Quiet-Verträge sind vereinheitlicht; identisches `SetServers` ist
ein No-op ohne Session-Invalidierung; Resolver-Refresh veröffentlicht nach Close
kein `ErrClosed`; Sessions sind node-sticky und bleiben caller-owned.

#### RELEASE-001 Workflow `--latest` — **Fixed**
Release ruft den vollständigen wiederverwendbaren Check-Workflow auf, prüft den
Changelog-Abschnitt und setzt `--latest` nur für den höchsten stabilen SemVer-
Tag. Release Notes werden aus GitHub generiert; Changelog und Audit sind
vorhanden.

## 6. Cross-Finding Interactions

- Close + Cleaner + TimeoutNone: durch LIFE-001 geschlossen.
- Warmup + paralleles Get: durch POOL-001 geschlossen.
- UDP Wrap + stale Datagramm: durch UDP-001 geschlossen.
- Meta Malformed/Transport + Sibling-Status: fail-closed umgesetzt.
- Nur die Quiet-MN-Barrier ohne Opaque bleibt protokollgemäß erfolgreich;
  reguläre Antworten ohne/fremdes Opaque nicht.

## 7. Test and Fuzzing Gaps

Geschlossen bzw. ergänzt:

- `TestConnPoolCloseCancelsCleanerDial`
- `TestWarmupRespectsMaxOpenUnderParallelGet` / `TestWarmupStopsWhenPoolClosed`
- `TestUDPWrapRequestRotatesBeforeRecv`
- `TestVersionAllRotatesUDPBeforeWrappedIDReceive`
- `TestMetaPipelineRejectsUnknownResponseOpaque`
- `TestMetaPipelineRejectsMissingResponseOpaque`
- `TestMetaPipelineMalformedClearsSiblingSuccess`
- `TestMetaPipelineTransportFailureClearsSiblingSuccess`
- `TestGetRejectsDuplicateVALUE`
- `TestRetrievalSupportsUnorderedAndRepeatedRequestedKeys`
- `TestValidNamespaceRejectsControls` / `TestValidMetaHintRejectsOverlong`
- `TestUnixSocketSymlinkRejected` / TLS- und Auth-Peer-Tests
- `TestConcurrentPutAndCloseDrainsIdle` / Session-Close-Ownership
- adversariale Value-Framing-Roundtrips
- Fuzz-Seeds/Invarianten für Duplicate VALUE, Meta-Opaque, Line-Limit und
  Multi-Fragment-UDP

Fuzz-Smoke (je ~30s): alle vier Targets PASS.

## 8. Commands and Verification Results

Siehe Tabelle in Abschnitt 2. Zusätzlich:

- kritische Suite `-race -count=20`: PASS
- Lokale Livetests gegen Memcached 1.6.45: PASS (TCP, UDP und Unix)

## 9. Release Checklist

- [x] Keine offenen P0/P1
- [x] P2 bewertet und behoben
- [x] Race-Suite grün
- [x] Lifecycle-/Leak-Regressionstests grün
- [x] Parser-Fuzz-Smoke grün
- [x] Security-Gates geprüft
- [x] API-Ownership dokumentiert
- [x] SECURITY.md vorhanden
- [x] Changelog vorhanden
- [x] SemVer-Tag entschieden: **v0.1.0** (Tag noch zu setzen)

## 10. Recommended Fix Order

Umgesetzt in dieser Reihenfolge:

1. Pool-Lifecycle-Context (LIFE-001)
2. MaxOpen-Slotreservation / Warmup (POOL-001)
3. UDP-Epoch vor Wrap-Request (UDP-001)
4. Parser-Policies (PARSE-001..003)
5. Input-/Unix-/Auth-Härtung
6. API-GoDoc + Release-Workflow + Changelog
7. Verifikation und dieser Report

## 11. Appendix

### Verworfene False Positives

- Stale `single.Load` bei 1→N SetServers (Snapshot-Semantik)
- Meta Quiet mit reinem `MN` als Client-Bug
- Watch/Dump über Close als Leak (Caller-Context-Vertrag)
- TimeoutNone Single-Node-I/O muss durch Close enden (bewusst nicht)
- Failover RetryAfter=DeadAfter=0 / RetryAttempts=1 / sticky Sessions
- Fehlender Tag/privates Repo als Software-P1
- Value-Command-Injection (Length-Framing)
- Custom Dial umgeht TLS; Admin/Flush/SetServers Gate-Bypass

### Bekannte akzeptierte Limits

- UDP ohne Authentizität/Integrität
- TCP-Default Plaintext
- Close beendet kein uncancellable ausgechecktes Single-Node-, Ephemeral-
  Fanout-, Broadcast- oder Session-I/O
- Unix Mode-Check bleibt TOCTOU gegenüber lokalem Filesystem-Angreifer

### Remediations Applied

| ID | Status |
|---|---|
| LIFE-001 | fixed |
| LIFE-002/004 | documented / tested |
| LIFE-003 | fixed |
| POOL-001 | fixed |
| POOL-002/005 | fixed |
| POOL-003 | fixed |
| POOL-004 | fixed |
| UDP-001..005 | fixed / tested |
| PARSE-001 | fixed |
| PARSE-002 | fixed |
| PARSE-003 | fixed |
| PARSE-004..006 | fixed / documented |
| INJ-001..007 | fixed / documented |
| SEC-001..004 | fixed / documented |
| API-001..008 | fixed / documented |
| RELEASE-001..003 | fixed / documented |

**Endgültige Entscheidung: RELEASE WITH ACCEPTED RISKS (v0.1.0)**
