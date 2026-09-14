# Pre-Release Audit: gomemcached

## 1. Executive Summary

**Gesamturteil:** Die im Audit bestätigten P1-Blocker und sicherheitsrelevanten P2-Findings wurden behoben. Lifecycle-, Parser-, Pool- und UDP-Härtungen sind mit Regressionstests und Fuzz-Targets abgesichert. Restrisiken (LIFE-002 Close-Vertrag für uncancellable Single-Node-I/O, UDP Spoofing, Plaintext-Default) sind dokumentiert und akzeptiert.

**Release-Empfehlung:** **READY FOR v0.1.0** (nicht v1.0.0)

| Severity | Offen | Behoben / akzeptiert dokumentiert |
|---|---:|---:|
| P0 | 0 | 0 |
| P1 | 0 | 4 behoben |
| P2 | 0 | 9 behoben oder Vertrag dokumentiert |
| P3 | 0 | Doku/SECURITY/Changelog/Fuzz geschlossen |

Behoben (Auswahl):

1. **POOL-001 / POOL-002:** MaxOpen-Wakeup + SessionPool-Cap
2. **LIFE-001 / LIFE-003 / CLUSTER-001:** Fanout-Close, ErrClosed-Mapping, Membership vor First-Dial
3. **PARSE-001–005 / UDP-001/002:** Restbytes, Zeilen-/Stats-Budgets, Opaque-Seen, UDP-Budget + ID-Epoch-Rotation
4. **LIFE-002:** bewusst als Close-Vertrag dokumentiert (kein Hotpath-Registry)

## Remediations Applied

| ID | Status | Hinweis |
|---|---|---|
| POOL-001 | fixed | `releaseOpen` + `waitCh` |
| POOL-002 | fixed | `size > MaxOpen` → `ErrInvalidArgument` |
| LIFE-001 | fixed | kein blockierender Enqueue unter Mutex |
| LIFE-002 | accepted | GoDoc auf `Client.Close` / package doc |
| LIFE-003 | fixed | `checkoutPool` mappt bei Client-Close |
| PARSE-001 | fixed | `ensureNoTrailingBytes` an Wellengrenzen |
| PARSE-002 | fixed | 64 KiB `protocolLineLimit` |
| PARSE-003 | fixed | 4 MiB `statsResponseBudget` |
| PARSE-004 | fixed | `invalidateRetrievalDest` |
| PARSE-005 | fixed | Meta Opaque Seen-Bitmap |
| CLUSTER-001 | fixed | Membership-Check vor Pool-Create |
| UDP-001 | fixed | Assembly-Budget aus MaxValueBytes |
| UDP-002 | fixed | `udpRequestEpoch` + Conn-Rotation |
| INJ-001 | fixed | Auth Controls inkl. BEL/DEL |
| DOC-*/SEC-*/RELEASE-*/TEST-* | fixed | Ownership-GoDoc, SECURITY.md, CHANGELOG, Fuzz |

## 9. Release Checklist

- [x] Keine offenen P0/P1
- [x] P2 bewertet und behoben oder dokumentiert akzeptiert
- [x] Race-Suite
- [x] Lifecycle-/Leak-Regressionstests
- [x] Parser-Fuzz-Targets vorhanden
- [x] Security-Gates geprüft
- [x] API-Ownership dokumentiert
- [x] SECURITY.md vorhanden
- [x] Changelog vorhanden
- [x] SemVer-Tag entschieden: **v0.1.0**

**Endgültige Entscheidung: READY FOR v0.1.0**

---

## Original Audit Snapshot

The remainder of this file preserves the original audit narrative for history.
The findings listed above as fixed supersede the earlier **DO NOT RELEASE** verdict.

### Original Executive Summary (pre-fix)

**Gesamturteil:** Der Client besitzt eine ungewöhnlich breite Testbasis, klar getrennte Security-Gates und überwiegend saubere Connection-Ownership. Der Audit bestätigt jedoch vier P1-Defekte: zwei deterministische Hänger im Fanout-/Pool-Lifecycle, einen Konstruktions-Hänger bei `SessionPool` und eine mögliche Wiederverwendung protokollwidrig vorbefüllter Stream-Antworten. Diese Pfade sind vor einem öffentlichen Release zu korrigieren.

**Release-Empfehlung:** **DO NOT RELEASE**

| Severity | Anzahl | Confirmed | Plausible | Coverage Gap | Improvement |
|---|---:|---:|---:|---:|---:|
| P0 | 0 | 0 | 0 | 0 | 0 |
| P1 | 4 | 4 | 0 | 0 | 0 |
| P2 | 9 | 7 | 2 | 0 | 0 |
| P3 | 10 | 2 | 0 | 2 | 6 |
| **Gesamt** | **23** | **13** | **2** | **2** | **6** |

Bestätigte Produktionsdefekte und -risiken sind nicht mit bloßen Testlücken vermischt. Die wichtigsten drei Release-Blocker sind:

1. **POOL-001:** MaxOpen-Waiter werden nach `discard` beziehungsweise `MaxIdle <= 0` nicht geweckt.
2. **LIFE-001:** `Client.Close` kann bei gesättigtem persistentem Fanout dauerhaft am RWMutex blockieren.
3. **PARSE-001:** Restbytes nach einer erfolgreich geparsten Einzelantwort können vom nächsten Request übernommen werden.

Zusätzlich blockiert **POOL-002**: `NewSessionPool(size > MaxOpen)` kann ohne Context-Deadline unbegrenzt hängen.

## 2. Scope and Methodology

### Repository und Baseline

- Repository: `/Users/arneleder/Development/gomemcached`
- Commit: `bb8538066508777cd48ad22edb30acbb13a36dd2`
- Branch: `main`
- Working Tree: bei Beginn und nach der Verifikation sauber; es lagen keine uncommitted Änderungen des Nutzers vor.
- Go: `go1.27.1 darwin/arm64`; Modul-Direktive `go 1.27`
- Modul: `github.com/EyJunge1/go-memcached`
- Root-Modul ohne Drittanbieter-Runtime-Dependencies und ohne Root-`go.sum`
- Öffentliche API: mit `go doc -all ./memcached` erfasst; 1.056 Ausgabezeilen. Geprüft wurden sämtliche exportierten Typen, Methoden, Sentinel-Errors und deren GoDoc.

### Vorgehen

1. Baseline von Git, Toolchain, Modul, Workflows, Tests, Dokumentation, Release- und Lint-Konfiguration.
2. Acht unabhängige Spezialaudits parallel: Lifecycle; Pool/Ownership; Parser/Desync; Injection; Cluster/Failover; UDP; Security-Gates/Transport; API/Release-Vertrag.
3. Zwei parallele adversariale Cross-Reviewer: False-Positive-Prüfung und Gap-/Interaktionsprüfung.
4. Manuelle Konsolidierung gegen den tatsächlichen Code; Duplikate und nicht tragfähige Severity-Einstufungen wurden verworfen oder herabgestuft.
5. Begrenzte gezielte Race-Stressläufe sowie TCP-/UDP-Livetests gegen eine temporäre lokale Memcached-Instanz.

Alle zehn Agents liefen read-only mit `cursor-grok-4.5-high-fast`. Nur der koordinierende Hauptagent schrieb diese Datei. Produktionscode und Tests wurden nicht verändert.

### Nicht geprüfte externe Voraussetzungen

- Kein externer, authentifizierter oder TLS-terminierter Memcached wurde angesprochen.
- Kein Unix-Socket-Livetest; der angegebene nicht existente Socket führte zum vorgesehenen Skip.
- Kein realer DNS-Resolver-Failover über längere Zeit.
- Kein aktiver UDP-On-Path-Angreifer und kein Langlauf über 65.535 UDP-Requests.
- Keine Fuzz-Runs: Im Repository existiert kein `func Fuzz...`-Target. Es wurden bewusst keine Testdateien angelegt.
- Keine Dependencies installiert und keine externen Systeme verändert.

## 3. Existing Strengths

- **Security-Gates:** `AllowAdmin`, `AllowFlushAll` und `AllowSetServers` werden getrennt geprüft. `Shutdown` ist nur über `Admin().On(server)` erreichbar; unbekannte Ziele werden abgelehnt.
- **TLS:** Ein custom `Transport.Dial` wird bei gesetztem TLS weiterhin mit `tls.Client` umschlossen. `RequireTLS`, `AllowInsecureTLS`, TLS-Clone, Mindestversion TLS 1.2 und SNI sind implementiert und getestet.
- **Auth:** Token-Auth wird auf UDP und Remote-Plaintext standardmäßig abgelehnt; `SetServers` validiert diese Policy erneut. Der zufällige Auth-Probe-Key und length-framed Credentials verhindern Command-Injection; Passwörter erscheinen nicht in Fehlertexten.
- **Request-Framing:** Text-Keys verwerfen Whitespace, Controls, CR, LF und NUL. Namespace und Key zählen gemeinsam gegen 250 Bytes. Binäre Meta-Keys werden Base64-kodiert und anhand der Wire-Länge begrenzt. Values sind in allen geprüften Storage-, Meta- und Prepared-Pfaden length-framed; eingebettete `CRLF`, `END`, `VALUE` oder `MN` sind keine Injection.
- **Parse-Guards:** VALUE-/VA-Größen werden vor der Allokation über `MaxValueBytes` begrenzt. Integer-Parser prüfen Overflow. Fehlerhafte Body-CRLFs, unerwartete VALUE-Keys und erkannte Parse-/Serverfehler führen zum Connection-Discard.
- **Quiet:** Text-Quiet verwirft die Connection bewusst; Pipeline und Session lehnen unbestimmte Text-Quiet-Pfade ab. Meta-Quiet verwendet eine `mn`-Barriere.
- **Pool-Ownership:** Normale Dispatch-, Broadcast-, Pipeline- und Prepared-Pfade besitzen genau eine Terminalaktion. Connections, die während Pool-Close erzeugt werden, werden geschlossen. Entfernte Idle-Pools, Cleaner und Waiter bei Pool-Close sind explizit getestet.
- **Cluster:** Picker werden als neue Snapshots veröffentlicht; Ketama-Hot-Caches verwenden atomare Pointer. Prepared Batches beachten `clusterVersion` und `healthVersion`. Es wurde keine inverse Lock-Reihenfolge `healthMu -> mu` gefunden.
- **Admin-Streams:** Deadline ist Pflicht, Cancel schließt die Dedicated Connection, Channel-Sends wählen zugleich `ctx.Done()`, und einzelne Stream-Zeilen sind auf 64 KiB begrenzt.
- **UDP:** Header, Reserved-Feld, Fragmentanzahl, Sequence-Bounds, Total-Konsistenz und Fragmentkonflikte werden geprüft. UDP ist auf read-only Commands beschränkt; TLS, Auth, Mutationen, Admin, Pipeline und Session werden abgelehnt.
- **Release/Supply Chain:** MIT-Lizenz vorhanden; GitHub Actions haben minimale Permissions und Actions sind auf vollständige SHAs gepinnt. CI enthält Lint, Race-Coverage, Livetest und govulncheck. Release-Tags werden formal und auf `main`-Abstammung geprüft.

## 4. Release Blockers

### POOL-001 — Freier MaxOpen-Slot weckt blockierte Waiter nicht

- **Severity / Confidence / Status / Release-Blocker:** P1 / high / Confirmed / **ja**
- **Ort / API:** `memcached/pool.go`, `(*connPool).get` Z. 126–169, `discard` Z. 189–196, `put` Z. 199–227; alle gepoolten APIs, besonders Quiet und `Pool.MaxIdle < 0`.
- **Ursache:** Ein Checkout wartet bei `open >= maxOpen` ausschließlich auf `p.conns`, `closedCh` oder `ctx.Done()`. `discard` und die Close-Zweige von `put` reduzieren `open`, senden aber kein Wake-Signal.
- **Interleaving:** `MaxOpen=1`; G1 hält Connection A; G2 wartet in `get`; G1 verwirft A nach Fehler/Quiet oder gibt A bei `MaxIdle=-1` zurück; `open` wird 0, G2 schläft dennoch bis Cancel oder Pool-Close.
- **Auswirkung:** deterministischer Request-Hänger; mit `context.Background()` dauerhaft, unter Quiet-Last systematisch.
- **Schutzmaßnahmen:** `pool.close` weckt über `closedCh`; bestehende Tests prüfen Close-Wakeup und isoliertes `discard`, nicht deren kritische Kombination.
- **Minimale Korrektur:** jeden Übergang `open--` unter `maxOpen` über einen Slot-/Condition-Kanal signalisieren und `get` in einer Recheck-Schleife warten lassen.
- **Regressionstest:** `MaxOpen=1`, zweiter Checkout wartet, erster wird einmal via `discard` und einmal via `MaxIdle=-1`-`put` beendet; zweiter Checkout muss ohne Context-Timeout neu dialen.
- **Verifikation:** statischer Interleaving-Beweis; `go test -race -count=20` der vorhandenen Pool-/Quiet-Tests grün, aber ohne den auslösenden Waiter-Pfad.

### LIFE-001 — Fanout-Sättigung blockiert `Client.Close`

- **Severity / Confidence / Status / Release-Blocker:** P1 / high / Confirmed / **ja**
- **Ort / API:** `memcached/fanout.go`, `(*fanoutPool).run` Z. 63–102 und `close` Z. 105–125; `Client.Close`, Multi-Node-Get/Pipeline/Prepared/Meta bei `Cluster.FanoutWorkers > 0`.
- **Ursache:** `run` hält `mu.RLock` während blockierender Sends auf `p.jobs`. `close` benötigt `mu.Lock`, bevor es `rootCtx` oder aktive Calls canceln kann.
- **Interleaving:** Ein Worker blockiert in Node-I/O; der Buffer `workers*4` wird gefüllt; ein weiterer `run` blockiert beim Send unter RLock; `Close` wartet auf den Write-Lock und erreicht weder `stopRoot` noch `workers.Wait`.
- **Auswirkung:** zuverlässiger Shutdown-Deadlock bei Queue-Sättigung; besonders kritisch mit `TimeoutNone`.
- **Schutzmaßnahmen:** geschlossene Pools werden vor dem Enqueue erkannt; `TestFanoutPoolCloseCancelsActiveWork` deckt nur den nicht gesättigten Pfad.
- **Minimale Korrektur:** keine blockierende Channel-Operation unter dem RWMutex; Close-Signal und Enqueue so trennen, dass Close zuerst canceln kann.
- **Regressionstest:** `FanoutWorkers=1`, blockierendes Node-I/O und mehr als vier wartende Calls; `Close` muss innerhalb einer festen kurzen Frist terminieren.
- **Verifikation:** statischer Lock-/Channel-Beweis; bestehende Fanout-Tests 20-fach unter Race grün, enthalten aber keine volle Queue.

### POOL-002 — `NewSessionPool(size > MaxOpen)` hängt beim Aufbau

- **Severity / Confidence / Status / Release-Blocker:** P1 / high / Confirmed / **ja**
- **Ort / API:** `memcached/client_session_pool.go`, `(*Client).NewSessionPool` Z. 27–52; `memcached/client_session.go`, `(*Client).Session` Z. 25–49; `Client.NewSessionPool`.
- **Ursache:** Jede erzeugte Session hält ihre Connection bis `Close`. Die Schleife erzeugt alle Sessions sequenziell, validiert `size` aber nicht gegen das per-node `MaxOpen`.
- **Interleaving:** `MaxOpen=1`, `size=2`; Session 1 hält den einzigen Checkout, Session 2 wartet auf eine Idle-Connection, die vor Rückkehr des Konstruktors nicht freigegeben wird.
- **Auswirkung:** Konstruktor hängt mit `context.Background()` unbegrenzt; mit Deadline liefert er spät einen wenig spezifischen Context-Fehler.
- **Schutzmaßnahmen:** `size > 0` wird geprüft und bereits erzeugte Sessions werden bei einem Fehler geschlossen. Ein MaxOpen-Guard fehlt.
- **Minimale Korrektur:** `size > effective MaxOpen` vor dem ersten Dial mit `ErrInvalidArgument` ablehnen; UDP bleibt wie bisher unsupported.
- **Regressionstest:** `MaxOpen=1`, `size=2` muss sofort und ohne Dial-Leak scheitern.
- **Verifikation:** statischer Ressourcenbeweis; SessionPool-Bestandstests 20-fach unter Race grün, testen nur zulässige Größen.

### PARSE-001 — Erfolgreiche Einzelantwort lässt Restbytes für den nächsten Request zurück

- **Severity / Confidence / Status / Release-Blocker:** P1 / medium / Confirmed / **ja**
- **Ort / API:** `memcached/protocol_response.go`, `read1` Z. 680–721, `read2` Z. 725–792, `readRetrieval` Z. 954–1001; `memcached/client_dispatch.go`, `finishConn` Z. 179–193 und `sendOnConn` Z. 195–217; alle Stream-Einzeloperationen und jeweils das Ende einer Pipeline-Welle.
- **Ursache:** Nach einer erwarteten Statuszeile beziehungsweise `END`/`MN` wird Erfolg gemeldet, ohne bereits im `bufio.Reader` befindliche unerwartete Bytes zu erkennen. `finishConn` gibt die Connection bei `err == nil` in den Pool zurück.
- **Feindliche Eingabe:** auf `set`: `STORED\r\nVALUE secret 0 6\r\nSECRET\r\nEND\r\n`; oder auf `get`: `VALUE k 0 1\r\nx\r\nEND\r\nEND\r\n`. Der nächste Request kann die gepufferte Fremdantwort konsumieren.
- **Auswirkung:** Cross-Request-Antwortvermischung, stille False-Misses oder falsche Values bei fehlerhaftem/feindlichem Peer oder Proxy.
- **Schutzmaßnahmen:** erkannte Parsefehler werden verworfen; Quiet verwirft immer. Ein Restbyte-Guard fehlt. Ein Guard darf innerhalb einer Pipeline-Welle nicht anschlagen, weil dort folgende Antworten legitimerweise schon gepuffert sein können.
- **Minimale Korrektur:** Restbytes an den Grenzen nicht gepipelter Antworten und nach dem letzten Frame einer Pipeline-Welle als `ErrMalformedResponse` behandeln und die Connection verwerfen. Die verbleibende Grenze — später eintreffende Bytes eines aktiven bösartigen Servers — ist protokollbedingt nicht vollständig eliminierbar.
- **Regressionstest:** Fake-Server-Seeds mit Extra-`END`, Status+`VALUE` und Meta-`HD`+`VA`; erster Call muss fehlschlagen oder die Connection sicher verwerfen, der zweite darf die Bytes nicht übernehmen.
- **Verifikation:** statischer Parser-/Reuse-Beweis; vorhandene malformed-body-, Quiet- und Barrier-Tests 10-fach unter Race grün, kein Trailing-Frame-Test vorhanden.

## 5. Findings

Die Findings sind innerhalb der Bereiche nach Severity und Confidence geordnet. Release-Blocker aus Abschnitt 4 werden hier nicht dupliziert.

### Lifecycle and Goroutines

#### LIFE-002 — `Close` beendet ausgecheckte Calls bei `TimeoutNone` nicht

- **Severity / Confidence / Status / Release-Blocker:** P2 / high / Confirmed / nein
- **Ort / API:** `memcached/client.go`, `(*Client).Close` Z. 179–209; `memcached/pool.go`, `close` Z. 58–89; `memcached/protocol_response.go`, `prepareOpIO` Z. 540–546; `Client.Close`, alle Calls.
- **Ursache / Interleaving:** Pool-Close drainiert nur Idle-Connections. Ein bereits ausgecheckter Single-Node-Call mit `context.Background()` und `TimeoutNone` besitzt weder Deadline noch Cancel; `Close` kehrt zurück, während I/O weiter blockiert. Bei `FanoutWorkers=0` existiert kein Client-Root-Context für ephemere Fanout-Jobs.
- **Auswirkung:** begrenzter FD-/Goroutine-Leak pro aktivem Call, bei nicht antwortendem Peer potenziell unbegrenzt.
- **Schutzmaßnahmen:** Die GoDoc sagt ausdrücklich „shuts idle sockets“; ein Caller-Context mit Cancel/Deadline beendet I/O. Persistent Fanout kann Background-Calls canceln, sofern LIFE-001 nicht greift.
- **Minimale Korrektur / Test:** entweder checked-out Connections/aktive Calls in `Close` registrieren und schließen oder den eingeschränkten Close-Vertrag an `Client` und `TimeoutNone` prominenter dokumentieren. Regression: Single- und Multi-Node-Call parallel zu Close.
- **Verifikation:** statisch bestätigt; bestehende `TestCloseDuringGet`- und Cancel-Tests prüfen nur Post-Close beziehungsweise Caller-Cancel, nicht aktives unbegrenztes I/O.

#### LIFE-003 — Pool-Close im Checkout-Race wird als `ErrTransport` klassifiziert

- **Severity / Confidence / Status / Release-Blocker:** P2 / high / Confirmed / nein
- **Ort / API:** `memcached/client_dispatch.go`, `getConn` Z. 10–30, `wrapTransport`/`isClassifiedNonTransport` Z. 98–133; `memcached/pool.go`, `get` Z. 126–166; alle APIs parallel zu `Client.Close`.
- **Ursache / Interleaving:** Der erste `c.closed`-Check passiert vor dem Pool-Close. Liefert `pool.get` anschließend das interne `errPoolClosed`, ist dieses weder `ErrClosed` noch „classified non-transport“ und wird in `ErrTransport` gewrappt.
- **Auswirkung:** Retry-, Logging- und Shutdown-Code kann eine normale Close-Race als Transportausfall behandeln.
- **Schutzmaßnahmen:** Calls, die erst nach vollständig gesetztem `c.closed` beginnen, liefern `ErrClosed`; `TestCloseThenOpsReturnErrClosed` deckt diesen Fall.
- **Minimale Korrektur / Test:** `errPoolClosed` bei geschlossenem Client in `ErrClosed` übersetzen. Race-Stresstest `Close || Get` muss für jeden Close-bedingten Fehler `errors.Is(err, ErrClosed)` erfüllen.
- **Verifikation:** statisch bestätigt; vorhandene Close-Tests liefen unter Race grün, prüfen diese Fehlerklasse nicht.

### Connection Pool and Ownership

#### Ownership-Tabelle

| Pfad | Checkout/Owner | Erfolg | Fehler/Desync | Terminalaktion |
|---|---|---|---|---|
| Einzel-Dispatch/Broadcast | `pool.get` / Call | `release -> put` | `shouldDiscardConn -> discard` | genau ein `finishConn` |
| Text-Quiet | `pool.get` / Call | keine Wiederverwendung | Fault-Probe, danach immer discard | `discard` |
| Pipeline/Prepared | `pool.get` / Node-Gruppe | `release -> put` | Pipeline-Discard-Klasse | genau ein `finishPipelineConn` |
| Session | einmaliger `pool.get` / Session | bleibt ausgecheckt | Desync schließt Session und verwirft | `Session.Close -> release` |
| SessionPool | N Sessions / Pool | exklusiver Lease | defekte Session wird lazy ersetzt | `SessionPool.Close` |
| Watch/Dump | dedicated Dial / Stream-Goroutine | nicht gepoolt | Cancel/Fehler | `cn.Close` |
| Cleaner/Pool-Close | Idle-Pool | Reusable bleibt idle | expired/removed | `closeSilent` bzw. `Close` |
| UDP | per-node Pool `1/1` | `put` | malformed/transport -> discard | kein `quit` |
| TLS/TCP/Unix | Pool nach Dial/Handshake/Auth | wie Stream | Fehler vor Pool-Attach schließt Raw-Conn | Pool oder direkte Close |

Abgesehen von POOL-001 und POOL-002 wurde auf öffentlichen Happy-/Error-Pfaden keine Double-Return- oder Double-Use-Stelle bestätigt. Ausgecheckte Sessions müssen vom Caller geschlossen werden; das ist dokumentierter Ownership-Vertrag, kein eigenständiger Produktionsdefekt.

### Parser and Wire Desynchronization

#### PARSE-002 — Normale Protokollzeilen besitzen kein Längenlimit

- **Severity / Confidence / Status / Release-Blocker:** P2 / high / Confirmed / nein
- **Ort / API:** `memcached/conn.go`, `readLine`/`readSlice` Z. 199–224; alle TCP-/Unix-Empfangspfade außer Watch/Dump.
- **Ursache / Eingabe:** `bufio.ReadBytes` akkumuliert eine beliebig lange Zeile. Seed: mehrere MiB ohne `\n` oder eine sehr große `STAT ...\r\n`-Zeile.
- **Auswirkung:** ungebundener Speicherverbrauch durch feindlichen oder defekten Server.
- **Schutzmaßnahmen:** VALUE-/VA-Bodies sind gecappt; Admin-Streams besitzen 64-KiB-Zeilenlimit. Dieser Guard gilt nicht für normale Antwortzeilen.
- **Minimale Korrektur / Test:** gemeinsames Protokollzeilenlimit mit `ErrMalformedResponse` und Discard. Test mit `limit+1` Bytes ohne Newline.
- **Verifikation:** direkter Codebeweis; keine dynamische Großallokation erzeugt, um den Audit selbst zu begrenzen.

#### PARSE-003 — Stats-Gesamtantwort kann unbegrenzt wachsen

- **Severity / Confidence / Status / Release-Blocker:** P2 / high / Confirmed / nein
- **Ort / API:** `memcached/protocol_response.go`, `read2` Z. 725–792, insbesondere Z. 756–764; `memcached/protocol_stats.go`, `appendStat` Z. 35–44 und `consumeStatsLine` Z. 92–124; `Stats`, `StatsAll`, Stats-Detail.
- **Ursache / Eingabe:** Bis `END` wird eine unbegrenzte Zahl auch künftig begrenzter `STAT`-Zeilen in `[]Stat` angehängt. Seed: `STAT k v\r\n` wiederholt ohne `END`.
- **Auswirkung:** ungebundener Speicherverbrauch; PARSE-002 zu beheben begrenzt nicht die Gesamtzahl.
- **Schutzmaßnahmen:** Context-/Read-Deadline begrenzen Zeit, nicht bereits akkumulierte Bytes.
- **Minimale Korrektur / Test:** Gesamtbyte- oder Zeilenbudget pro Stats-Antwort; Überschreitung führt zu malformed+discard. Test mit kleinem konfiguriertem/internem Budget.
- **Verifikation:** statisch bestätigt; normale Stats- und lokale Livetests grün.

#### PARSE-004 — Retrieval-Destination bleibt nach Parsefehler teilweise gefüllt

- **Severity / Confidence / Status / Release-Blocker:** P2 / medium / Plausible / nein
- **Ort / API:** `memcached/protocol_response.go`, `readRetrieval` Z. 954–1001; `Get`, `GetOne`, Pipeline-Get.
- **Ursache / Eingabe:** `Found`, Flags, CAS und Value-Slice werden vor vollständigem Body/abschließendem `END` gesetzt. Seed: erstes VALUE korrekt, zweites mit zu kurzem Body oder falschem CRLF.
- **Auswirkung:** Caller kann trotz Methodenfehler bereits gesetzte Slots irrtümlich als vertrauenswürdig verwenden.
- **Schutzmaßnahmen:** Connection wird bei malformed verworfen. Go-Konvention erlaubt grundsätzlich undefinierte Out-Parameter bei Fehler, aber der öffentliche Vertrag beschreibt diesen Fall nicht.
- **Minimale Korrektur / Test:** entweder Ergebnisse bei Parse-/Transportfehler atomar invalidieren oder Teilerfolg bei Fehler explizit dokumentieren. Test muss den gewählten Vertrag festschreiben.
- **Verifikation:** statischer Schreib-vor-Ende-Pfad; vorhandene malformed-value-Tests grün, prüfen den Destination-Zustand nicht.

#### PARSE-005 — Doppelte serverseitige Meta-Opaque-Antwort überschreibt denselben Slot

- **Severity / Confidence / Status / Release-Blocker:** P2 / medium / Confirmed / nein
- **Ort / API:** `memcached/client_meta_get.go`, `fillMetaPipe` Z. 654–708 und `matchMetaPipelineOpaque` Z. 900–935; Meta-Pipeline und PreparedMetaBatch.
- **Ursache / Eingabe:** Client-seitig doppelte Opaques werden abgelehnt, der Empfangspfad markiert einen bereits beantworteten Opaque jedoch nicht. Seed: zwei `VA ... O0` oder `HD O0` vor `MN`.
- **Auswirkung:** derselbe Ergebnis-Slot wird überschrieben, ein anderer bleibt Default; Connection-Position bleibt zwar durch `MN` bestimmbar.
- **Schutzmaßnahmen:** unbekannte Opaques werden in Scratch geparst, Bodies sicher verworfen; Client-Index lehnt doppelte Request-Opaques ab.
- **Minimale Korrektur / Test:** Seen-Bitmap pro Pipeline-Gruppe; zweite Antwort für denselben Opaque als malformed verwerfen. Regression mit `O0, O0, MN`.
- **Verifikation:** statischer Mapping-Pfad; bestehende Opaque-Index-Tests 10-fach unter Race grün, testen nur Request-Duplikate.

### Request Injection

#### INJ-001 — Auth-Validator akzeptiert einzelne ASCII-Control-Zeichen

- **Severity / Confidence / Status / Release-Blocker:** P3 / high / Improvement / nein
- **Ort / API:** `memcached/client_auth.go`, `validAuthSecret` Z. 38–45; `Config.Auth`.
- **Ursache / Eingabe:** CR, LF, NUL und Whitespace werden verworfen, beispielsweise BEL `\x07` und DEL `\x7f` jedoch nicht.
- **Auswirkung:** keine Command-Injection, weil Credentials als length-framed Storage-Body übertragen werden; lediglich unnötig breite Credential-Zeichenmenge.
- **Schutzmaßnahmen:** Remote-Plaintext-Gate, UDP-Ablehnung, Secret-freie Fehler und Probe-Cleanup.
- **Minimale Korrektur / Test:** Controls `<= 0x20` und `0x7f` konsistent ablehnen; Table-Test für BEL/DEL/NUL/CR/LF.
- **Verifikation:** Codeprüfung; Auth-/Injection-Tests und Livetest grün.

### Cluster and Failover

#### CLUSTER-001 — In-flight Pick kann nach Node-Entfernung einen neuen Zombie-Pool anlegen

- **Severity / Confidence / Status / Release-Blocker:** P2 / high / Confirmed / nein
- **Ort / API:** `memcached/client_dispatch.go`, `getConn` Z. 10–91; `memcached/client.go`, `applyServers` Z. 109–174; `SetServers`/ResolverRefresh parallel zu Requests.
- **Ursache / Interleaving:** Ein Request hält einen alten `*addr` ohne Pool. `SetServers` entfernt ihn. Danach legt `getConn` unter `c.mu` anhand der alten ID einen neuen Pool an, ohne aktuelle Membership zu prüfen.
- **Auswirkung:** Traffic zu einem dekommissionierten Node und ein Pool außerhalb der aktuellen Membership bis zu weiterem `SetServers` oder `Close`.
- **Schutzmaßnahmen:** Bereits existierende entfernte Pools werden geschlossen; Re-Put in einen geschlossenen Pool schließt die Connection. Das schützt den „noch nie gedialt“-Pfad nicht.
- **Minimale Korrektur / Test:** vor Pool-Erzeugung Membership/Generation prüfen. Synchronisierter Test „pick -> remove -> first getConn“ darf weder Dial noch Map-Eintrag erzeugen.
- **Verifikation:** statischer enger Interleaving-Beweis; vorhandene SetServers-/Prepared-/Session-Tests 20-fach unter Race grün, enthalten keinen First-Dial-Barrier.

Nicht als Defekt gewertet wurde der atomare `single`-Fast-Path bei `1 -> N SetServers`: Ein Request, dessen `single.Load` vor dem Membership-Swap linearisiert, darf wie andere in-flight Snapshot-Requests noch die alte Topologie verwenden. Eine strengere Linearizierbarkeitsgarantie ist nicht dokumentiert.

### UDP

#### UDP-001 — Fragment-Reassembly begrenzt Anzahl, nicht Gesamtbytes

- **Severity / Confidence / Status / Release-Blocker:** P2 / high / Confirmed / nein
- **Ort / API:** `memcached/protocol_udp.go`, `udpFragmentCap` Z. 215–221 und `assembleUDPPayload` Z. 234–329; alle UDP-Reads.
- **Ursache / Eingabe:** Jedes Fragment kann nahezu Datagrammgröße besitzen; bei `total <= maxFragments` werden Payloads ohne Gesamtbudget angehängt. Default-Floor 2.048 Fragmente erlaubt theoretisch etwa 128 MiB, bei großem `MaxValueBytes` deutlich mehr.
- **Auswirkung:** Memory-DoS durch kompromittierten oder on-path UDP-Peer vor Anwendung des VALUE-/VA-Limits.
- **Schutzmaßnahmen:** Header/Reserved/seq/total/Fragmentkonflikte sind geprüft; große Buffer über 64 KiB gehen nicht zurück in den Pool.
- **Minimale Korrektur / Test:** kumulatives Assembly-Budget, abgeleitet aus `MaxValueBytes` plus begrenztem Protokolloverhead; bei Überschreitung malformed+discard. Test mit vielen großen Fragmenten unterhalb des Fragmentzählers.
- **Verifikation:** statisch bestätigt; UDP-Guard-Tests 10-fach unter Race und lokale UDP-Livetests grün.

#### UDP-002 — 16-Bit-ID-Wrap kann verspätetes Datagramm falsch zuordnen

- **Severity / Confidence / Status / Release-Blocker:** P2 / medium / Plausible / nein
- **Ort / API:** `memcached/protocol_udp.go`, `nextUDPRequestID` Z. 65–74 und `assembleUDPPayload` Z. 234–329; UDP-Pool `MaxOpen=1` in `memcached/client_dispatch.go` Z. 71–74.
- **Ursache / Sequenz:** Der Socket wird wiederverwendet, IDs wiederholen sich nach 65.535 Nonzero-Werten. Ein verspätetes oder injiziertes Datagramm mit der wiederverwendeten ID kann vor der echten Antwort gelesen werden.
- **Auswirkung:** falsche Antwortzuordnung/Cross-Request-Integritätsverlust.
- **Schutzmaßnahmen:** falsche aktuelle IDs werden verworfen; pro Node ist nur ein Request gleichzeitig aktiv. README dokumentiert fehlende UDP-Integrität.
- **Minimale Korrektur / Test:** Socket nach Operation drainieren oder nicht wiederverwenden; Wraparound-Test mit vorab eingespeistem Datagramm derselben recycelten ID.
- **Verifikation:** Protokoll-/Codepfad bestätigt, nicht über 65.535 Live-Requests reproduziert. Aktive Spoofing-Abwehr ist mit Memcached-UDP grundsätzlich nicht vollständig möglich.

### Security Gates and Transport

#### SEC-001 — Plaintext-Auth vertraut bei custom Dial auf den deklarierten Zielserver

- **Severity / Confidence / Status / Release-Blocker:** P3 / high / Improvement / nein
- **Ort / API:** `memcached/config.go`, `validateAuthAgainstServers` Z. 283–298; `memcached/runtime.go`, TLS-Wrap Z. 164–205; `memcached/addr.go`, Dial Z. 50–57; `Transport.Dial`, `Config.Auth`.
- **Ursache / Beispiel:** Die Loopback-/Unix-Ausnahme prüft `Config.Servers`. Ein vom Nutzer gelieferter raw Dialer kann diese Adresse ignorieren und ohne TLS zu einem Remote-Ziel verbinden.
- **Auswirkung:** Credentials können durch eine selbst bereitgestellte, fehlleitende Trusted-Dial-Funktion remote im Klartext gesendet werden.
- **Schutzmaßnahmen:** normale Remote-Adressen werden blockiert; gesetztes TLS umschließt auch custom Dial.
- **Minimale Korrektur / Test:** Trust-Boundary des Dialers dokumentieren; optional Auth ohne TLS bei custom Dial nur mit explizitem `AllowPlaintext`. Test Dialer „deklariert loopback, verbindet remote“.
- **Verifikation:** statisch bestätigt; keine externe Remote-Verbindung ausgeführt.

#### SEC-002 — Unix-Socket-Mode-Check ist vor dem Connect nicht atomar

- **Severity / Confidence / Status / Release-Blocker:** P3 / medium / Improvement / nein
- **Ort / API:** `memcached/conn.go`, `checkUnixSocketMode` Z. 55–71 und `newConnContext` Z. 125–132; Unix-Transport.
- **Ursache / Interleaving:** Zwischen `Lstat` und `Dial` kann ein lokaler Angreifer mit Verzeichnisrechten den Socket ersetzen.
- **Auswirkung:** best-effort Mode-Gate kann lokal umgangen werden; erforderlich sind bereits lokale Filesystem-Privilegien.
- **Schutzmaßnahmen:** world-writable und über `UnixSocketMaxMode` unzulässige Modi werden im Normalfall abgelehnt.
- **Minimale Korrektur / Test:** Peer/FD nach Connect soweit plattformgerecht verifizieren oder TOCTOU-Grenze dokumentieren. Kein flake-anfälliger Timing-Test empfohlen.
- **Verifikation:** statische Sequenz; normale Unix-Mode-Unit-Tests grün, kein Unix-Livetest.

### Public API and Documentation

#### DOC-001 — Slice-Ownership ist für Storage- und Meta-Values nicht durchgängig dokumentiert

- **Severity / Confidence / Status / Release-Blocker:** P3 / high / Improvement / nein
- **Ort / API:** `memcached/item.go` Z. 17–65; `protocol_storage.go` Z. 17–27, 58; `meta.go` Z. 227–241; `protocol_meta_set.go` Z. 103–117; `Item.Value`, `MetaOp.Body`, `MetaItem.Value`, `Store`, `Session.Store`, Meta-Pipeline.
- **Ursache / Beispiel:** synchrone Sends und Meta-Set borgen Caller-Slices, während Prepared-Pfade Ownership teilweise explizit beschreiben. Parallele Mutation bis zum Methodenende kann Race/Wire-Korruption erzeugen.
- **Auswirkung:** Caller kann Lifetime und Wiederverwendung nicht allein aus der Typ-GoDoc sicher ableiten.
- **Schutzmaßnahmen:** `GetResult` und Prepared-`RunValues` sind gut dokumentiert; Prepared defaults werden kopiert.
- **Minimale Korrektur / Test:** an jedem öffentlichen Slice-Feld Borrow-/Copy-/Reuse-Lifetime angeben; GoDoc-Review als Release-Gate.
- **Verifikation:** `go doc -all ./memcached`; keine Runtime-Behauptung über automatische Kopie auf den genannten Pfaden.

#### DOC-002 — Close-, Teilerfolg- und Ringwechsel-Verträge sind über APIs verteilt

- **Severity / Confidence / Status / Release-Blocker:** P3 / high / Improvement / nein
- **Ort / API:** `memcached/client.go` Z. 179–227; `client_get.go` Z. 25–38, 188–222; `client_pipeline.go` Z. 496–520; `client_stats.go` Z. 65–88; `SetServers`, `Get`, Pipeline, StatsAll, Close.
- **Ursache / Beispiel:** README enthält Teile der Semantik, die jeweilige Methoden-GoDoc sagt aber nicht konsistent: erfolgreiche Slots bei joined node errors, in-flight Calls über Close, nil/empty `SetServers`, Ringwechsel ist keine Datenmigration.
- **Auswirkung:** schwerwiegende, aber derzeit dokumentierbare Fehlannahmen bei Retry, Partial Result und Clusterbetrieb.
- **Schutzmaßnahmen:** `VersionAll` beschreibt Partial Results; Session invalidiert Membership-Wechsel und Prepared Batches routen bei Folgeruns neu.
- **Minimale Korrektur / Test:** einheitliche Sätze an allen Fanout-APIs und `Client`; `SetServers(nil)` sowie Remap/keine Migration explizit machen.
- **Verifikation:** öffentliche GoDoc und README verglichen; kein Widerspruch bei Miss- oder Quiet-Semantik.

#### DOC-003 — `Failover.Eject`-Kommentar widerspricht `liveAddrs`

- **Severity / Confidence / Status / Release-Blocker:** P3 / high / Confirmed / nein
- **Ort / API:** `memcached/config.go`, `Failover.Eject` Z. 97–106; `memcached/client_failover.go`, `liveAddrs` Z. 22–66.
- **Ursache / Beispiel:** Kommentar behauptet, ohne Eject würden „other ops“ quarantänisierte Nodes überspringen. `liveAddrs` gibt bei `!Eject` jedoch die vollständige Liste zurück; nur gesonderte First-Live-Pfade können skippen.
- **Auswirkung:** falsche Betreibererwartung an Failover.
- **Schutzmaßnahmen:** Implementierung ist lock-/race-sicher und Eject-Tests belegen das tatsächliche Ketama-Verhalten.
- **Minimale Korrektur / Test:** Kommentar exakt an die API-Matrix anpassen; Contract-Test für `Eject=false`.
- **Verifikation:** direkter Code-/Kommentarvergleich.

### Supply Chain and Release Hygiene

#### TEST-001 — Keine Fuzz-Targets für Parser und UDP-Reassembly

- **Severity / Confidence / Status / Release-Blocker:** P3 / high / Coverage Gap / nein
- **Ort / API:** kein `func Fuzz` in `*_test.go`; relevante Parser in `protocol_response.go`, `protocol_get.go`, `client_meta_get.go`, `protocol_udp.go`.
- **Ursache:** adversariale Parser-Invarianten werden nur durch deterministische Unit-Tests geprüft.
- **Auswirkung:** neue Kombinationen aus Header, Body, Barrier, Opaque und Fragmenten können unentdeckt bleiben.
- **Schutzmaßnahmen:** umfangreiche Table-/Fake-Server-Tests für malformed VALUE, Meta, Quiet, UDP Header und Fragmentkonflikte.
- **Minimale Korrektur / Tests:** Targets `FuzzReadRetrievalFrame`, `FuzzMetaPipelineFrames`, `FuzzAssembleUDPPayload`, `FuzzProtocolLine`; Seeds: extra `END`, kurze/lange VALUE/VA, duplicate Opaque, unknown flags, total mismatch, duplicate/conflicting fragments, Byte-Budget.
- **Verifikation:** Repository-Suche ergab null Fuzz-Targets; daher kein 30–60-s-Smoke-Run möglich.

#### TEST-002 — Kritische Lifecycle-/Cluster-Interleavings fehlen

- **Severity / Confidence / Status / Release-Blocker:** P3 / high / Coverage Gap / nein
- **Ort / API:** `memcached/fanout_test.go`, `pool_test.go`, `client_session_pool_test.go`, `client_dispatch_test.go`, `client_admin_test.go`; Close, Pool, SessionPool, SetServers, Watch.
- **Ursache:** bestehende Tests prüfen isolierte Happy-/Close-Pfade, nicht volle Fanout-Queue, Slot-Wakeup nach Discard, `size > MaxOpen`, First-Dial nach Node-Remove oder aktiven Watch parallel zu Close.
- **Auswirkung:** LIFE-001, POOL-001, POOL-002 und CLUSTER-001 blieben trotz grüner Race-Suite unentdeckt.
- **Schutzmaßnahmen:** Race-Suite und viele gezielte Concurrency-Tests vorhanden.
- **Minimale Korrektur / Tests:** deterministische Barriers statt Sleeps für genau die genannten Interleavings; `-race -count=50` im CI für die kleine Lifecycle-Gruppe.
- **Verifikation:** Testkatalog und gezielte 20-fach-Runs geprüft.

#### RELEASE-001 — `SECURITY.md` und Disclosure-Prozess fehlen

- **Severity / Confidence / Status / Release-Blocker:** P3 / high / Confirmed / nein für v0.1.0, vor v1 dringend
- **Ort / API:** Datei `SECURITY.md` fehlt; `README.md` Z. 58–78 enthält Security-Hinweise, aber keinen vertraulichen Meldeweg.
- **Ursache:** keine Supported-Version-Policy, Kontaktmöglichkeit oder Reaktionsbeschreibung.
- **Auswirkung:** Finder haben vor dem ersten öffentlichen Tag keinen definierten Disclosure-Kanal.
- **Schutzmaßnahmen:** Security-Abschnitt, govulncheck-CI und restriktive Gates sind vorhanden.
- **Minimale Korrektur / Test:** `SECURITY.md` mit privatem Kanal, Scope, SLA und Supported Versions; README-Link und Release-Checklist.
- **Verifikation:** Root-Dateisuche; Datei fehlt.

#### RELEASE-002 — Changelog, SemVer-Entscheidung und Latest-Policy sind nicht festgelegt

- **Severity / Confidence / Status / Release-Blocker:** P3 / high / Improvement / nein als Runtime-Bug; Entscheidung vor Tag erforderlich
- **Ort / API:** Changelog-Datei fehlt; `.github/workflows/release.yml` Z. 47–57 nutzt ausschließlich `--generate-notes --latest`; `README.md` Z. 82–84 empfiehlt `@latest`; keine `v*`-Tags.
- **Ursache:** kuratierte Breaking-/Security-/Ownership-Hinweise und Policy für ältere Release-Linien fehlen.
- **Auswirkung:** ein sofortiges `v1.0.0` würde einen stärkeren Stabilitätsvertrag signalisieren als Dokumentation und offene P1/P2 zulassen.
- **Schutzmaßnahmen:** Tagformat und main-Ancestry werden validiert.
- **Minimale Korrektur / Test:** nach Behebung der Blocker zuerst `v0.1.0`; `CHANGELOG.md`; `v1.0.0` erst nach festgeschriebenen Ownership-/Partial-Verträgen. `--latest` nur für tatsächlich neueste Linie.
- **Verifikation:** `git tag -l 'v*'` leer; Dateien/Workflow geprüft.

## 6. Cross-Finding Interactions

- **Close + Fanout:** LIFE-001 verhindert gerade den Cancel, der persistenten Background-Fanout sonst sicher beendet. Mit `TimeoutNone` wird aus Queue-Sättigung ein dauerhafter Shutdown-Hang.
- **Pool-Waiter + Quiet:** Quiet verwirft absichtlich die Connection. Genau diese Sicherheitsmaßnahme triggert POOL-001 unter `MaxOpen`-Sättigung, weil der freigewordene Slot nicht signalisiert wird.
- **Pool-Waiter + SessionPool:** POOL-002 ist ein Selbst-Wait auf dauerhaft von demselben Konstruktor gehaltene Connections; ein genereller POOL-001-Fix allein löst ihn nicht.
- **SetServers + First Dial:** CLUSTER-001 betrifft nur einen alten in-flight Snapshot ohne bestehenden Pool. Bereits bestehende entfernte Pools werden sicher geschlossen.
- **Failover + PreparedBatch:** sicher: `healthVersion` triggert Routing-Refresh. Sessions sind ausdrücklich sticky und failovern nicht; dies wurde nach Cross-Review nicht als Defekt gezählt.
- **Watch + Close:** aktive Dedicated Streams werden nicht durch Client-Close beendet, aber benötigen eine Context-Deadline und schließen bei Cancel. Da `Close` ausdrücklich nur Idle-Sockets verspricht, wurde dies als dokumentierter Vertrag und nicht als P2 gezählt.
- **TimeoutNone + Context-Cancel:** sicher: Caller-Cancel setzt über den I/O-Watcher eine Deadline; unsicher ist nur `Background`/nicht cancelbares Context zusammen mit dem bewusst unbegrenzten Timeout.
- **TLS + custom Dial:** sicher gegen stillen Plaintext-Escape bei gesetztem TLS; SEC-001 betrifft nur die ausdrücklich plaintext erlaubte Loopback-Ausnahme mit einem selbst fehlleitenden Dialer.
- **UDP + große Antwort:** VALUE-/VA-Limits greifen erst nach Fragment-Reassembly; deshalb verhindert `MaxValueBytes` UDP-001 nicht.
- **Pool-Cleaner + entfernte Nodes:** Cleaner, `pool.close`, Create-during-Close und Re-Put sind race-sicher getestet; CLUSTER-001 entsteht vor der Zuordnung zu einem aktuellen Member-Pool.

## 7. Test and Fuzzing Gaps

Empfohlene konkrete Tests:

- `TestFanoutPoolCloseWithFullQueue`
- `TestConnPoolWaiterWakesAfterDiscard`
- `TestConnPoolWaiterWakesWithNoIdleRetention`
- `TestNewSessionPoolRejectsSizeAboveMaxOpen`
- `TestSingleResponseTrailingBytesDiscardConnection`
- `TestRetrievalExtraEndDoesNotPoisonNextCall`
- `TestProtocolLineLimit`
- `TestStatsResponseBudget`
- `TestGetMalformedResponseDestinationContract`
- `TestMetaPipelineRejectsDuplicateResponseOpaque`
- `TestSetServersRemoveBeforeFirstDial`
- `TestUDPAssemblyByteBudget`
- `TestUDPRequestIDWrapDropsLateDatagram`
- `TestCloseDuringActiveWatchContract`

Empfohlene Fuzz-Targets und Seeds:

- `FuzzReadRetrievalFrame`: VALUE length 0/1/MaxInt/overflow, kurze/lange Bodies, falsches CRLF, extra END, globale Errors zwischen VALUEs.
- `FuzzMetaPipelineFrames`: VA/HD/EN/MN, duplicate/unknown/numeric/string Opaques, unbekannte Flags, Body mit `MN`.
- `FuzzAssembleUDPPayload`: short header, reserved, seq/total mismatch, duplicate/conflicting fragments, reorder, kumulative Byteüberschreitung.
- `FuzzProtocolLine`: fehlende Newline, CR-only, LF-only, sehr lange Tokens und Integergrenzen.

Ein Fuzz-Smoke-Run wurde nicht ausgeführt, weil kein Target existiert und der Audit keine neuen Testdateien anlegen durfte.

## 8. Commands and Verification Results

| Kommando | Exit | Ergebnis |
|---|---:|---|
| `git status --short` | 0 | keine Ausgabe; Working Tree sauber |
| `git rev-parse HEAD` | 0 | `bb8538066508777cd48ad22edb30acbb13a36dd2` |
| `go version` | 0 | `go1.27.1 darwin/arm64` |
| `go doc -all ./memcached` | 0 | öffentliche API vollständig erfasst, 1.056 Zeilen |
| `go test ./...` | 0 | `memcached` OK (cached) |
| `go test -race ./memcached` | 0 | OK, 5.774 s Testausgabe |
| `go vet ./...` | 0 | keine Befunde |
| `golangci-lint run ./...` | 0 | `0 issues` (`golangci-lint 2.13.2`, einschließlich gosec) |
| `govulncheck ./...` | 0 | `No vulnerabilities found` (`govulncheck 1.8.0`, DB 2026-09-10) |
| Lifecycle/Pool/Cluster-Auswahl: `go test -race -count=20 ...` | 0 | OK, 3.490 s Testausgabe |
| Parser/UDP-Auswahl: `go test -race -count=10 ...` | 0 | OK, 2.726 s Testausgabe |
| Security-Gates/TLS-Auswahl: `go test -race -count=10 ...` | 0 | OK, 1.689 s Testausgabe |
| lokale Livetests: `cd livetest && ... go test -race -count=1 ./...` | 0 | OK, 2.626 s gegen Memcached 1.6.45 auf TCP/UDP `127.0.0.1:31211`; Prozess anschließend beendet; Unix übersprungen |
| `rg '^func Fuzz' --glob '*_test.go'` | 1/keine Treffer | keine Fuzz-Targets |

Die grünen Suites belegen vorhandene Guards, nicht die Abwesenheit der statisch bestätigten, bislang ungetesteten Interleavings. Es wurden keine langlaufenden Fuzz- oder Race-Jobs gestartet.

## 9. Release Checklist

- [ ] Keine offenen P0/P1
- [ ] P2 bewertet
- [x] Race-Suite grün
- [ ] Lifecycle-/Leak-Tests grün
- [ ] Parser-Fuzz-Smoke grün
- [x] Security-Gates geprüft
- [ ] API-Ownership dokumentiert
- [ ] SECURITY.md vorhanden
- [ ] Changelog vorhanden
- [ ] SemVer-Tag entschieden

## 10. Recommended Fix Order

1. **POOL-001** beheben und beide Wakeup-Regressions hinzufügen; darauf baut verlässliches Pool-Fortschrittsverhalten auf.
2. **POOL-002** separat durch Konfigurationsvalidierung schließen.
3. **LIFE-001** so umbauen, dass kein blockierender Enqueue unter `fanout.mu` stattfindet; anschließend Close-/TimeoutNone-Stresstest.
4. **PARSE-001** an korrekten Response-Wellengrenzen beheben; gleichzeitig Seeds für Extra-Status/END/MN ergänzen.
5. **UDP-001**, **PARSE-002** und **PARSE-003** mit klaren Bytebudgets härten.
6. **CLUSTER-001**, **LIFE-003**, **PARSE-004/005** und **UDP-002** anhand festgeschriebener API-Semantik korrigieren.
7. Ownership-, Partial-Result-, Close-, Ringwechsel- und Failover-Dokumentation vervollständigen.
8. `SECURITY.md`, `CHANGELOG.md` und Tag-Policy ergänzen; danach erstes öffentliches Tag **v0.1.0**, nicht v1.0.0.

## 11. Appendix

### Verworfene False Positives

- **`single.Load` bei `1 -> N SetServers` als P1:** verworfen. Ein vor dem Swap gestarteter Call darf auf dem alten Snapshot linearisiert werden; kein nach abgeschlossenem Swap beginnender Call sieht den alten Atomic-Pointer.
- **Aktive Watch/Dump-Connection als Leak durch Close:** als Defekt verworfen. Deadline ist Pflicht, Caller-Cancel schließt den Socket, und `Close` verspricht nur Idle-Sockets. Die Semantik sollte dennoch in DOC-002 zentral stehen.
- **Vergessene Session nach SetServers als Pool-Leak:** verworfen. `Session.Close` ist ausdrücklicher Caller-Ownership-Vertrag.
- **Session ohne `healthVersion` als Failover-Bug:** verworfen. Sessions sind dokumentiert node-sticky und „do not fail over“; `validateKey` verhindert stilles Schreiben auf den falschen Node.
- **Dial kurz nach Close als dauerhafter Leak:** verworfen. Der Pool prüft nach Create erneut `closed` und schließt die Connection; übrig bleibt nur die Fehlerklassifizierung LIFE-003.
- **Value-Command-Injection:** verworfen. Storage- und Meta-Bodies sind length-framed, einschließlich Prepared-`RunValues`.
- **Security-Gate-Bypässe:** keine bestätigt. Admin/Flush/SetServers, node-lokales Shutdown, TLS-Wrap, Insecure-TLS-Opt-in und Remote-Plaintext-Auth hielten Code- und Regressionstest.
- **Fehlende SECURITY.md/Changelog als P2-Produktionsbug:** auf P3 Release-Hygiene herabgestuft.

### Nicht reproduzierbare beziehungsweise nur statisch verifizierte Risiken

- LIFE-001, POOL-001 und POOL-002 besitzen deterministische Code-Interleavings, aber keine vorhandenen Regressionstests; wegen des Verbots neuer Testdateien wurden keine Overlay-Tests erzeugt.
- PARSE-001 wurde am Parser-/Pool-Pfad bestätigt, aber nicht mit einem neuen Fake-Server-Test reproduziert.
- UDP-002 wurde nicht über einen 65.535-Request-Langlauf reproduziert.
- SEC-002 wurde wegen timingabhängiger lokaler Socket-Ersetzung nicht dynamisch provoziert.

### Bekannte UDP-/Plaintext-Limits

- UDP hat keine Authentizität oder Integrität; 16-Bit-Request-IDs sind Korrelation, kein Schutz.
- Falsche IDs, Duplikate und Reorder sind behandelbar; aktives Spoofing und Verlust sind prinzipbedingt nicht vollständig lösbar.
- Der Client erlaubt über UDP nur `get`/`gets`, `version`, `me`, `mn`, `mg` ohne Side-Effect-Flags und Meta-Debug; Mutationen, TLS, Auth, Admin, Stats, Pipeline und Session sind gesperrt.
- TCP ist standardmäßig Plaintext. Jeder On-Path-Teilnehmer kann Cache-Daten und — sofern serverseitig erreichbar — die Administration beeinflussen. `RequireTLS` ändert nicht den Default, sondern erzwingt explizite TLS-Konfiguration.

### Relevante Wire-Invarianten

1. Jede Stream-Connection besitzt höchstens einen aktiven Owner; jede Checkout-Route endet genau einmal in `put`, `discard` oder Close.
2. Nach erkanntem Transport-, Server- oder Malformed-Fehler darf die Connection nicht wiederverwendet werden.
3. VALUE/VA-Bodylänge muss vor Allokation begrenzt und exakt einschließlich folgendem CRLF konsumiert werden.
4. Einzelantworten enden an Statuszeile, `END` oder `MN`; unerwartete Restbytes an einer Wellengrenze bedeuten Desynchronisation.
5. Innerhalb einer Pipeline-Welle sind gepufferte Folgeantworten legitim; Restbyte-Prüfungen dürfen erst nach dem letzten erwarteten Frame erfolgen.
6. Meta-Pipeline-Opaques müssen request- und response-seitig eindeutig genau einem Slot zugeordnet sein.
7. UDP-Reassembly benötigt sowohl Fragmentanzahl- als auch Gesamtbytegrenze.
8. Membership-Snapshots dürfen in-flight weiterleben, dürfen aber nach Entfernung keinen neuen ungebundenen Pool erzeugen.

**Endgültige Entscheidung: DO NOT RELEASE**
