# Auftrag: Memcached-Fehlerinventar vs. go-memcached

Copy **Gemeinsamer Kopf** plus genau einen **AGENT**-Block.

**Modell:** ausschließlich **Grok 4.6 extra high fast** (xhigh fast). Kein anderes Modell. Kein Grok 4.6 fast high, kein high max, kein anderes Haus.

Vier Agenten parallel (1–4), danach AGENT 5. Jeder Agent schreibt nur in seine Dateien. AGENT 5 konsolidiert nach `docs/audit/13-errors.md`.

Kein Anwendungscode ändern.

**Findings nur in Markdown-Dateien.** Chat ist kein Deliverable: kein Inventar, keine Tabelle, keine Token-Liste nur in der Antwort. Jeder Agent **legt oder ergänzt** seine `.md` und schreibt jedes Finding dort. Im Chat nur: welche Dateien geschrieben wurden.

| Wer | Datei |
| --- | --- |
| AGENT 1 | `docs/audit/fragments/errors-agent1-global-text.md` |
| AGENT 2 | `docs/audit/fragments/errors-agent2-meta.md` |
| AGENT 3 | `docs/audit/fragments/errors-agent3-admin-conn.md` |
| AGENT 4 | `docs/audit/fragments/errors-agent4-api-tests.md` |
| AGENT 5 | `docs/audit/13-errors.md` |

Ordner anlegen falls fehlend. Existierende Fragmente **ergänzen**, nicht leer überschreiben, außer du bist derselbe Agent und ersetzt den eigenen Lauf.

Sprache: Deutsch. Spec-Zitate und Wire-Tokens bleiben Englisch.

Pre-release, kein Legacy-Schonmodus. Breaking Changes vorschlagen ist erlaubt und erwünscht, wenn die Spec klarer wird. yeqown und gomemcache sind nur Vergleich, kein Vorbild.

---

## 1. Zuerst Spec fetchen (nicht aus dem Gedächtnis)

In dieser Reihenfolge lesen. Jede Claim braucht URL + Abschnitt + Zeile/Token.

| Was | URL |
| --- | --- |
| Doku-Start | https://docs.memcached.org/ |
| Basic Text | https://docs.memcached.org/protocols/basic/ |
| Meta Text | https://docs.memcached.org/protocols/meta/ |
| Kanonische Spec | https://raw.githubusercontent.com/memcached/memcached/master/doc/protocol.txt |
| Repo-Kopie (gegen Upstream diffen) | `docs/protocol.txt` |
| Meta-Wiki | https://github.com/memcached/memcached/wiki/MetaCommands |

Binary-Protokoll und SASL: nur listen, **out of scope**. Nicht als Lücke zählen.

Zusätzlich den Server-Quelltext stichprobenweise prüfen, wenn die Doku unklar ist:

- https://github.com/memcached/memcached — suche `out_string`, `CLIENT_ERROR`, `SERVER_ERROR`, `ERROR`, `BUSY`, `BADCLASS`, Meta-Codes (`HD`, `EN`, `EX`, `NS`, `NF`, `VA`, `MN`).
- Ziel: Tokens finden, die in `protocol.txt` fehlen oder anders heißen (Wiki-Tippfehler vs Wire).

Wenn Spec und Wiki widersprechen: **Wire/protocol.txt gewinnt**. Wiki-Abweichung extra markieren.

---

## 2. Was „richtig abfangen und werfen“ heißt

Zwei Richtungen, beide Pflicht:

**A. Empfangen (Parser)**
Jede mögliche Antwortzeile des Servers muss erkannt werden. Kein stilles `nil`. Kein `ErrMalformedResponse` für eine dokumentierte Statuszeile.

**B. Werfen (API)**
Die öffentliche API muss denselben Fall als Sentinel zurückgeben, den Caller so prüfen:

```go
errors.Is(err, memcached.ErrNotFound)
errors.Is(err, memcached.ErrExists)
errors.Is(err, memcached.ErrNotStored)
errors.Is(err, memcached.ErrClientError)
errors.Is(err, memcached.ErrServerError)
```

Wrapper sind Pflicht: `fmt.Errorf("…: %w", sentinel)`, damit die Server-Message erhalten bleibt (`CLIENT_ERROR bad flags` → `errors.Is(..., ErrClientError)` und `err.Error()` enthält `bad flags`).

Kein String-Match auf `err.Error()` als öffentliche Semantik.

---

## 3. Vollständiges Inventar bauen (Spec → Tabelle)

Zuerst eine **vollständige Liste aller dokumentierten Antwort-Tokens**, gruppiert. Danach erst den Code. Nichts auslassen, auch wenn wir den Befehl nicht implementieren — dann Status `BEWUSST AUSGELASSEN` plus Grund.

### 3.1 Globale Error-Strings (gelten vor jedem Command)

Aus `protocol.txt` Abschnitt „Error strings“. Jeder Command **kann** sie zusätzlich zu den Command-spezifischen Antworten senden. Clients **müssen** das immer erlauben.

| Wire | Bedeutung | Erwarteter Sentinel | Conn danach |
| --- | --- | --- | --- |
| `ERROR\r\n` | nonexistent command name | eigener Sentinel, nicht ClientError | keepalive |
| `ERROR <message>\r\n` | bei manchen Admin-Cmds: falsche Args (LRU, watch, …) — **nicht** dasselbe wie nacktes `ERROR` | nicht `ErrNonexistentCommand` | keepalive |
| `CLIENT_ERROR <message>\r\n` | Input verletzt das Protokoll | `ErrClientError` + Message | keepalive (außer Spec sagt Close) |
| `SERVER_ERROR <message>\r\n` | Server kann Command nicht ausführen; bei schweren Fehlern **schließt der Server die Conn** | `ErrServerError` + Message | **discard** |

Prüfe Prefix-Match: `CLIENT_ERROR` nur als Token + Space/Ende, niemals `CLIENT_ERRORFOO`. CRLF und nacktes `\n` beide.

### 3.2 Text-Command-Status (Erfolg und Miss sind oft **keine** Exceptions im Protokoll, aber API-Fehler)

Pro Command aus der Spec die **komplette** Antwortmenge:

Storage (`set`/`add`/`replace`/`append`/`prepend`/`cas`):

- `STORED` → nil
- `NOT_STORED` → `ErrNotStored` (add/replace/append/prepend-Bedingung)
- `EXISTS` → `ErrExists` (CAS mismatch)
- `NOT_FOUND` → `ErrNotFound` (CAS auf fehlendem Key)

Retrieval (`get`/`gets`/`gat`/`gats`):

- fehlende Keys werden **weggelassen**, Abschluss `END` ist Erfolg
- Multi-Get: leere Map ist **nicht** `ErrNotFound`
- Single-Key-API (`GetOne`): Miss **ist** `ErrNotFound`
- Value-Body darf die Tokens `ERROR`, `END`, `CLIENT_ERROR` als Payload enthalten — nur die **Statuszeile** ist ein Fehler

Delete: `DELETED` / `NOT_FOUND`
Incr/Decr: `<zahl>\r\n` / `NOT_FOUND` / `CLIENT_ERROR cannot increment or decrement non-numeric value`
Touch: `TOUCHED` / `NOT_FOUND`
Flush: `OK` (+ optionales Delay-Token)
Version: `VERSION …`
Stats: `STAT …` / `END` / `RESET` / Fehlerzeilen
Auth (`-Y`, Fake-`set`): `STORED` / `CLIENT_ERROR [message]`
Quit: Conn close, keine Antwort erwartet

`noreply`: unterdrückt die **nominelle** Reply. Spec: bei malformed Request kommt `CLIENT_ERROR` **trotzdem**. Nichtlesen desynchronisiert TCP. Meta-`q` ist das korrekte Quiet-Modell (`mn`-Barriere, Non-Nominal inkl. `EX`/`CLIENT_ERROR` bleibt).

### 3.3 Meta-Response-Codes

Aus Meta-Spec, nicht aus dem alten Wiki erfinden:

| Code | Text-Äquivalent | Sentinel / Result |
| --- | --- | --- |
| `HD` | STORED / DELETED / OK ohne Value | Erfolg, kein Error |
| `VA <n>` | VALUE | Erfolg + Body |
| `EN` | Miss (`mg`/`me`) | `ErrNotFound` nur wenn die API Miss als Fehler verkauft |
| `NF` | NOT_FOUND | `ErrNotFound` |
| `NS` | NOT_STORED | `ErrNotStored` |
| `EX` | EXISTS / CAS fail | `ErrExists` — auch bei `q` **nicht** hidbar |
| `MN` | meta noop / Quiet-Barriere | intern, kein App-Error |

Token-Match: `EN` darf **nicht** `END` matchen. `EN` / `EN kfoo` ja, `END` / `ENTRY` nein.

Quiet `q`: nominelle Codes weg (`mg`→`EN`, `ms`→`HD`). Fehler und Nicht-Nominal (`EX`, `CLIENT_ERROR`, `SERVER_ERROR`) bleiben. Pipeline: Charge-Fehler vs per-Key-Status trennen.

Wiki-`ST` / Spec-Tippfehler `DE` unter `md q`: Wire ist `HD`. Nicht als Missing Error bauen.

### 3.4 Admin-/Ops-Keywords (eigene Tokens, nicht Malformed)

Mindestens:

- `OK`
- `BUSY [message]`
- `BADCLASS [message]`
- `NOSPARE [message]`
- `NOTFULL [message]`
- `UNSAFE [message]`
- `SAME [message]`
- `ERROR` vs `ERROR [message]` (siehe 3.1)
- Crawler/Watch-Varianten derselben Tokens
- Shutdown: Conn close ist Erfolg, nicht Transport-Panic

Prefix-Match inkl. Message-Suffix: `NOSPARE source class has no spare pages` darf **nicht** `ErrMalformedResponse` werden.

### 3.5 Client-lokale Fehler (kein Wire)

Nicht mit Server-Tokens vermischen, aber in der Datei kurz inventarisieren:

`ErrInvalidKey`, `ErrInvalidValue`, `ErrInvalidArgument`, `ErrInvalidAddress`, `ErrInvalidNetworkProtocol`, `ErrNotSupported`, `ErrFlushAllNotAllowed`, `ErrAdminNotEnabled`, `ErrSetServersNotAllowed`, `ErrMixedTransport`, `ErrInsecureTLS`, `ErrDeadlineRequired`, `ErrTransport`, `ErrAdminTargetRequired`, `ErrMalformedResponse`, `ErrUnknownIndicator`.

`ErrMalformedResponse` nur für **undokumentiertes** oder kaputtes Framing. Nie für `NOT_STORED` / `EXISTS` / `BUSY foo`.

---

## 4. Code-Stellen (Ist-Zustand, gegen Spec halten)

Lies mindestens:

- `memcached/errors.go` — öffentliche Sentinels
- `memcached/protocol_builder.go` — `forecastCommonFaultLine`, `prefixedProtocolError`, `metaStatusCode`
- `memcached/protocol_response.go` — `read1`/`read2`/`mapStatusLine`, Quiet-Barrier, `forecastCommonFaultLine` auf jeder Zeile
- `memcached/protocol_storage.go` — `mapStorageReply`
- `memcached/protocol_get.go`, `protocol_incr.go`, `protocol_touch.go`, `protocol_delete.go`, `protocol_flush.go`
- `memcached/protocol_meta_*.go` — HD/VA/EN/NF/NS/EX/MN, Pipeline
- `memcached/protocol_admin.go` — `mapAdminKeyword`, `mapAdminStatus`, `adminStreamLineError`
- `memcached/protocol_stats.go` — Fehler in STAT-Streams
- `memcached/client_dispatch.go` — `isKeepaliveProtocolError`, `shouldDiscardConn`, `wrapRecvError`, `wrapTransport`
- `memcached/client_storage.go`, `client_get.go`, `client_incr.go`, `client_auth.go`, `client_admin*.go`
- Tests: `protocol_builder_test.go`, `errors_test.go`, `protocol_admin_test.go`, `protocol_storage_test.go`

Cluster: Teilfehler via `errors.Join` müssen `errors.Is` auf **jeden** enthaltenen Sentinel erlauben.

---

## 5. Pflicht-Prüfungen (Checkliste, jede Zeile abhaken)

Für **jedes** Token aus Abschnitt 3:

1. Erkennt der Parser die Zeile? (CRLF, LF, mit/ohne Message)
2. Welcher Sentinel kommt raus? `errors.Is`?
3. Bleibt die Server-Message in `err.Error()`?
4. Wird die Conn held oder discarded? Spec: nur schweres `SERVER_ERROR` schließt; `NOT_FOUND`/`EXISTS`/`CLIENT_ERROR` halten die Session.
5. Wird der Fehler geschluckt (`noreply`/`q` → `nil`)? Wenn ja: Spec oder Bug?
6. Wird ein dokumentierter Status als `ErrMalformedResponse` / `ErrUnknownIndicator` / `ErrNonexistentCommand` falsch klassifiziert?
7. Gibt die öffentliche Methode denselben Sentinel zurück, oder nur der interne Parser?
8. Meta-Pipeline: per-Key-Status vs Charge-`CLIENT_ERROR` vs Transport?
9. Fake-Server vs echter Server: unterdrückt der Fake `CLIENT_ERROR` bei `noreply`, obwohl der echte Server sie sendet?
10. Value-Payloads, die wie Statuszeilen aussehen (`set k 0 0 5` + `ERROR`) — Get darf nicht den Body als Fault lesen.

Bekannte Stolpersteine aus dem bestehenden Audit **nicht nachplappern**, sondern am **aktuellen** Code neu belegen:

- nacktes `ERROR` vs `ERROR <msg>`
- Admin-Suffixe `NOSPARE …` usw.
- `EN` vs `END`
- `CLIENT_ERROR` ohne Space / mit Message / trotz `noreply`
- `SERVER_ERROR` → Conn wegwerfen
- `Incr` non-numeric → `ErrClientError`, nicht Malformed
- Auth-Fail → `ErrClientError`, nicht Stored
- Meta `q` + `EX` muss Error bleiben
- `Get` Miss ≠ `GetOne` Miss

---

## 6. Ausgabeformat

Jeder Agent 1–4 schreibt **nur** seine Fragment-Datei. Nicht `13-errors.md` anfassen.

```text
docs/audit/fragments/errors-agent1-global-text.md
docs/audit/fragments/errors-agent2-meta.md
docs/audit/fragments/errors-agent3-admin-conn.md
docs/audit/fragments/errors-agent4-api-tests.md
docs/audit/13-errors.md              # nur AGENT 5
```

Kopf jeder Datei:

- Datum, Agent-Nummer, Modell **Grok 4.6 extra high fast**
- Spec-URL + Abgleich `docs/protocol.txt` vs Upstream
- Zählung im eigenen Scope: VOLLSTÄNDIG / TEILWEISE / FALSCH / FEHLT / BEWUSST AUSGELASSEN

Dann eine Tabelle im eigenen Scope:

| Token / Situation | Spec-Stelle | Command | Unser Sentinel | `errors.Is` | Message erhalten | Conn | Status | Code | Soll |

Status-Wörter nur: `VOLLSTÄNDIG` | `TEILWEISE` | `FALSCH` | `FEHLT` | `BEWUSST AUSGELASSEN`

Danach Findings, eines pro Token-Gruppe:

```
### <Titel>
- Status:
- Priorität: P0 | P1 | P2
- Kategorie: App | Admin | Meta | Infra
- Spec: URL — Abschnitt
- Code: datei:zeile
- Ist:
- Soll:
- Vorschlag: konkrete Signatur / Sentinel / Mapping
- Breaking: ja/nein — warum
```

Prio:

- **P0**: Caller kann Erfolg/Miss/CAS-Fail nicht unterscheiden, oder Conn desynct, oder dokumentierter Status wird Malformed
- **P1**: Admin-Tokens, Message verloren, `ERROR` vs `ERROR msg` verwechselt, Discard-Policy falsch
- **P2**: Benennung, fehlende Typed-Unterscheidung `EN` vs `NF`, Doku

AGENT 5 hängt an `13-errors.md` zusätzlich:

- Master-Tabelle über alle Tokens
- vorgeschlagene finale Sentinel-Liste (was bleibt, was neu, was weg)
- Mapping-Funktion die **alle** Wire-Tokens zentral können sollte (heute split: `forecastCommonFaultLine` + `mapAdminKeyword` + Meta-Parser)
- fehlende Tests, je Token eine Zeile: Input-Wire → `errors.Is` → Discard ja/nein

Querverweis nur, nicht doppelschreiben: Quiet/Desync kanonisch hier nur bzgl. Fehlerzuordnung; API-Umbau verweist auf `docs/audit/02-new-features.md` falls vorhanden.

---

## 7. Gemeinsamer Kopf

```text
Repo: Workspace-Root go-memcached. Package memcached/.

Modell: ausschließlich Grok 4.6 extra high fast (xhigh fast).
Kein anderes Modell. Kein Grok 4.6 fast high. Kein high max.

Auftrag: Error-Audit. Keine Commits, in diesem Lauf kein Anwendungscode ändern.
Findings NUR als Markdown in die Datei deines Agenten (Tabelle oben).
Kein Chat-only-Ergebnis. Kein „siehe oben, Datei spare ich mir“.
Chat: nur welche Dateien geschrieben/ergänzt wurden.

WICHTIG — pre-release, nicht in Benutzung:
Kein Legacy. Keine API-Stabilität. Breaking Changes ohne Stress vorschlagen.
Alte Fehlertypen nicht schonen. Nicht „Kompatibilität behalten“.

Zuerst fetchen:
https://docs.memcached.org/
https://docs.memcached.org/protocols/basic/
https://docs.memcached.org/protocols/meta/
https://raw.githubusercontent.com/memcached/memcached/master/doc/protocol.txt
https://github.com/memcached/memcached/wiki/MetaCommands

Lokale Spec: docs/protocol.txt gegen Upstream halten.
Wire/protocol.txt gewinnt gegen Wiki.

Jede Aussage: Datei:Zeile + Spec-URL + Token.
Status: VOLLSTÄNDIG | TEILWEISE | FALSCH | FEHLT | BEWUSST AUSGELASSEN
Prio: P0 | P1 | P2
Kategorie: App | Admin | Meta | Infra

Deutsch. Eintrags-Template aus dem Prompt verwenden.
Altes Audit (docs/audit/03, 05, 09) nicht nachplappern — am aktuellen Code neu belegen.
```

---

## AGENT 1 — Globale Error-Strings + Text-Status

```text
Rolle: Text-Protokoll-Fehler. Gemeinsamer Kopf gilt. Abschnitte 2, 3.1, 3.2, 5.

ERROR / ERROR <msg> / CLIENT_ERROR / SERVER_ERROR.
Storage: STORED NOT_STORED EXISTS NOT_FOUND.
Retrieval: END, Miss weglassen vs GetOne ErrNotFound, Value-Body der wie ERROR aussieht.
delete / incr / decr / touch / flush / version / stats / auth / quit.
noreply: CLIENT_ERROR kommt trotzdem.

Code: errors.go, protocol_builder.go, protocol_response.go, protocol_storage.go,
protocol_get.go, protocol_incr.go, protocol_touch.go, protocol_delete.go,
protocol_flush.go, client_storage.go, client_get.go, client_incr.go, client_auth.go.

Schreiben:
- docs/audit/fragments/errors-agent1-global-text.md
```

---

## AGENT 2 — Meta-Codes + Quiet/Pipeline

```text
Rolle: Meta-Fehler. Gemeinsamer Kopf gilt. Abschnitte 2, 3.3, 5 (Punkte 7–8).

HD VA EN NF NS EX MN. EN vs END. q + mn-Barriere.
EX / CLIENT_ERROR / SERVER_ERROR bleiben bei q.
Pipeline: per-Key-Status vs Charge-CLIENT_ERROR vs Transport.
Wiki-ST / Spec-DE: Wire ist HD.

Code: protocol_meta_*.go, client_meta_*.go, meta.go, protocol_response.go
(read2 collectFaults, endIndicatorQuietBarrier).

Schreiben:
- docs/audit/fragments/errors-agent2-meta.md
```

---

## AGENT 3 — Admin-Keywords + Conn-Discard

```text
Rolle: Admin- und Session-Fehler. Gemeinsamer Kopf gilt. Abschnitte 2, 3.4, 5 (Punkte 4–6).

BUSY BADCLASS NOSPARE NOTFULL UNSAFE SAME — Prefix plus Message.
ERROR vs ERROR <msg> bei lru/watch/crawler.
Shutdown: Close ist Erfolg.
shouldDiscardConn / isKeepaliveProtocolError / wrapRecvError / wrapTransport.
SERVER_ERROR → discard. NOT_FOUND / EXISTS / CLIENT_ERROR → keepalive.
Cluster errors.Join: errors.Is auf jeden Sentinel.

Code: protocol_admin.go, client_admin.go, client_admin_watch.go,
protocol_stats.go, client_dispatch.go.

Schreiben:
- docs/audit/fragments/errors-agent3-admin-conn.md
```

---

## AGENT 4 — Öffentliche API, lokale Fehler, Tests

```text
Rolle: API + Tests. Gemeinsamer Kopf gilt. Abschnitte 2.B, 3.5, 5 (Punkte 2, 7, 9, 10).

Wirft die öffentliche Methode denselben Sentinel wie der Parser?
Client-lokale Fehler nicht mit Wire-Tokens vermischen.
ErrMalformedResponse nie für dokumentierte Statuszeilen.
Fake-Server vs echter Server: noreply + CLIENT_ERROR.
Tests: protocol_builder_test.go, errors_test.go, protocol_admin_test.go,
protocol_storage_test.go, client_*_test.go — was fehlt pro Token.

Schreiben:
- docs/audit/fragments/errors-agent4-api-tests.md
```

---

## AGENT 5 — Synthese (erst nach 1–4)

```text
Rolle: Chefredakteur. Gemeinsamer Kopf gilt.

Nur die vier Fragment-Dateien lesen, nicht neu spekulieren.
Duplikate zusammenziehen. Widerspruch: Spec/Wire > klare API > aktueller Code.

Schreiben:
- docs/audit/13-errors.md     (Master-Tabelle + Findings + Sentinel-Liste + Tests)
- Fragmente nicht löschen
```

---

## 8. Ablauf

1. Vier Agenten parallel: Kopf + AGENT 1 / 2 / 3 / 4. Nur **Grok 4.6 extra high fast**.
2. AGENT 5 mit Kopf + den vier Fragmenten.
3. Fertig = die Markdown-Dateien unter `docs/audit/`. Pflicht: vier Fragmente plus `13-errors.md`. In diesem Lauf kein Patch am Client.

Nicht tun:

- Anwendungscode patchen
- Binary/SASL als Missing Feature
- „für Kompatibilität behalten“
- Tokens erfinden, die weder Spec noch Server-Source haben
- ein anderes Modell wählen, „weil xhigh busy ist“
- Findings nur im Chat lassen statt in die `.md` zu schreiben
