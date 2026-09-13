# Auftrag: Memcached-Feature-Inventar vs. go-memcached

Copy **Gemeinsamer Kopf** plus genau einen **AGENT**-Block.

**Modell:** ausschließlich **Grok 4.6 extra high fast** (xhigh fast). Kein anderes Modell. Kein Grok 4.6 fast high, kein high max, kein anderes Haus.

Vier Agenten parallel (1–4), danach AGENT 5. Jeder Agent schreibt nur in seine Dateien. AGENT 5 konsolidiert nach `docs/audit/14-features.md`.

Kein Anwendungscode ändern.

**Findings nur in Markdown-Dateien.** Chat ist kein Deliverable: kein Inventar, keine Tabelle, keine Flag-Liste nur in der Antwort. Jeder Agent **legt oder ergänzt** seine `.md` und schreibt jedes Finding dort. Im Chat nur: welche Dateien geschrieben wurden.

| Wer | Datei |
| --- | --- |
| AGENT 1 | `docs/audit/fragments/features-agent1-text.md` |
| AGENT 2 | `docs/audit/fragments/features-agent2-meta.md` |
| AGENT 3 | `docs/audit/fragments/features-agent3-admin.md` |
| AGENT 4 | `docs/audit/fragments/features-agent4-auth-udp.md` |
| AGENT 5 | `docs/audit/14-features.md` |

Ordner anlegen falls fehlend. Existierende Fragmente **ergänzen**, nicht leer überschreiben, außer du bist derselbe Agent und ersetzt den eigenen Lauf.

Sprache: Deutsch. Spec-Zitate, Command-Namen und Flags bleiben Englisch.

Pre-release, kein Legacy-Schonmodus. Breaking Changes vorschlagen ist erlaubt. yeqown und gomemcache sind nur Vergleich, kein Vorbild.

**Nicht** das alte Audit (`docs/audit/01-missing-features.md`, `04`, `05`) nachplappern. Die Dateien dürfen als Hinweis dienen, aber jede Claim muss am **aktuellen** Code und an der **aktuellen** Spec neu belegt werden. README/`doc.go` sind Ist-Behauptungen, keine Spec.

---

## 1. Zuerst Spec fetchen (nicht aus dem Gedächtnis)

In dieser Reihenfolge. Jede Claim braucht URL + Abschnitt + Command/Flag.

| Was | URL |
| --- | --- |
| Doku-Start | https://docs.memcached.org/ |
| Protokolle (Meta empfohlen, Binary deprecated) | https://docs.memcached.org/protocols/ |
| Basic Text | https://docs.memcached.org/protocols/basic/ |
| Meta Text | https://docs.memcached.org/protocols/meta/ |
| Binary (nur um zu wissen, was **raus** ist) | https://docs.memcached.org/protocols/binary/ |
| Kanonische Spec | https://github.com/memcached/memcached/blob/master/doc/protocol.txt |
| Spec roh | https://raw.githubusercontent.com/memcached/memcached/master/doc/protocol.txt |
| Repo-Kopie (gegen Upstream diffen) | `docs/protocol.txt` |
| Meta-Wiki | https://github.com/memcached/memcached/wiki/MetaCommands |
| Server-Repo | https://github.com/memcached/memcached |

Wenn Spec und Wiki widersprechen: **Wire / `protocol.txt` gewinnt**. Wiki-Abweichung extra markieren.

`docs/protocol.txt` gegen Upstream vergleichen (Inhalt, nicht nur Datum). Weicht die Kopie ab: das in den Kopf von `14-features.md` schreiben und **Upstream** als Inventar-Quelle nutzen.

Es gibt **keine** offizielle Client-Library vom Memcached-Projekt. libmemcached ist Vergleich für Cluster/Behaviors, nicht die Spec.

---

## 2. Scope

### Rein — jedes aktuelle Text- und Meta-Feature

Aus `protocol.txt` plus Basic/Meta-Doku:

- alle Storage-, Retrieval- und Other-Commands
- alle Meta-Commands (`mg` `ms` `md` `ma` `me` `mn`) **und jedes dokumentierte Request-/Response-Flag**
- optionale Tokens (`noreply`, `cas`, `exptime`, `flush_all` Delay, `shutdown graceful`, …)
- alle dokumentierten `stats`-Argumente, die der Server noch anbietet
- Admin/Ops: slabs, lru, lru_crawler, watch, dumps, verbosity, cache_memlimit, shutdown
- Token-Auth `-Y` (Text, nicht SASL)
- UDP-Framing, soweit die Spec es für Text beschreibt
- Expiration-Regel (0 / relativ ≤30 Tage / Unix / negativ)

„Haben“ heißt: **öffentliche API kann den Wire-Befehl mit den dokumentierten Optionen senden und die dokumentierten Antworten lesen.** Nur interner Builder ohne Caller-Pfad = `TEILWEISE` oder `FEHLT`.

Ein Feature zählt nicht als vollständig, wenn:

- der Builder das Token kennt, die öffentliche Methode es aber nie setzt
- nur ein Teil der Spec-Optionen erreichbar ist (z. B. nur Single-Key, obwohl die Spec `key*` erlaubt)
- ein Flag sendbar ist, die Response aber nicht geparst wird
- README ✅ sagt, der Code den Befehl aber hinter einem anderen Namen / Guard versteckt und die Spec-Semantik verbiegt

### Raus — nicht als Lücke zählen

Nur in einem kurzen Block `BEWUSST AUSGELASSEN` listen, mit Spec-Beleg warum:

- komplettes Binary-Protokoll
- SASL (hängt am Binary)
- TAP, VBucket, Range-Ops (`RGet`/`RSet`/… — nicht im aktuellen Server)
- Serialisierung/Kompression als Library-Dependency (Werte sind `[]byte`)
- `misbehave` (Debug/seccomp-Hook)
- `stats sizes_enable` / `sizes_disable` (Spec: Runtime-Toggle entfernt)
- `stats cachedump` (Spec empfiehlt `metadump`)
- Wiki-Response `ST` (Wire ist `HD`)
- libmemcached-only: Binary-Replicas, Ketama-SPY, VBucket-Map, Config-File-Sprache, `WithCodec`, OTel im Core

Meta **ersetzt** Binary. Fehlt ein Binary-Opcode, Meta kann dasselbe: **kein Missing Feature**.

Client-Infrastruktur (Pool, Ketama, TLS, Failover) ist **kein** Spec-Command. Nur erwähnen, wenn die Spec ein Transport-Feature vorschreibt (UDP-Header, TLS-Stats). Kein zweites Infra-Audit.

Fehler-Mapping (`ERROR` / `CLIENT_ERROR` / `NOT_FOUND` …) gehört **nicht** hierher. Dafür gibt es `docs/audit-errors-prompt.md`. Hier nur: kann der Befehl gesendet und seine **Erfolgs-/Status-Antworten** gelesen werden?

---

## 3. Inventar zuerst aus der Spec, dann Code

Baue **eine vollständige Command-/Flag-Liste aus der Spec**, bevor du den Code bewertest. Nichts auslassen. Nicht implementiert + in Scope = `FEHLT`. Nicht implementiert + Deprecated = `BEWUSST AUSGELASSEN`.

### 3.1 Text — Storage

| Command | Spec-Optionen die zählen |
| --- | --- |
| `set` | flags, exptime, bytes, optional `noreply` |
| `add` | wie set |
| `replace` | wie set |
| `append` | kein flags/exptime auf dem Wire (Spec) |
| `prepend` | wie append |
| `cas` | flags, exptime, bytes, cas-unique, optional `noreply` |

### 3.2 Text — Retrieval

| Command | Spec-Optionen die zählen |
| --- | --- |
| `get` | ein **oder mehrere** Keys, kein CAS |
| `gets` | ein oder mehrere Keys, mit CAS |
| `gat` | exptime + ein oder mehrere Keys, kein CAS |
| `gats` | exptime + ein oder mehrere Keys, mit CAS |

Multi-Key in **einem** Request pro Node zählt. N×`get` ist nicht `get key*`.

### 3.3 Text — Mutation ohne Body

| Command | Spec-Optionen die zählen |
| --- | --- |
| `delete` | key, optional `noreply` |
| `incr` | key, delta, optional `noreply` (noreply liefert **keine** Zahl) |
| `decr` | wie incr |
| `touch` | key, exptime, optional `noreply` |

### 3.4 Meta — Commands + Flags

Commands: `mg`, `ms`, `md`, `ma`, `me`, `mn`.

Jedes in der Spec genannte Request-Flag und jedes Response-Extra, mindestens:

Request: `b` `c` `C` `f` `F` `h` `k` `l` `O` `q` `s` `t` `u` `v` `E` `N` `R` `T` `I` `M` `J` `D` `x` `P` `L`

Response-Extras: `W` `X` `Z` plus echo von `k` `b` `c` `f` `s` `t` `O` …

Response-Codes: `HD` `VA` `EN` `NF` `NS` `EX` `MN` `ME` — als Status lesbar, nicht nur intern.

Pro Flag: gilt es für welchen Command? Sendbar? Response geparst? Ungültige Kombination vom Client blockiert oder als `CLIENT_ERROR` durchgelassen?

`P`/`L` sind Spec-No-Ops für den Daemon, aber **dokumentierte** Tokens — müssen sendbar sein, sonst `FEHLT`.

`q` ohne `mn`-Barriere zählt nicht als vollständig.

### 3.5 Admin / Ops

| Command | Spec-Optionen die zählen |
| --- | --- |
| `slabs reassign` | src (inkl. `-1`), dest |
| `slabs automove` | `0\|1\|2` |
| `lru tune` | vier Zahlen |
| `lru mode` | `flat\|segmented` |
| `lru temp_ttl` | int, negativ = aus |
| `lru_crawler enable\|disable` | |
| `lru_crawler sleep` | 0…1_000_000 µs |
| `lru_crawler tocrawl` | uint32 |
| `lru_crawler crawl` | classids oder `all` |
| `lru_crawler metadump` | classids / `all` / `hash` |
| `lru_crawler mgdump` | classids / `all` / `hash` |
| `watch` | alle dokumentierten Topics, stapelbar |
| `stats` | ohne Args + dokumentierte Subcommands |
| `flush_all` | optional Delay (Expiration-Regel), optional `noreply` |
| `cache_memlimit` | MB, optional `noreply` |
| `shutdown` | default + `graceful` |
| `version` | |
| `verbosity` | level, optional `noreply` |
| `quit` | Conn schließen reicht; explizites `quit` optional |

`stats`-Subcommands aus der Spec (Settings / Items / Sizes / Slabs / Conns / TLS / Extra-Args wie `reset`, `detail …`). Freie Args sind ok, wenn der Server sie akzeptiert. Entfernte Toggles nicht als Lücke.

Watch-Topics aus der Spec, nicht aus dem Gedächtnis: `fetchers`, `mutations`, `evictions`, `connevents`, `proxyreqs`, `proxyevents`, `proxyuser`, `deletions` — plus alles, was Upstream neu listet.

### 3.6 Auth, UDP, Expiration (Spec, kein Infra-Essay)

- Auth `-Y`: Fake-`set` mit User/Pass; `STORED` / `CLIENT_ERROR`. Key/Flags/Exptime werden nur bei `-Y` ignoriert.
- UDP: 8-Byte-Header, Request muss in **ein** Datagramm, reserved = 0. Welche Commands die Spec für UDP sinnvoll macht, prüfen — große Sets/Multi-Gets sind Spec-mäßig TCP.
- Expiration: eine Regel für Item-TTL, Meta-`T`/`N`/`R`, `touch`/`gat`, `flush_all`-Delay.

---

## 4. Code-Stellen (Ist, gegen Spec halten)

Öffentliche Fläche zuerst, dann Builder:

- `memcached/doc.go`, `README.md` — Behauptungen
- `memcached/client.go`, `config.go`, `options.go`
- `memcached/client_storage.go`, `client_get.go`, `client_delete.go`, `client_incr.go`, `client_touch.go`
- `memcached/client_meta_*.go`, `memcached/meta.go`
- `memcached/client_stats.go`, `client_version.go`, `client_flush.go`
- `memcached/client_admin.go`, `client_admin_watch.go`, `client_auth.go`
- `memcached/protocol_*.go` — was der Wire-Builder wirklich emittiert
- `memcached/exptime.go` — 30-Tage-Regel

Grep nach Command-Strings: `"set"`, `"add"`, `"replace"`, `"append"`, `"prepend"`, `"cas"`, `"get"`, `"gets"`, `"gat"`, `"gats"`, `"delete"`, `"incr"`, `"decr"`, `"touch"`, `"mg"`, `"ms"`, `"md"`, `"ma"`, `"me"`, `"mn"`, `"flush_all"`, `"version"`, `"stats"`, `"verbosity"`, `"shutdown"`, `"cache_memlimit"`, `"slabs"`, `"lru"`, `"lru_crawler"`, `"watch"`, `"quit"`.

Jedes Meta-Flag-Byte im Request-Builder und im Response-Parser.

---

## 5. Pflicht-Prüfungen

Für **jedes** Inventar-Item:

1. Gibt es eine öffentliche Methode (oder dokumentiertes Options-Feld), die den Befehl sendet?
2. Gehen **alle** Spec-Optionen auf die Leitung, nicht nur der Happy Path?
3. Ist Multi-Key wirklich ein Request (`get k1 k2`), kein Loop?
4. Werden optionale Tokens (`noreply`, `graceful`, Delay, `hash` bei Dump) erreichbar, ohne globale Seiteneffekte?
5. Admin hinter Opt-in ist ok — zählt als „drin“, wenn `Admin()` den Befehl hat. Fehlt der Befehl auch dort → `FEHLT`.
6. Meta-Flag: sendbar **und** Response gelesen? Nur eins von beiden → `TEILWEISE`.
7. README-Tabelle vs Code: ✅ ohne API = Finding unter Doku, nicht unter Feature-da.
8. Fake-Server-only ohne Client-API = nicht vorhanden.

Status-Wörter nur: `VOLLSTÄNDIG` | `TEILWEISE` | `FEHLT` | `BEWUSST AUSGELASSEN`

`FALSCH` nur wenn die öffentliche Form die Spec **verbiegt** (z. B. `Get` sendet immer `gets`; `Append` schickt Dummy-Flags; `Incr`+Quiet gibt eine Zahl zurück). Sonst `TEILWEISE`/`FEHLT`.

Prio:

- **P0**: Kern-App-Befehl fehlt oder Spec-Semantik ist falsch (Storage/Retrieval/Incr/CAS/Meta-Kernflags)
- **P1**: Admin/Stats/Watch/Dumps/optionale Tokens/Meta-Nebenflags
- **P2**: No-Op-Hints (`P`/`L`), `quit`, `noreply` an Admin-Zeilen, Doku-Drift

---

## 6. Ausgabeformat

Jeder Agent 1–4 schreibt **nur** seine Fragment-Datei. Nicht `14-features.md` anfassen.

```text
docs/audit/fragments/features-agent1-text.md
docs/audit/fragments/features-agent2-meta.md
docs/audit/fragments/features-agent3-admin.md
docs/audit/fragments/features-agent4-auth-udp.md
docs/audit/14-features.md            # nur AGENT 5
```

Kopf jeder Datei:

- Datum, Agent-Nummer, Modell **Grok 4.6 extra high fast**
- Spec-URL + Abgleich `docs/protocol.txt` vs Upstream (gleich / Diff-Kurz)
- Zählung im eigenen Scope: VOLLSTÄNDIG / TEILWEISE / FEHLT / BEWUSST AUSGELASSEN
- Agent 5 zusätzlich: sind wir feature-complete für Text+Meta außer Deprecated? **ja / nein**

Dann eine Tabelle im eigenen Scope:

| Feature (Command/Flag) | Spec-Stelle | Öffentliche API | Wire wirklich | Status | Code | Lücke |

Danach nur die Nicht-`VOLLSTÄNDIG`-Items als Findings:

```
### <Command oder Flag>
- Status:
- Priorität: P0 | P1 | P2
- Kategorie: App | Meta | Admin | Transport
- Spec: URL — Abschnitt
- Code: datei:zeile
- Ist:
- Soll:
- Vorschlag: konkrete Signatur / Flag-Feld
- Breaking: ja/nein — warum
```

AGENT 5 hängt an `14-features.md` zusätzlich:

- Master-Tabelle über alle Commands/Flags
- Liste `BEWUSST AUSGELASSEN` (kurz, mit Grund)
- „Nächste Lücken nach Prio“ — max 15 Zeilen, keine Infra-Wunschliste
- README-Zellen die lügen (✅ vs `FEHLT`/`TEILWEISE`)

---

## 7. Gemeinsamer Kopf

```text
Repo: Workspace-Root go-memcached. Package memcached/.

Modell: ausschließlich Grok 4.6 extra high fast (xhigh fast).
Kein anderes Modell. Kein Grok 4.6 fast high. Kein high max.

Auftrag: Feature-Audit. Keine Commits, in diesem Lauf kein Anwendungscode ändern.
Findings NUR als Markdown in die Datei deines Agenten (Tabelle oben).
Kein Chat-only-Ergebnis. Kein „siehe oben, Datei spare ich mir“.
Chat: nur welche Dateien geschrieben/ergänzt wurden.

WICHTIG — pre-release, nicht in Benutzung:
Kein Legacy. Breaking Changes ohne Stress vorschlagen.
Nicht „Kompatibilität behalten“.

Zuerst fetchen:
https://docs.memcached.org/
https://docs.memcached.org/protocols/
https://docs.memcached.org/protocols/basic/
https://docs.memcached.org/protocols/meta/
https://docs.memcached.org/protocols/binary/
https://raw.githubusercontent.com/memcached/memcached/master/doc/protocol.txt
https://github.com/memcached/memcached/wiki/MetaCommands

Lokale Spec: docs/protocol.txt gegen Upstream halten.
Wire/protocol.txt gewinnt gegen Wiki.

ALLES aus der aktuellen Text+Meta-Spec im eigenen Agent-Scope.
Nichts auslassen außer Deprecated (Binary, SASL, TAP, VBucket, Range-Ops).
Die stehen nur als BEWUSST AUSGELASSEN.

Jede Aussage: Datei:Zeile + Spec-URL + Command/Flag.
Status: VOLLSTÄNDIG | TEILWEISE | FEHLT | BEWUSST AUSGELASSEN
FALSCH nur wenn die öffentliche Form die Spec verbiegt.
Prio: P0 | P1 | P2
Kategorie: App | Meta | Admin | Transport

Deutsch. Eintrags-Template aus dem Prompt verwenden.
Altes Audit (docs/audit/01, 04, 05) nicht nachplappern — am aktuellen Code neu belegen.
README/doc.go sind Ist-Behauptungen, keine Spec.
Fehler-Sentinels nicht hier auditieren (docs/audit-errors-prompt.md).
```

---

## AGENT 1 — Text Storage / Retrieval / Mutation

```text
Rolle: Text-App-Befehle. Gemeinsamer Kopf gilt. Abschnitte 2, 3.1, 3.2, 3.3, 5.

set add replace append prepend cas + flags/exptime/noreply.
get gets gat gats — Multi-Key in einem Request, CAS opt-in.
delete incr decr touch + noreply (Incr+Quiet liefert keine Zahl).
Append/Prepend ohne Dummy-Flags auf dem Wire.

Code: client_storage.go, client_get.go, client_delete.go, client_incr.go,
client_touch.go, protocol_storage.go, protocol_get.go, protocol_incr.go,
protocol_touch.go, protocol_delete.go, exptime.go.

Schreiben:
- docs/audit/fragments/features-agent1-text.md
```

---

## AGENT 2 — Meta Commands + jedes Flag

```text
Rolle: Meta-Vollständigkeit. Gemeinsamer Kopf gilt. Abschnitt 3.4, 5 (Punkt 6).

mg ms md ma me mn.
Jedes Request-Flag: b c C f F h k l O q s t u v E N R T I M J D x P L.
Response-Extras W X Z und Echo k b c f s t O.
Response-Codes HD VA EN NF NS EX MN ME als Status lesbar.
q ohne mn-Barriere ist nicht vollständig. P/L müssen sendbar sein.

Code: client_meta_*.go, meta.go, protocol_meta_*.go.

Schreiben:
- docs/audit/fragments/features-agent2-meta.md
```

---

## AGENT 3 — Admin / Stats / Watch / Other

```text
Rolle: Admin- und Ops-Befehle. Gemeinsamer Kopf gilt. Abschnitt 3.5.

slabs reassign/automove, lru tune/mode/temp_ttl,
lru_crawler enable/disable/sleep/tocrawl/crawl/metadump/mgdump,
watch (alle Spec-Topics, stapelbar),
stats + dokumentierte Subcommands,
flush_all Delay+noreply, cache_memlimit, shutdown [graceful],
version, verbosity, quit.

Admin hinter Opt-in zählt als drin, wenn Admin() den Befehl hat.

Code: client_admin.go, client_admin_watch.go, client_stats.go,
client_flush.go, client_version.go, protocol_admin.go, protocol_stats.go,
protocol_flush.go.

Schreiben:
- docs/audit/fragments/features-agent3-admin.md
```

---

## AGENT 4 — Auth, UDP, Expiration, README vs Code

```text
Rolle: Transport-Spec + Doku-Drift. Gemeinsamer Kopf gilt. Abschnitt 3.6, 5 (Punkte 7–8).

Auth -Y (Text, nicht SASL). UDP-Header, ein Datagramm, reserved=0,
welche Commands UDP laut Spec tragen darf.
Expiration eine Regel: Item, Meta T/N/R, touch/gat, flush_all-Delay.
README-Tabelle und doc.go gegen dieselbe Feature-Liste: lügt eine ✅-Zelle?

Kein Pool/TLS/Ketama-Essay. Kein Error-Sentinel-Audit.

Schreiben:
- docs/audit/fragments/features-agent4-auth-udp.md
```

---

## AGENT 5 — Synthese (erst nach 1–4)

```text
Rolle: Chefredakteur. Gemeinsamer Kopf gilt.

Nur die vier Fragment-Dateien lesen, nicht neu spekulieren.
Duplikate zusammenziehen. Widerspruch: Spec/Wire > klare API > aktueller Code.

Schreiben:
- docs/audit/14-features.md   (Master-Tabelle + ja/nein + Lücken + README-Lügen)
- Fragmente nicht löschen
```

---

## 8. Ablauf

1. Vier Agenten parallel: Kopf + AGENT 1 / 2 / 3 / 4. Nur **Grok 4.6 extra high fast**.
2. AGENT 5 mit Kopf + den vier Fragmenten.
3. Fertig = die Markdown-Dateien unter `docs/audit/`. Pflicht: vier Fragmente plus `14-features.md`. In diesem Lauf kein Patch am Client.

Nicht tun:

- Anwendungscode patchen
- Binary/SASL als Missing Feature
- Pool/TLS/Ketama als Memcached-Command verkaufen
- Fehler-Sentinels hier neu auditieren
- Tokens erfinden, die weder Spec noch Server-Source haben
- „für Kompatibilität behalten“
- ein anderes Modell wählen, „weil xhigh busy ist“
- Findings nur im Chat lassen statt in die `.md` zu schreiben
