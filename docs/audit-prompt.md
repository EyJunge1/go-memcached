# go-memcached — Multi-Agent Audit Prompt

Copy **Gemeinsamer Kopf** plus genau einen **AGENT**-Block.

**Modell:** ausschließlich **Grok 4.6** (fast high, xhigh fast oder high max). Kein anderes Modell.

**Kein Anwendungscode in diesem Audit ändern.** Findings nur als Markdown unter `docs/audit/` schreiben.

**Kein Legacy.** Das Package ist **noch nicht in Benutzung**, keine produktiven Nutzer, keine SemVer-Garantie. Bestehende APIs, Signaturen, Fehlertypen, Options und internes Layout dürfen **komplett neu** vorgeschlagen werden. Kein „müssen wir so lassen wegen Kompatibilität“. Kein Schonmodus für alten Fork-Code (yeqown). Ziel ist der beste aktuelle Text/Meta-Client, nicht ein schonender Diff.

---

## Offizielle Quellen (zuerst fetchen)

### Memcached — Protokoll und Doku

| Was | URL |
| --- | --- |
| Doku-Start | https://docs.memcached.org/ |
| Protokolle (Meta empfohlen, Binary deprecated) | https://docs.memcached.org/protocols/ |
| Basic Text Protocol | https://docs.memcached.org/protocols/basic/ |
| Meta Text Protocol | https://docs.memcached.org/protocols/meta/ |
| Binary Protocol (nur um zu wissen, was **out of scope** ist) | https://docs.memcached.org/protocols/binary/ |
| Kanonische Spec `protocol.txt` | https://github.com/memcached/memcached/blob/master/doc/protocol.txt |
| Spec roh | https://raw.githubusercontent.com/memcached/memcached/master/doc/protocol.txt |
| Kopie im Repo (gegen Upstream prüfen) | `docs/protocol.txt` |
| Meta-Wiki | https://github.com/memcached/memcached/wiki/MetaCommands |
| Server-Repo | https://github.com/memcached/memcached |

Es gibt **keine offizielle Client-Library** vom Memcached-Projekt. Nur Server + Spec. libmemcached ist der de-facto C-Vergleich, nicht „ihre“ Library.

### Vergleichs-Clients

| Was | URL |
| --- | --- |
| libmemcached-awesome | https://github.com/awesomized/libmemcached |
| libmemcached Doku | https://awesomized.github.io/libmemcached/ |
| API-Index | https://awesomized.github.io/libmemcached/libmemcached/index.html |
| Behaviors | https://awesomized.github.io/libmemcached/libmemcached/memcached_behavior.html |
| Konfiguration / Ketama | https://awesomized.github.io/libmemcached/libmemcached/configuration.html |
| bradfitz/gomemcache | https://github.com/bradfitz/gomemcache |
| yeqown/memcached (Fork-Ursprung) | https://github.com/yeqown/memcached |

---

## Scope: alles außer Deprecated

**Reinpacken — jedes aktuelle Text- und Meta-Feature aus `protocol.txt`.**
App-Befehle, Admin/Ops, alle Meta-Flags, alle `stats`-Subcommands, optionale Tokens (`noreply`, `flush_all` Delay, …). Auch Client-Infrastruktur, die ein voller Client braucht (Pool, Hashing, Failover, TLS, Timeouts), wenn libmemcached oder die Spec sie hergeben.

**Raus — nicht als Lücke zählen, nur in `11-deprecated-out-of-scope.md` listen:**

- komplettes Binary-Protokoll ([docs](https://docs.memcached.org/protocols/binary/))
- SASL (hängt am Binary, von Memcached selbst als Problem genannt)
- TAP, VBucket, Range-Ops (`RGet`/`RSet`/… — nicht im Server)
- Serialisierung/Kompression als Library-Dependency (go-memcached macht bewusst `[]byte`)

Meta **ersetzt** Binary. Fehlt ein Binary-Opcode, aber Meta kann dasselbe: **kein Missing Feature**, höchstens Hinweis unter Deprecated.

---

## Stand: pre-release, alles darf kippen

go-memcached ist **nicht in Produktion**. Es gibt keine Downstream-Nutzer, die eine stabile API erwarten.

Daraus folgt für jeden Vorschlag:

- **Kein Legacy-Code verteidigen.** Fork-Reste, schiefe Signaturen, `ErrMalformedResponse` für `NOT_STORED`, globale `WithNoReply`, `Incr` das bei noreply `0` zurückgibt — weghauen oder neu schneiden, wenn die Spec oder eine klare API besser ist.
- **Breaking Changes sind der Default**, nicht die Ausnahme. Lieber eine saubere API vorschlagen als „optional hinten andocken“.
- **Umbenennen, zusammenlegen, löschen** von Methoden, Options, Typen ist erwünscht, wenn es die Spec klarer abbildet.
- **Nicht** schreiben: „bestehende Nutzer würden brechen“, „für Kompatibilität behalten“, „deprecated wrapper lassen“.
- **Doch** schreiben: ideale Signatur, welche alte API weg soll, warum.
- Internas (Pool, Parser, Cluster-Pick) dürfen neu gedacht werden. Kein Zwang, die aktuelle Struktur zu schonen.
- yeqown/memcached und bradfitz/gomemcache sind nur Vergleich, kein Vorbild das man nachbauen muss.

---

## Ausgabe: mehrere Markdown-Dateien

Jeder Agent **schreibt Dateien**, nicht nur Chat. Ordner: `docs/audit/`.

Anlegen, falls fehlend. Existierende Dateien **ergänzen** (nicht leer überschreiben), außer AGENT 5 darf konsolidieren.

```text
docs/audit/
  README.md                         # Index: Datei → Inhalt, wer geschrieben hat
  00-summary.md                     # nur AGENT 5
  01-missing-features.md            # jedes FEHLT / TEILWEISE aus der Spec
  02-new-features.md                # neue APIs; Breaking-Redesigns der bestehenden Oberfläche erlaubt
  03-bug-fixes.md                   # Bugs; Fix darf die öffentliche API ändern
  04-meta-flags.md                  # jedes Meta-Flag vs. protocol_meta.go
  05-admin-ops.md                   # slabs, lru*, watch, stats-args, verbosity, …
  06-client-infra.md                # Pool, Hashing, Failover, TLS, UDP, Timeouts
  07-optimizations.md               # Performance, Allokationen, Parser
  08-security.md
  09-tests.md                       # fehlende Spec-Fälle
  10-readme-corrections.md          # README/doc.go vs Realität
  11-deprecated-out-of-scope.md     # bewusst nicht bauen
  12-roadmap.md                     # nur AGENT 5; Breaking in Iteration 1 ist OK
```

**Pro Eintrag (Pflichtfelder):**

```markdown
### <Kurzname>
- Status: VOLLSTÄNDIG | TEILWEISE | FEHLT | FALSCH | BEWUSST AUSGELASSEN
- Priorität: P0 | P1 | P2
- Kategorie: App | Admin | Infra | Meta
- Spec: <URL> — <Abschnitt>
- Code: <Datei:Zeile> oder „fehlt“
- Ist: …
- Soll: …
- Vorschlag: ideale öffentliche Go-Signatur oder Fix-Idee (Breaking OK, kein Schon-API)
- Breaking: ja/nein — wenn ja, kurz warum die alte Form weg kann
```

Status-Wörter genau so. Deutsch. Tabellen erlaubt.

---

## Checkliste — vollständig prüfen (nicht kürzen)

Aus `protocol.txt` / [Basic](https://docs.memcached.org/protocols/basic/) / [Meta](https://docs.memcached.org/protocols/meta/) abhaken. Fehlt etwas in der Spec, das hier nicht steht: **trotzdem aufnehmen**.

### Storage
- [ ] `set` / `add` / `replace` / `append` / `prepend` / `cas`
- [ ] optionales `noreply` bei allen
- [ ] Responses: `STORED`, `NOT_STORED`, `EXISTS`, `NOT_FOUND` — nicht als Malformed wrappen

### Retrieval
- [ ] `get` / `gets` / `gat` / `gats` (ein oder viele Keys)
- [ ] `VALUE`-Blöcke per Byte-Länge, nicht per `\r\n` im Value
- [ ] `END`, Missing Keys weglassen vs. `ErrNotFound`

### Sonstige Item-Befehle
- [ ] `delete` + `noreply`
- [ ] `incr` / `decr` + `noreply` (Text: kein Initialwert)
- [ ] `touch` + `noreply`
- [ ] `flush_all` + **Delay** + `noreply`
- [ ] Expiration: `0` = nie; ≤30 Tage relativ; >30 Tage Unix-Timestamp

### Meta (vollständig, jedes Flag)
- [ ] `mg` `ms` `md` `ma` `me` `mn`
- [ ] alle Flags und Token-Argumente laut Spec/Wiki
- [ ] Flag `b` (Base64-Key), `q` (quiet), `O` (opaque), `k` (key zurück)
- [ ] Recache / stale / vivify (`N`, `R`, `W`, `X`, `N` bei ms, … — exakt Spec)
- [ ] `ma`: Mode incr/decr, Initial `J`, Autocreate `N`
- [ ] Responses: `HD` `EN` `NF` `NS` `EX` `VA` …

### Admin / Ops (reinpacken, nicht unter den Teppich)
- [ ] `stats` und **alle** Subcommands aus der Spec (`items`, `slabs`, `settings`, `sizes`, `conns`, …)
- [ ] `version`
- [ ] `verbosity`
- [ ] `quit`
- [ ] `shutdown` (falls in aktueller Spec)
- [ ] `cache_memlimit`
- [ ] `slabs reassign` / `slabs automove`
- [ ] `lru tune|mode|temp_ttl`
- [ ] `lru_crawler` enable/disable/sleep/tocrawl/crawl/metadump/mgdump
- [ ] `watch` (fetchers, mutations, evictions, connevents, proxy*, deletions)

### Client-Infra (Spec + libmemcached, ohne Binary/SASL)
- [ ] TCP / UDP / Unix
- [ ] TLS (Dialer reicht oder First-Class?)
- [ ] Pool, Idle, Lifetime, Keepalive
- [ ] Timeouts: Dial, Read, Write, Deadline pro Op
- [ ] Cluster-Hash: crc32 / murmur3 / rendezvous vs Ketama / Weights
- [ ] Failover, Dead-Server, Retry
- [ ] Multi-Get / Pipeline pro Node
- [ ] `SetServers` / dynamische Node-Liste
- [ ] Keys: 1–250 Bytes, keine Whitespace/Control (außer Meta `b`)

### Korrektheit (immer mitprüfen)
- [ ] Context-Cancel während send/recv
- [ ] noreply: keine Response lesen, keine Pipeline-Korruption
- [ ] Fehler: `errors.Is` für `NOT_FOUND`, `EXISTS`, `NOT_STORED`, Transport
- [ ] Cluster: partielle Fehler (`errors.Join`), FlushAll/Ping/Stats alle Nodes?

---

## Was go-memcached bereits behauptet (Code = Wahrheit)

Text: set/add/replace/append/prepend/cas, get/gets/gat/gats, delete, incr/decr, touch, version, stats, flush_all, ping, SetServers.

Meta: ms/mg/md/ma/me/mn inkl. vieler Flags.

Transport: TCP, UDP, Unix; Pool; Cluster (crc32, murmur3, rendezvous).

Werte: `[]byte` + `uint32` flags. SASL entfernt. Kein Binary. Kein Raw-Command.

API-Dateien: `client.go`, `client_commands.go`, `protocol.go`, `protocol_builder.go`, `protocol_meta.go`, `options.go`, `conn.go`, `client_cluster.go`, `errors.go`.

---

## Gemeinsamer Kopf

```text
Repo: Workspace-Root go-memcached. Package memcached/.

Modell: nur Grok 4.6 (fast high / xhigh fast / high max).

Auftrag: Audit. Keine Commits, in diesem Lauf kein Anwendungscode ändern.
Findings NUR nach docs/audit/*.md schreiben (siehe Dateiliste im Prompt).
Chat: kurz sagen, welche Dateien geschrieben/ergänzt wurden.

WICHTIG — pre-release, nicht in Benutzung:
Kein Legacy. Keine API-Stabilität. Breaking Changes ohne Stress vorschlagen.
Alte Signaturen, Fehlertypen, Options, interne Struktur nicht schonen.
Nicht „Kompatibilität behalten“. Idealzustand beschreiben, was weg soll dazu.

Zuerst fetchen:
https://docs.memcached.org/protocols/
https://docs.memcached.org/protocols/basic/
https://docs.memcached.org/protocols/meta/
https://raw.githubusercontent.com/memcached/memcached/master/doc/protocol.txt
https://github.com/memcached/memcached/wiki/MetaCommands
https://awesomized.github.io/libmemcached/libmemcached/index.html
https://awesomized.github.io/libmemcached/libmemcached/memcached_behavior.html
https://github.com/bradfitz/gomemcache
https://github.com/yeqown/memcached

Lokale Spec: docs/protocol.txt gegen Upstream halten.

ALLES aus der aktuellen Text+Meta-Spec prüfen (Checkliste im Prompt).
Nichts auslassen außer Deprecated (Binary, SASL, TAP, VBucket, Range-Ops).
Die stehen nur in 11-deprecated-out-of-scope.md.

Jede Aussage: Datei:Zeile + Spec-URL.
Status: VOLLSTÄNDIG | TEILWEISE | FEHLT | FALSCH | BEWUSST AUSGELASSEN
Prio: P0 | P1 | P2
Kategorie: App | Admin | Infra | Meta

Deutsch. Eintrags-Template aus dem Prompt verwenden.
docs/audit/README.md als Index aktuell halten.
```

---

## AGENT 1 — Protokoll-Vollständigkeit

```text
Rolle: Protokoll-Anwalt. Gemeinsamer Kopf gilt.

Spec komplett durchgehen. Jeden Befehl und jedes Meta-Flag gegen
client_commands.go, protocol_builder.go, protocol.go, protocol_meta.go.

Besonders: flush_all Delay, alle stats-Args, noreply-Responses,
Expiration-Regel, VALUE-Länge, Meta-Flag-Vollständigkeit.

API neu schneiden wenn die aktuelle Form die Spec verbiegt
(z. B. eine Error-Typ-Hierarchie statt alles als Malformed).
Bestehende Methoden umbenennen/löschen ist OK.

Schreiben / ergänzen:
- docs/audit/01-missing-features.md   (jedes FEHLT/TEILWEISE)
- docs/audit/02-new-features.md       (API-Vorschläge für die Lücken)
- docs/audit/04-meta-flags.md         (Flag-für-Flag)
- docs/audit/05-admin-ops.md
- docs/audit/10-readme-corrections.md (nur Protokoll-Behauptungen)
- docs/audit/11-deprecated-out-of-scope.md
- docs/audit/README.md
```

---

## AGENT 2 — Client-Vergleich (libmemcached + Go)

```text
Rolle: API-Vergleich. Gemeinsamer Kopf gilt.

libmemcached und die zwei Go-Clients gegen go-memcached.
Binary/SASL-Behaviors von libmemcached nicht als Missing zählen.

Cluster: crc32/murmur3/rendezvous vs Ketama/Weights.
Wenn Ketama der bessere Default für bestehende Memcached-Pools ist:
Default wechseln vorschlagen, alten Hash nicht aus Kompatibilität behalten.

Schreiben / ergänzen:
- docs/audit/06-client-infra.md
- docs/audit/01-missing-features.md   (nur Infra/Komfort, das Spec oder libmemcached rechtfertigt)
- docs/audit/02-new-features.md
- docs/audit/11-deprecated-out-of-scope.md
- docs/audit/README.md
```

---

## AGENT 3 — Bugs, Robustheit, Tests

```text
Rolle: Implementierungs-Review. Gemeinsamer Kopf gilt. Nur bestehender Code.

Pool, UDP, Timeouts, noreply, Parser, errors.Is, Expiration,
Add/Replace/Cas-Responses, Incr+noreply=0, Cluster-Teilfehler, Tests.

Fixes dürfen API und Internas neu schneiden. Kein „workaround damit
die alte Signatur bleibt“. Lieber Breaking-Fix + neue Signatur.

Schreiben / ergänzen:
- docs/audit/03-bug-fixes.md
- docs/audit/07-optimizations.md
- docs/audit/09-tests.md
- docs/audit/README.md
```

---

## AGENT 4 — Sicherheit und Scope

```text
Rolle: Threat + Scope. Gemeinsamer Kopf gilt.

TLS, Keys/Injection, Stats/Watch/Dumps, Admin-Befehle.
README vs Realität. Security-Fixes dürfen die API brechen
(eigene Error-Typen, strengere Key-Validierung, Admin hinter
expliziten Methoden statt „heimlich weglassen“).

Schreiben / ergänzen:
- docs/audit/08-security.md
- docs/audit/10-readme-corrections.md
- docs/audit/11-deprecated-out-of-scope.md
- docs/audit/README.md
```

---

## AGENT 5 — Synthese (erst nach 1–4)

```text
Rolle: Chefredakteur. Gemeinsamer Kopf gilt.

Die Dateien unter docs/audit/ lesen, nicht neu spekulieren.
Duplikate zusammenziehen. Widerspruch: Spec > klare API > aktueller Code.
Kein Kompatibilitäts-Kompromiss. Roadmap darf die öffentliche Fläche
in Iteration 1 brechen.

Schreiben:
- docs/audit/00-summary.md
- docs/audit/12-roadmap.md            (3 Iterationen, P0 zuerst)
- docs/audit/README.md                (finaler Index)
- 01–11 bei Bedarf nur entdoppeln / Prioritäten angleichen
```

---

## Ablauf

1. Vier Agenten parallel: Kopf + AGENT 1 / 2 / 3 / 4. Nur Grok 4.6.
2. AGENT 5 mit Kopf + den geschriebenen `docs/audit/*.md`.
3. Fertig = die Dateien unter `docs/audit/`. In diesem Lauf kein Patch am Client.
   Die Vorschläge selbst dürfen die spätere API hart umbauen.
