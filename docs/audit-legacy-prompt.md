# Auftrag: Legacy- und Rückwärtskompatibilität rauswerfen

Copy **Gemeinsamer Kopf** plus genau einen **AGENT**-Block.

**Modell:** ausschließlich **Grok 4.6 extra high fast** (xhigh fast). Cursor-Slug: `cursor-grok-4.6-xhigh-fast`. Kein anderes Modell. Kein Grok 4.6 fast high, kein high max, kein Grok 4.5, kein Claude, kein GPT, kein Composer. Kein Fallback, „weil xhigh busy ist“.

Vier Agenten **parallel** (1–4), danach AGENT 5. Agenten 1–4 schreiben nur Arbeitsnotizen (Fragmente), damit sie sich nicht überschreiben. **Deliverable ist eine Datei:** `docs/audit/15-legacy.md`. Chat ist kein Deliverable.

Kein Anwendungscode ändern.

**Findings:** offiziell nur in `docs/audit/15-legacy.md`. Fragmente sind Rohmaterial für AGENT 5 und danach irrelevant. Im Chat nur: welche Dateien geschrieben wurden.

| Wer | Schreibt | Rolle |
| --- | --- | --- |
| AGENT 1 | `docs/audit/fragments/legacy-agent1-app-api.md` | Arbeitsnotiz App-API |
| AGENT 2 | `docs/audit/fragments/legacy-agent2-types-cluster.md` | Arbeitsnotiz Typen/Cluster |
| AGENT 3 | `docs/audit/fragments/legacy-agent3-wire-errors.md` | Arbeitsnotiz Wire/Aliase |
| AGENT 4 | `docs/audit/fragments/legacy-agent4-fork-docs.md` | Arbeitsnotiz Fork/Doku |
| AGENT 5 | **`docs/audit/15-legacy.md`** | **einziges Deliverable** |

Ordner anlegen falls fehlend. Dieser Lauf ersetzt den eigenen Agent-Output (neuer vollständiger Lauf). Alte `legacy-agent*.md` / `15-legacy.md` nicht fortschreiben — neu schreiben.

Fehler-Audit (`13-errors.md`) und Features-Audit (`14-features.md`) nicht nachplappern. Hier nur: **unnötige Kompatibilität, Doppel-APIs, Fork-Reste, tote Exports, Interop ohne Nutzer**.

Sprache: Deutsch. Spec-Zitate, Wire-Tokens, Identifier bleiben Englisch.

---

## 0. Warum dieser Lauf existiert

Das Package **ist nicht veröffentlicht**. Kein Tag, kein Downstream, keine SemVer-Garantie, keine produktiven Importer.

Alles, was nur existiert, weil

- eine **ältere interne API** nicht brechen sollte,
- der **Fork** (yeqown/memcached) so hieß / so exportierte,
- **gomemcache / php-memcached / libmemcached** drop-in sein sollen,
- jemand „später noch“ den alten Pfad braucht,

ist **Schuld, nicht Feature**. Pre-release heißt: **jetzt löschen oder zusammenlegen**, nicht nach v1.0 als Deprecated mitzuschleppen.

Nicht schreiben: „bestehende Nutzer würden brechen“, „für Kompatibilität behalten“, „thin wrapper lassen“, „Interop zu gomemcache rechtfertigt den Extra-Export“.

Doch schreiben: **eine** öffentliche Form, was weg soll, warum, konkrete Signatur danach.

yeqown und gomemcache sind **Tatort**, nicht Vorbild. Sie dienen nur dazu, Fork-Kopien zu erkennen.

---

## 1. Was Legacy hier heißt (und was nicht)

### 1.1 Legacy — muss auf die Liste

| Klasse | Beispiel (nur Richtung, am Code neu belegen) |
| --- | --- |
| **DOPPELT** | Zwei öffentliche Wege, derselbe Wire-Befehl, kein Semantik-Unterschied |
| **SHIM** | Öffentliche Methode, die nur eine andere öffentliche Methode aufruft |
| **FORK-REST** | Name, Typ, Export oder Metadaten-API, die yeqown mitgeschleppt hat und wir nicht mehr brauchen |
| **INTEROP-ONLY** | Extra-Hash, Extra-Signatur, Extra-Typ nur damit ein anderer Client dieselben Keys trifft |
| **TOT** | Exportiert oder intern, aber kein Caller außer Tests, die die alte Form einfrieren |
| **LECK** | Interner Typ (`Addr`, Pool-Details, Parser-Flags) ist öffentlich, Caller braucht ihn nicht |
| **ALIAS** | Zweite Konstante / zweiter Fehlertyp / zweites Feld für dasselbe Token |
| **SCHON-POLICY** | Code oder Kommentar, der eine schlechtere Form hält, „damit nichts bricht“ |

### 1.2 Nicht Legacy — nicht als ENTFERNEN zählen

- Aktuelles Text- und Meta-Protokoll (`set`, `mg`, `flush_all`, `watch`, …). Fehlt etwas in der Spec: das ist Features, nicht dieser Lauf.
- Binary / SASL / TAP / VBucket / Range-Ops: **bewusst nicht gebaut**. Nur listen, wenn Code oder Doku so tut, als gäbe es sie noch, oder wenn ein Binary-Name in der Text-API hängt.
- Zwei APIs mit **verschiedener Spec-Semantik** (Beispiel-Richtung: Multi-Get lässt Miss weg, Single-Key-Miss kann `ErrNotFound` sein). Dann **BEHALTEN** plus eine Zeile Begründung — oder **ZUSAMMENLEGEN**, wenn eine Option/`GetOpts` beide Fälle klarer abdeckt als zwei Methodennamen.
- Convenience, die **nicht** eine zweite öffentliche Fläche ist (unexported Helper).
- UDP, TLS, Admin-Opt-in, `noreply`: aktuelle Spec / Sicherheitsentscheidung, kein Fork-Schonmodus — außer die **Form** ist doppelt (`Incr` + `IncrQuiet` vs ein `Quiet`-Feld überall).

Wenn unsicher: Finding schreiben, Status `UNKLAR`, beide Schnitte vorschlagen. Nicht verschweigen.

---

## 2. Zuerst Inventar, dann Urteil

Nicht aus dem Gedächtnis. Reihenfolge Pflicht:

1. **Export-Fläche:** `go doc -all github.com/EyJunge1/go-memcached/memcached` (oder Workspace-Modulpfad). Jeder exportierte Typ, jede Methode, jede `var Err*`, jede `func New*`.
2. **Grep** im Package `memcached/` (ohne `vendor`):
   - `compat`, `legacy`, `deprecated`, `alias`, `yeqown`, `gomemcache`, `interop`, `for now`, `kept`, `wrapper`, `same as`, `also alias`
   - `NewAddr`, `GetMetadata`, `ParseGeneralStats`, `With`, `Option`, `NoReply`, `Quiet`
3. **yeqown-Vergleich** (nur zum Finden, nicht zum Nachbauen):
   - https://github.com/yeqown/memcached — exportierte Namen, `Addr`, Options-Pattern, `Statistic`
4. **gomemcache-Vergleich** (nur Interop-Verdacht):
   - https://github.com/bradfitz/gomemcache — `GetMulti`, CRC-Modulo, SASL, Item-Felder
5. Spec nur, wenn ein Token/Befehl als „behalten weil Wire“ gerechtfertigt werden soll:
   - https://docs.memcached.org/protocols/basic/
   - https://docs.memcached.org/protocols/meta/
   - https://raw.githubusercontent.com/memcached/memcached/master/doc/protocol.txt
   - lokale Kopie `docs/protocol.txt`

Jede Claim: `datei:zeile` plus exportierter Identifier. Ohne Zeile kein Finding.

---

## 3. Pflicht-Prüfungen (jede Zeile abhaken)

### 3.1 Öffentliche App-API — ein Weg pro Job

Inventarisiere **alle** `func (c *Client)` und `func (p *Pipeline)` / `func (a *AdminClient)`. Für jede Gruppe:

| Gruppe | Muss geprüft werden |
| --- | --- |
| Storage | `Store` + `StoreRequest` vs `Set`/`Add`/`Replace`/`Append`/`Prepend`/`Cas` — Shim oder gewollte Kurzform? Eine Fläche vorschlagen. |
| Get | `Get`+`GetOpts` vs `GetOne` vs `GetMulti` — Map vs Slice, Miss vs `ErrNotFound`, implizites `CAS: true` bei GetMulti. Drei Namen für eine Familie? |
| Mutate | `Delete`/`Touch` + `MutateOpts.Quiet` vs `Incr`/`Decr` die `Quiet` **ablehnen** vs Extra-`IncrQuiet`/`DecrQuiet`. Ein Quiet-Modell. |
| Pipeline | Text-`Pipeline` vs `MetaPipeline` — zwei Batch-APIs nötig oder eine? |
| Recache | `GetOrVivify` / `GetEarlyRecache` / `Invalidate` vs `MetaGet`/`MetaDelete` mit Flags. Helper behalten nur, wenn sie nicht die „echte“ API verdoppeln. |
| Stats | `Client.Stats` / `StatsAll` vs `Admin().Stats` / `StatsDetail` — wer darf was, und ist die Spaltung klar oder historisch? |
| Version | `Version` / `VersionAll` / `Ping` — drei Einstiege, ein Wire? |
| Flush | `Admin().FlushAll` (unbound = broadcast) vs `All().FlushAll` vs `On().FlushAll`. `FlushOpts.Delay` **und** `FlushOpts.Exp` für dasselbe Token. |
| Meta-Modes | `MetaArithmeticModeIncr` / `IncrPlus` (`I` vs `+`) und analog Decr — Spec-Alias auf dem Wire, nicht zwei exportierte Konstanten, wenn der Typ unexported bleiben kann. |

Regel: **Nach dem Schnitt gibt es pro Wire-Befehl höchstens eine öffentliche Methode**, plus höchstens eine Options-Struct. Kurzformen (`Set`) nur, wenn sie **dieselbe** Options-Struct nutzen oder wir sie bewusst streichen.

### 3.2 Typen, Config, Exports, die niemand von außen braucht

| Verdacht | Prüfen |
| --- | --- |
| `Addr`, `NewAddr`, `NewAddrWeighted` | Caller sieht `Server` + `ParseServers`. Ist `Addr` nur Cluster-Interna + yeqown-Export? `GetMetadata` / `Addr.Add` — gibt es **einen** Produktionscaller? |
| `clientOptions` vs `Config` | Parallelwelt aus der Options-Ära? Felder 1:1 mappen oder Reste (`authAllowPlain` nur intern ok). |
| `Statistic` + `ParseGeneralStats` | Riesiges getyptes STAT-Struct. Braucht die öffentliche API eine Map, oder diesen yeqown-Katalog? Unbekannte Keys in `Extra` — behalten als Map reicht oft. |
| Hash-Builder | Default ist Ketama/MD5. `NewCRC32HashPickBuilder` ist in README **gomemcache interop**. Ohne veröffentlichte Nutzer: behalten als optionales Extra oder streichen? murmur3 64-bit ≠ libhashkit 32-bit — Interop-Lüge? rendezvous: Spec oder Komfort? |
| `Failover *Failover` | `nil` = an, `&Failover{}` = aus. Zeiger-Trick für Defaults — klar oder Legacy-Semantik? |
| `Cluster.HashWithPrefix` / `Namespace` | libmemcached-Verhalten oder ungenutzte Fläche? |
| `Resolver` / `Picker` / `Builder` | Müssen die Interfaces exportiert sein, oder reicht ein unexported Pick plus 1–2 Konstruktoren? |
| `PoolStat` / `PoolStats` | Diagnose ok; Felder `MaxConns` die intern anders heißen — Fork-Namen? |
| `MetaOp.Flags any` | Typ-Flucht statt generischer/union API. Legacy-Bequemlichkeit? |
| `Item.String` / `MetaItem.String` | Debug-Export, CAS in String aber Exp nicht — unfertig oder tot? |

Export-Regel: Was ein App-Caller für Get/Set/Meta/Admin nicht anfassen muss, wird **UNEXPORTED** vorgeschlagen. Breaking ist hier der Default.

### 3.3 Wire, Parser, Fehler — keine Schon-Aliase

Nicht das Error-Inventar von `13-errors.md` wiederholen. Nur Kompatibilität:

- Meta-Wiki `ST`, Spec-Tippfehler `DE`, Server-Flag `meta_response_old` → `OK`: **kein** Mapping auf `HD`. Wenn Code oder Test so ein Alias andeutet: ENTFERNEN.
- `ERROR` vs `ERROR <msg>`: zwei Sentinels sind Spec, kein Legacy — nicht anfassen außer ein dritter Name existiert.
- Admin-Tokens, die die **Spec** hat, der **aktuelle Server** aber nicht sendet (`NOTFULL`, `UNSAFE`): BEHALTEN mit Kommentar „Spec“, nicht löschen „weil tot auf Wire“. Umgekehrt: Token **nur** in altem Server, nicht in Spec und nicht in Source — ENTFERNEN.
- Doppelte Map-Pfade (`MapWireLine` vs `forecastCommonFaultLine` vs Meta-Parser) nur erwähnen, wenn ein Pfad **nur** eine alte Klassifikation am Leben hält.
- Interne `endIndicator*` / `buildNoReplyResponse`: ok, wenn unexported.

### 3.4 Fork, Doku, Tests

- README / `doc.go`: Sätze der Form „for gomemcache-style“, „unlike yeqown“, „compat with libmemcached“. Interop-Versprechen streichen oder als nicht-Ziel markieren.
- Tests, die **nur** eine alte Signatur oder einen Alias einfrieren (`Test_buildMetaArithmeticCommand_modeAliases`, GetMulti-CAS-Implizitheit, CRC32-Golden gegen gomemcache). Test darf nach dem Schnitt weg oder muss die **neue** eine API testen.
- Kommentare `also alias for`, `compatible with`, `not compatible with Ketama-SPY` — gehören die in die öffentliche Doku?
- Dateien / Typen, die noch `options.go` + paralleles `Config` heißen, ohne dass Options öffentlich sind.
- Ungenutzte Exports: `go test` / Referenzen im Package. Export ohne Nicht-Test-Caller = TOT.

---

## 4. Bekannte Verdachtsstellen (Start, nicht Ende)

Das ist **keine** Vorverurteilung. Jede Zeile am **aktuellen** Code belegen, dann Status setzen. Weiter suchen — diese Liste ist unvollständig.

- `client_storage.go`: `Set`/`Add`/`Replace`/`Append`/`Prepend`/`Cas` → `Store`
- `client_get.go`: `Get`, `GetOne`, `GetMulti` (`GetMulti` setzt `CAS: true`)
- `client_incr.go`: `Incr`/`Decr` lehnen `MutateOpts.Quiet` ab; Extra `IncrQuiet`/`DecrQuiet`
- `client_delete.go`: `MutateOpts` gilt laut Kommentar auch für incr — tut es nicht
- `client_recache.go`: drei Helper über Meta
- `client_pipeline.go` vs `MetaPipeline`
- `client_flush.go`: `FlushOpts.Delay` + `Exp`; `AdminClient.FlushAll` unbound = broadcast = `All().FlushAll`
- `client_stats.go`: `ParseGeneralStats`; `protocol_stats.go`: `Statistic`
- `addr.go`: exportiertes `Addr`, `NewAddr`, `NewAddrWeighted`, `GetMetadata`, `Add`
- `options.go`: `clientOptions` Spiegel von `Config`
- `config.go`: `Failover` nil vs empty; `NewCRC32HashPickBuilder` in README als gomemcache-Interop
- `meta.go` / `protocol_meta_arith.go`: `I`/`+`, `D`/`-`; öffentliche Mode-Konstanten auf unexported Typ
- `errors.go`: Sentinel-Menge gegen echte Caller und gegen Spec (keine Geister-Sentinels ohne Wire)
- `item.go`: `Item` vs `StoreRequest` vs Pipeline-`Item.Exp`

---

## 5. Ausgabeformat

**Eine Findings-Datei.** AGENT 5 schreibt sie neu. Agenten 1–4 fassen `15-legacy.md` nicht an.

```text
docs/audit/fragments/legacy-agent1-app-api.md      # Roh, parallel
docs/audit/fragments/legacy-agent2-types-cluster.md
docs/audit/fragments/legacy-agent3-wire-errors.md
docs/audit/fragments/legacy-agent4-fork-docs.md
docs/audit/15-legacy.md                            # Deliverable, nur AGENT 5
```

Kopf jeder Datei:

- Datum, Agent-Nummer, Modell **Grok 4.6 extra high fast**
- Wie die Export-Liste erzeugt wurde (`go doc` / manuell)
- Zählung im eigenen Scope: ENTFERNEN / ZUSAMMENLEGEN / UNEXPORTED / TOT / BEHALTEN / UNKLAR

Dann eine Tabelle im eigenen Scope:

| Identifier / Fläche | Art | Caller (App / nur intern / nur Test) | Wire / Spec | Status | Code | Soll |

Art nur: `DOPPELT` | `SHIM` | `FORK-REST` | `INTEROP-ONLY` | `TOT` | `LECK` | `ALIAS` | `KLAR` (bewusst eine API)

Status nur: `ENTFERNEN` | `ZUSAMMENLEGEN` | `UNEXPORTED` | `TOT` | `BEHALTEN` | `UNKLAR`

Danach Findings, eines pro Fläche:

```
### <Titel>
- Status:
- Priorität: P0 | P1 | P2
- Kategorie: App | Admin | Meta | Infra | Docs
- Art: DOPPELT | SHIM | FORK-REST | INTEROP-ONLY | TOT | LECK | ALIAS
- Code: datei:zeile
- Ist: öffentliche Form(en) heute
- Soll: eine Form danach (Signatur)
- Weg: was gelöscht / unexported wird
- Breaking: ja — warum das vor Release weg muss
- Nicht tun: kein Deprecated-Alias, kein zweiter Name
```

Prio:

- **P0**: Zwei öffentliche APIs für denselben Job, oder exportierter Intern-Typ (`Addr`, Riesen-`Statistic`, …), der nach v1 nicht mehr einfangbar ist
- **P1**: Interop-only Hash/Signatur, Fork-Name, Quiet-Modell uneinheitlich, Shim-Wald (`Set`+`Store`)
- **P2**: Kommentare, README-Interop-Sätze, Stringer, Test-only Alias

AGENT 5 hängt an `15-legacy.md` zusätzlich:

- Master-Tabelle aller Identifier
- **Kill-Liste** (löschen)
- **Merge-Liste** (eine Signatur, Rest weg)
- **Unexport-Liste**
- **Keep-Liste** mit Begründung (Spec-Semantik oder eine klare API)
- Vorgeschlagene **finale öffentliche Fläche** (nur Namen, kein Roman): `New`, `Config`, Client-Methoden, Admin, Meta, Cluster
- Was README/`doc.go` nach dem Schnitt nicht mehr versprechen darf (gomemcache-Keys, yeqown-Vergleich als Feature)

Querverweis nur: Fehler-Sentinels kanonisch in `13-errors.md`; fehlende Spec-Befehle in `14-features.md` / `01-missing-features.md`. Hier keine neuen Features erfinden.

---

## 6. Gemeinsamer Kopf

```text
Repo: Workspace-Root go-memcached. Package memcached/.
Modul: github.com/EyJunge1/go-memcached

Modell: ausschließlich Grok 4.6 extra high fast (xhigh fast).
Cursor-Slug: cursor-grok-4.6-xhigh-fast.
Kein anderes Modell. Kein Grok 4.6 fast high. Kein high max. Kein Grok 4.5.
Kein Claude, kein GPT, kein Composer, kein inherit-ausweichen.

Auftrag: Legacy-Audit. Keine Commits, in diesem Lauf kein Anwendungscode ändern.
Agent 1–4: Findings nur ins eigene Fragment (Rohmaterial).
AGENT 5: alle Findings in EINE Datei docs/audit/15-legacy.md.
Kein Chat-only-Ergebnis. Kein „siehe oben, Datei spare ich mir“.
Chat: nur welche Dateien geschrieben/ergänzt wurden.

WICHTIG — pre-release, nicht veröffentlicht, nicht in Benutzung:
Kein Legacy. Keine API-Stabilität. Kein SemVer.
Rückwärtskompatibilität ist kein Argument.
Deprecated-Wrapper sind verboten (kein „alte Methode bleibt, ruft neue auf“).
Fork-Reste (yeqown) und gomemcache-Interop sind Verdacht, kein Ziel.
Breaking Changes ohne Stress vorschlagen. Eine öffentliche Form pro Job.

Zuerst:
1) go doc -all auf das Package (Export-Inventar)
2) Grep: compat, legacy, deprecated, alias, yeqown, gomemcache, interop, Quiet, NewAddr
3) https://github.com/yeqown/memcached  — nur um Kopien zu finden
4) https://github.com/bradfitz/gomemcache — nur Interop-Verdacht
5) Spec nur als Rechtfertigung für BEHALTEN:
   https://docs.memcached.org/protocols/basic/
   https://docs.memcached.org/protocols/meta/
   https://raw.githubusercontent.com/memcached/memcached/master/doc/protocol.txt
   docs/protocol.txt

Jede Aussage: Datei:Zeile + Identifier.
Status: ENTFERNEN | ZUSAMMENLEGEN | UNEXPORTED | TOT | BEHALTEN | UNKLAR
Art: DOPPELT | SHIM | FORK-REST | INTEROP-ONLY | TOT | LECK | ALIAS | KLAR
Prio: P0 | P1 | P2
Kategorie: App | Admin | Meta | Infra | Docs

Deutsch. Eintrags-Template aus dem Prompt verwenden.
13-errors / 14-features nicht nachplappern — am aktuellen Code neu belegen.
Nicht „Kompatibilität behalten“. Idealzustand + was weg soll.
```

---

## AGENT 1 — Öffentliche App-API (Methoden)

```text
Rolle: API-Schnitt. Gemeinsamer Kopf gilt. Abschnitte 1, 3.1, 4 (Client-Methoden).

Alle exportierten Methoden auf Client, Pipeline, AdminClient, AdminBroadcast.
Pro Wire-Befehl: eine Methode + eine Options-Struct, oder begründen warum zwei Semantiken.

Storage-Shims, Get-Familie, Quiet-Bruch Incr vs Delete, beide Pipelines,
Recache-Helper, Stats/Version/Flush-Doppelungen.

Schreiben:
- docs/audit/fragments/legacy-agent1-app-api.md
```

---

## AGENT 2 — Typen, Config, Cluster, Export-Lecks

```text
Rolle: Fläche nach innen. Gemeinsamer Kopf gilt. Abschnitte 1, 3.2, 4 (Typen).

go doc -all: jeder exportierte Typ/Konstruktor außer den Methoden aus Agent 1
(Identifer trotzdem listen, wenn der Typ nur für eine Doppel-API da ist).

Addr/NewAddr/Metadata, Config vs clientOptions, Statistic/ParseGeneralStats,
Hash-Builder (CRC32-gomemcache, murmur3, rendezvous, Ketama),
Failover-nil-Trick, Namespace, exportierte Interfaces, MetaOp.Flags any,
Item vs StoreRequest.

Schreiben:
- docs/audit/fragments/legacy-agent2-types-cluster.md
```

---

## AGENT 3 — Wire, Aliase, tote Sentinels

```text
Rolle: Protokoll ohne Schon-Aliase. Gemeinsamer Kopf gilt. Abschnitte 1, 3.3.

Keine volle Error-Matrix (das ist 13). Nur:
- Aliase ST/DE/OK→HD, Mode I/+ D/-
- Sentinels ohne Spec und ohne Server-Source
- Zweite Mapper-Pfade, die nur alte Klassifikation halten
- Öffentliche Konstanten auf unexported Wire-Typen
- Kommentare „also alias for“

Code: errors.go, protocol_wire.go, protocol_meta_*.go, protocol_meta_arith.go,
meta.go, protocol_builder.go, protocol_response.go.

Schreiben:
- docs/audit/fragments/legacy-agent3-wire-errors.md
```

---

## AGENT 4 — Fork, Doku, Tests, tote Exports

```text
Rolle: Herkunft und Versprechen. Gemeinsamer Kopf gilt. Abschnitte 1, 3.4.

yeqown-Repo gegen unsere Exports (Namen, Addr, Statistic, Options-Ära).
README/doc.go Interop-Sätze (gomemcache CRC32, „unlike yeqown“).
Tests, die Aliase oder alte Signaturen einfrieren.
Exports ohne Nicht-Test-Caller.

Nicht die Feature-Vollständigkeit der README neu auditieren (10-readme).
Nur Versprechen, die uns auf Legacy festnageln.

Schreiben:
- docs/audit/fragments/legacy-agent4-fork-docs.md
```

---

## AGENT 5 — Synthese (erst nach 1–4)

```text
Rolle: Chefredakteur. Gemeinsamer Kopf gilt.

Nur die vier Fragment-Dateien lesen, nicht neu spekulieren.
Duplikate zusammenziehen. Widerspruch: eine klare API > Spec-Abbildung > aktueller Code.
Interop und Fork verlieren immer gegen eine kleinere öffentliche Fläche.

Schreiben:
- docs/audit/15-legacy.md   ← einziges Deliverable
  (Kopf mit Modell, Master-Tabelle, Kill-/Merge-/Unexport-/Keep-Liste, finale API-Namen)
- Fragmente nicht löschen (Rohmaterial), aber nichts nur dort lassen
```

---

## 7. Ablauf

1. Vier Agenten **parallel**, jeder mit Kopf + seinem AGENT-Block. Modell nur **Grok 4.6 extra high fast** (`cursor-grok-4.6-xhigh-fast`).
2. AGENT 5 (erst nach 1–4, gleiches Modell) liest die vier Fragmente und schreibt **eine** Datei `docs/audit/15-legacy.md`.
3. Fertig = `docs/audit/15-legacy.md` vollständig. Fragmente dürfen liegen bleiben, zählen aber nicht. In diesem Lauf kein Patch am Client.

Nicht tun:

- Anwendungscode patchen
- Deprecated-Aliase vorschlagen („alte Funktion bleibt“)
- Binary/SASL als Missing Feature
- „für gomemcache-Kompatibilität behalten“
- Error- oder Feature-Audit abschreiben
- ein anderes Modell wählen, „weil xhigh busy ist“
- Findings nur im Chat oder nur in Fragmenten lassen
- mehrere gleichrangige Audit-MDs statt einer `15-legacy.md`
