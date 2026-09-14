# API-Review: parallele Agents

Prompt für einen Orchestrator. Er startet **6 Explore-Agents parallel**. Jeder Agent
bekommt nur seine API-Gruppe. Am Ende fasst der Orchestrator auf Deutsch zusammen.

**Modell (hart):** Jeder der 6 Agents und der Orchestrator selbst **nur**
**Grok 4.6 Extra High Fast**. Im Task-Tool **immer** `model=cursor-grok-4.6-xhigh-fast`.
Kein `inherit`, kein anderes Modell, kein Fallback, kein Weglassen des `model`-Felds.

**Findings (hart):** Nur der Orchestrator schreibt die Ergebnisse nach
`docs/api-findings.md`. Nicht in diese Prompt-Datei, nicht in den Chat als einzige Ablage,
nicht in eine andere Datei. Agents schreiben **keine** Dateien.

In Cursor: neuen Chat, Agent-Mode, Text unten einfügen. Die 6 Agents müssen in
**einer** Nachricht parallel starten.

---

Du bist Orchestrator. Prüfe die GESAMTE öffentliche API von
github.com/EyJunge1/go-memcached (Workspace: `/Users/arneleder/Development/gomemcached`)
auf **Einheitlichkeit, Richtigkeit und professionelle Form**.
Sprich am Ende Deutsch. Agents dürfen intern Englisch denken.

**Modell:** nur Grok 4.6 Extra High Fast. Jeder Task-Aufruf **muss**
`model=cursor-grok-4.6-xhigh-fast` setzen. `inherit` ist verboten.

**Ablage:** Nach den 6 Agents schreibst du die Findings **nur** nach
`docs/api-findings.md` (anlegen oder überschreiben). Chat-Zusammenfassung zusätzlich
erlaubt, Ersatz für die Datei nicht.

STARTE GENAU 6 EXPLORE-AGENTS PARALLEL (Task tool, `subagent_type=explore`,
thoroughness=very thorough, **`model=cursor-grok-4.6-xhigh-fast`** — Pflichtfeld).
Jeder Agent bekommt NUR seine Gruppe.
Kein Agent ändert Dateien (auch nicht `docs/api-findings.md`). Kein Agent schreibt Exploits.
Kein Agent darf ein anderes Modell verwenden oder `model` weglassen.
Kein Agent bewertet Allocs, Pools, Hot-Path oder `ns/op` — das ist ein anderer Review.

## Hart (für jeden Agent, unverändert kopieren)

Repo: `/Users/arneleder/Development/gomemcached`  
Paket: `memcached/` (`livetest/` und `benchmark/` sind getrennte Module)  
Öffentliche Oberfläche: nur exportierte Typen, Konstanten, Funktionen, Methoden,
Felder und Godoc/`Example*` in `memcached/`. Interne Parser/Builder nur, wenn sie
die öffentliche Semantik belegen (Fehler, Flags, Dest-Vertrag).

Modell: **nur** Grok 4.6 Extra High Fast. Task-Feld: `model=cursor-grok-4.6-xhigh-fast`.
Kein `inherit`, kein anderes Modell. Dieser Agent schreibt **keine** Dateien.

Quellen der Wahrheit (nicht neu erfinden):

- `memcached/doc.go`, `README.md`, `memcached/example_test.go`
- `memcached/config.go` (Konvention A–E im `Config`-Kommentar)
- `docs/protocol.txt` für Wire-Semantik
- Go-Bibliotheksstandard: context zuerst, `error` zuletzt, Zero-Value nützlich,
  `errors.Is`/`errors.As`, kein Package-Stutter, Godoc-Satz beginnt mit dem Namen,
  Options als letzte Argumente, Dest-Reuse ist in diesem Paket Absicht.

Dokumentierte Konventionen (nicht als Bug werten, sondern prüfen ob sie
**überall** gelten):

- A — Opt-in-Gruppen (`Failover`, `Auth`): Zero/`nil` schaltet aus.
- B — Default-an: invertiertes Bool (`DisableTCPNoDelay`), nie `*bool`.
- C — `0` ist legaler Wire-Token → Pointer (`GetOpts.Touch`, `FlushOpts.Delay`,
  Meta `*Exptime` / `*uint64`).
- D — `0` hat keine Wire-Bedeutung → Zero lässt Flag weg (`MetaGet.Recache`,
  `Cluster.ResolverRefresh`).
- E — numerische Defaults feldweise (`DialTimeout 0` → 3s; `TimeoutNone` = kein Extra-Deadline).
- Keys sind `[]byte` (1–250, kein Whitespace/Control). Werte sind roh `[]byte` + `uint32` Flags.
- Dest-Reuse: Caller liefert `dst`; Zero-Alloc nur wenn Cap reicht. Get kopiert Keys nicht.
- Get-Miss: `Found == false`, **nicht** `ErrNotFound`. Leerer Hit: `Found` und `len(Value)==0`.
- Store-Fehler `NOT_STORED`/`EXISTS`/`NOT_FOUND` über `errors.Is`.
- Text-`noreply` (`Quiet`) verwirft die Conn; Meta-`q` paart immer mit `mn`.
- Text-Pipeline ist write-then-read, kein `noreply`. Quiet-Meta bleibt auf `Meta().Pipeline`.
- Admin nur hinter `Config.AllowAdmin` + `Admin().On`/`All`. Flush extra `AllowFlushAll`.
- Default bleibt Klartext-TCP. Binary/SASL/TAP sind bewusst nicht implementiert.

Was zählt als echter Fund:

- Zwei ähnliche öffentliche Methoden mit **unterschiedlichem Dest-/Options-/Error-Vertrag** ohne dokumentierten Grund
- Konvention A–E an einer exportierten Stelle verletzt
- Name täuscht Semantik (Go-Konvention oder Wire-Token)
- Leicht falsch zu benutzen (nil-Dest, Quiet, UDP, Admin, Miss-als-Error)
- Godoc fehlt, widerspricht der Signatur, oder `Example*` deckt den Happy Path nicht
- Flag/Status aus `docs/protocol.txt` falsch gemappt oder als Stringly-API durchgereicht, obwohl der Rest typisiert ist
- Dual-API (Text vs Meta, Pipeline vs MetaPipeline) mit unnötiger Form-Divergenz
- Exportiertes Feld/Typ, das ein internes Wire-Detail leakt, ohne dass Caller es brauchen

Was NICHT zählt (als „must change“):

- Allocs, Pools, `unsafe`, Hot-Path (anderer Prompt)
- Binary-Protocol / Ketama-Wechsel
- Default Klartext-TCP (dokumentierte Sicherheitslage)
- Dest-Reuse statt Return-Value auf Get/Meta/Stats/Version (Absicht)
- Watch/Dump als Channel + `map`/`DumpEntry` (Absicht)
- `Get` index-aligned, Miss nicht `ErrNotFound` (Absicht)
- Incr/Decr ohne Quiet; quiet arith nur Meta (dokumentiert)

Urteile (nur diese vier):

- **STIMMIG** — professionell, konsistent, so lassen
- **BRUCH** — Uneinheitlichkeit oder falsche Semantik; Breaking oder Vor-1.0-Korrektur
- **ABSICHT** — dokumentierter Trade-off; nicht „fixen“, nur benennen
- **GLATTZIEHEN** — Name/Godoc/Beispiel; Verhalten bleibt

Pro exportierter Funktion/Typ liefern:

1. Signatur (oder Typ + exportierte Felder)
2. Vertrag: Args, Dest, Zero-Value, Fehler, UDP/Admin-Gates
3. Abgleich: Nachbarn in derselben Gruppe + Konvention A–E + `protocol.txt`
4. Urteil + eine Zeile Begründung
5. Bei BRUCH/GLATTZIEHEN: konkrete Ziel-Signatur oder Feldname in einem Satz, ohne Code zu schreiben

## Agent 1 — Text Application API

Modell: Grok 4.6 Extra High Fast — `model=cursor-grok-4.6-xhigh-fast` (Pflicht, kein inherit)

Dateien: `client.go` (nur `New`/`Close` soweit der Client-Vertrag), `item.go`,
`client_storage.go`, `client_get.go`, `client_delete.go`, `client_touch.go`,
`client_incr.go`, `client_pipeline.go`, `exptime.go`, `doc.go`

Öffentliche Symbole:

- `New`, `Client.Close`
- `Client.Store` + `Item` + `StoreMode`
- `Client.Get` / `GetOne` + `GetResult` + `GetOpts`
- `Client.Delete` / `Touch` + `MutateOpts`
- `Client.Incr` / `Decr`
- `Client.Pipeline` / `Pipeline.Store` / `Delete` / `Touch` / `Run`
- `Exptime`, `ExptimeNever`, `ExptimeImmediate`, `Rel`, `UnixExp`

Prüfe besonders:

- Store packt Mode/Quiet/CAS/Exp **in Item**; Get/Delete/Touch haben separates Opts-Argument — ist das eine klare Regel oder ein Bruch?
- `Get` füllt Slice, `GetOne` Pointer, `Incr` **returned** `uint64` — Dest vs Return.
- Pipeline: Builder + `Run` vs Meta-Pipeline (Slice + dst). `Item.Quiet` in Pipeline = Fehler.
- `Item.Exp` vs `GetOpts.Touch` vs später Meta `TTL` — gleiche Zeitbedeutung, gleiche Typen?
- `StoreCAS` als Mode vs `Item.CAS` vs `GetOpts.CAS`.
- Fehlende Convenience (`Add`/`Replace` als eigene Methoden) vs eine `Store`+Mode-API — Urteil, nicht Geschmack.

## Agent 2 — Meta API

Modell: Grok 4.6 Extra High Fast — `model=cursor-grok-4.6-xhigh-fast` (Pflicht, kein inherit)

Dateien: `client_meta.go`, `meta.go`, `item.go` (`MetaItem`, `MetaItemDebug`,
`DebugField`), `client_meta_get.go`, `client_meta_set.go`, `client_meta_delete.go`,
`client_meta_arith.go`, `client_meta_debug.go`, `client_meta_noop.go`,
`protocol_meta_set.go` (`MetaSetMode`), `protocol_meta_arith.go` (`MetaArithmeticMode`)

Öffentliche Symbole:

- `Client.Meta`, `MetaClient`
- `MetaClient.Get` / `Set` / `Delete` / `Arithmetic` / `Noop` / `Debug` / `Pipeline`
- `MetaGet`, `MetaGetReturn`, `MetaSet`, `MetaDelete`, `MetaArithmetic`, `MetaDebug`, `MetaNoop`, `MetaOp`
- `MetaResult`, `MetaItem`, `MetaStatus`, `MetaItemDebug`
- `MetaSetMode*`, `MetaArithmeticMode*`

Prüfe besonders:

- Reihenfolge `(ctx, key, flags, dst)` vs Text `(ctx, …, opts)`. Dest immer `*MetaResult` außer Debug (`*MetaItemDebug`) und Pipeline (`[]MetaResult`).
- Pointer-Regel C/D: `CompareCAS`/`NewCAS`/`Vivify`/`Touch`/`TTL`/`Initial` vs `Recache time.Duration` vs `Flags uint32` (0 ist legal — wird 0 gesendet?).
- Return-Flags: `MetaGet.Return.CAS` vs `MetaSet.ReturnCAS` vs `MetaArithmetic.ReturnValue`.
- Create-on-miss: `Vivify` vs `AutoVivify` vs `AutoCreate` — drei Namen, eine Idee?
- Exportierte `P`/`L` (Wire-Buchstaben) vs sprechende Namen sonst. `Opaque string` Request vs `[]byte` Response.
- `StoreMode` ist `uint8`, `MetaSetMode` ist Wire-String `"S"`/`"E"`/… — duale Mode-APIs.
- Miss: Methoden-`error` nil, Status EN/NF, `Result.Err()` → `ErrNotFound`. Widerspruch zu Text-Get?
- `MetaOp`: genau eines von Get/Set/Del/Arith/Noop; Debug nicht pipelinebar. Leicht falsch zu bauen?
- `MetaItem` Felder vs `MetaResult` Wrapper — Caller sieht beides.

## Agent 3 — Config, Transport, Cluster

Modell: Grok 4.6 Extra High Fast — `model=cursor-grok-4.6-xhigh-fast` (Pflicht, kein inherit)

Dateien: `config.go`, `client.go` (`SetServers`, `Servers`, `PoolStats`, `ResolverErr`),
`client_failover.go` (`DeadServers`), `runtime.go` (`DialContextFunc`), `addr.go` soweit exportiert

Öffentliche Symbole:

- `Config`, `Server`, `Transport`, `Pool`, `Cluster`, `Failover`, `Auth`
- `TimeoutNone`, `DefaultFailover`, `ParseServers`
- `New`, `SetServers`, `Servers`, `DeadServers`, `PoolStats`, `PoolStat`, `ResolverErr`
- `DialContextFunc`

Prüfe besonders:

- Gilt A–E auf **jedem** exportierten Config-Feld? Gegenbeispiele (0 bedeutet Default **und** wäre legaler Wire-Wert).
- `Auth *Auth` vs `Failover` by value — A sagt beides Opt-in; Form unterschiedlich.
- `AllowFlushAll` / `AllowAdmin` / `AllowSetServers` — drei Bools, gleiche Idee, gleiche Namen?
- `Transport.TLS` + `RequireTLS` + `AllowInsecureTLS` — hart zu missbrauchen?
- `ParseServers` vs `[]Server` handgebaut — Gewicht, unix/udp, Round-Trip `formatServerSpec`.
- `Cluster.Namespace` nur on-wire; GetResult ohne Key — Caller-Falle?
- `SetServers` braucht Flag, UDP darf nicht mutieren — ist der Fehler `ErrNotSupported` vs `ErrSetServersNotAllowed` konsistent?
- Zero-Value von `Config` ohne Servers: `New` Fehler — gut. Fehlende Server-Validierung an anderen Eingängen?

## Agent 4 — Fehler, Dest-Vertrag, Godoc

Modell: Grok 4.6 Extra High Fast — `model=cursor-grok-4.6-xhigh-fast` (Pflicht, kein inherit)

Dateien: `errors.go`, `doc.go`, `example_test.go`, Godoc aller exportierten
Methoden (kurz `client_*.go` / `item.go` / `meta.go` / `config.go` / `exptime.go`),
`client_errors_api_test.go` nur als Beleg welche Sentinel öffentlich sind

Öffentliche Symbole: alle `Err*` plus der Dest-/Error-Vertrag jeder IO-Methode.

Prüfe besonders:

- Jeder Sentinel: Name, Godoc, Wire-Zeile, `errors.Is` nach Wrap (`request failed: %w`).
- Get-Miss ≠ `ErrNotFound`; Meta-Miss = `Status.Err()` = `ErrNotFound` bei nil Methoden-error.
- Quiet: welcher error kommt noch, welcher nicht (CLIENT_ERROR vs send-ok Admin).
- UDP: `ErrNotSupported` überall gleich, oder mal `ErrInvalidArgument`?
- Dest nil: GetOne/Meta/Stats/Version ablehnen; Get Slice-Längen-Mismatch.
- Nach `Close`: `ErrClosed` auf allen öffentlichen IO-Methoden?
- Godoc: erster Satz = Name; Examples für Store/Get/GetOne/Meta/Pipeline/Admin/ParseServers/TLS.
- README-Tabelle vs tatsächliche Signaturen (veraltet = BRUCH der Doku).

## Agent 5 — Admin, Stats, Version, Streams

Modell: Grok 4.6 Extra High Fast — `model=cursor-grok-4.6-xhigh-fast` (Pflicht, kein inherit)

Dateien: `client_admin.go`, `client_admin_watch.go`, `client_flush.go`,
`client_stats.go`, `client_version.go`, `protocol_admin.go` (nur öffentliche Mapping-Belege)

Öffentliche Symbole:

- `Client.Admin`, `Admin.On`, `Admin.All`
- `AdminNode` + `AdminBroadcast` Methoden (Flush, Verbosity, Shutdown, CacheMemLimit,
  Stats, StatsDetail, Slabs*, LRU*, Watch, LRUCrawlerMetaDump/MgDump)
- `AdminOpts`, `FlushOpts`, `StatsDetailMode`, `WatchTopic`, `WatchEvent`, `DumpEntry`
- `Client.Stats` / `StatsAll` / `Version` / `VersionAll`
- `Stat`, `NodeStats`, `NodeVersion`

Prüfe besonders:

- App-`Stats` vs `Admin().On().Stats` (reset/sizes/detail) — discoverability.
- `Stats(ctx, dst *[]Stat, args ...string)` ist stringly; Rest der API ist typisiert.
- `StatsDetail(ctx, dst, mode)` typisiert — warum detail speziell, items/slabs nicht?
- `On` vs `All`: welche Ops fehlen bewusst auf All (`Shutdown`)? Fehlende Godoc-Falle.
- `LRUMode(ctx, mode string)`, `LRUCrawlerCrawl(classes string)`, Dump-`selector string` vs `WatchTopic` Konstanten.
- Flush: `Delay *Exptime` (Regel C) vs `Stagger time.Duration`; `Quiet` vs `AdminOpts.Quiet`.
- Streams: Channel-API, Pflicht-Deadline (`ErrDeadlineRequired`), Cancellation.
- Version dest `*[]byte` vs VersionAll `*[]NodeVersion` — analog Stats/StatsAll?

## Agent 6 — Professionelle Go-Library-Form (Querschnitt)

Modell: Grok 4.6 Extra High Fast — `model=cursor-grok-4.6-xhigh-fast` (Pflicht, kein inherit)

Dateien: gesamte exportierte Oberfläche (`go doc` mental über `memcached/*.go` ohne `_test.go`),
`README.md`, `doc.go`, `example_test.go`

Kein Line-by-Line der Parser. Frage: wirkt das Paket wie eine **stabile, enge, go-mäßige** Client-API?

Prüfe besonders:

- Package-Name vs Typen (`memcached.Client` gut; `memcached.MetaClient` / `GetResult` / `Item`).
- Ein Einstieg (`New`+`Config`) vs zu viele Wurzel-Typen.
- Dual-Welt Text+Meta: klar getrennt (`Meta()`) oder verwirrend (zwei Store/Get/Delete/Incr)?
- Zwei Pipeline-Formen (Builder vs `[]MetaOp`) — eine Regel, oder Altlast?
- Options-Muster: Struct-Opts (gut) vs Functional Options — nur rügen wenn gemischt.
- Exportierte Einzelbuchstaben `P`/`L`, Wire-Modes `"S"`/`"I"` — für Caller oder fürs Protokoll?
- Fehlende Symmetrie: Text hat GetOne, Meta nicht; Meta hat Noop/Debug, Text nicht.
- Härte gegen Missbrauch vs Ergonomie (zu viele Flags, zu wenig Helpers).
- Vergleichsmaßstab (nicht kopieren, nur Qualität): `database/sql`, `net/http` Client,
  go-redis v9 Command-API — Dest-out ist hier begründet; trotzdem: ist die kognitive Last professionell?
- Vor-1.0: welche BRÜCHE lohnen sich **jetzt**, welche wären nach Tag v1.0 zu teuer?

## Nach den 6 Agents (nur Orchestrator)

Orchestrator-Modell: ebenfalls **nur** Grok 4.6 Extra High Fast (`cursor-grok-4.6-xhigh-fast`).

1. Dedupliziere. Derselbe Dest-Bruch gilt einmal, nicht 6×.
2. Tabelle aller **exportierten** Typen und Methoden: Urteil + eine Zeile.
3. Liste NUR **BRUCH** (Breaking/Korrektheit) priorisiert: Caller-Falle zuerst, dann Uneinheitlichkeit, dann Protokoll-Mismatch.
4. Zweite Liste **GLATTZIEHEN** (Namen/Godoc), kurz.
5. **ABSICHT** nur sammeln, nicht als Arbeit verkaufen.
6. Keine Implementierung, außer ich sage danach „behebe“.
7. Kein Geschmack ohne Regelverstoß. „Ich hätte Add() als Methode“ ist kein BRUCH, wenn `Store`+Mode dokumentiert und durchgängig ist.
8. **Pflicht:** komplette Findings nach `docs/api-findings.md` schreiben (anlegen oder überschreiben). Nicht in `docs/api-agents.md`. Nicht nur im Chat. Datei muss enthalten: Tabelle, BRUCH-Liste, GLATTZIEHEN-Liste, ABSICHT-Kurzliste, Abschlussurteil.
9. Abschlussurteil in 5–8 Sätzen: Ist die API professionell und in sich optimal für eine Go-Memcached-Library — ja / ja mit GLATTZIEHEN / nein wegen BRUCH X.
10. Erst fertig, wenn `docs/api-findings.md` geschrieben ist.
