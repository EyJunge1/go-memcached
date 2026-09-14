# Performance-Analyse: parallele Agents

Prompt für einen Orchestrator. Er startet **6 Explore-Agents parallel**. Jeder Agent
bekommt nur seine API-Gruppe. Am Ende fasst der Orchestrator auf Deutsch zusammen.

**Modell (hart):** Jeder der 6 Agents und der Orchestrator selbst nur
**Grok 4.6 High Extra Fast**. Im Task-Tool: `model=cursor-grok-4.6-xhigh-fast`.
Kein `inherit`, kein anderes Modell, kein Fallback.

In Cursor: neuen Chat, Agent-Mode, Text unten einfügen. Die 6 Agents müssen in
**einer** Nachricht parallel starten.

---

Du bist Orchestrator. Analysiere die Performance der GESAMTEN öffentlichen API
von github.com/EyJunge1/go-memcached (Workspace: `/Users/arneleder/Development/gomemcached`).
Sprich am Ende Deutsch. Agents dürfen intern Englisch denken.

STARTE GENAU 6 EXPLORE-AGENTS PARALLEL (Task tool, `subagent_type=explore`,
thoroughness=very thorough, **`model=cursor-grok-4.6-xhigh-fast`**).
Jeder Agent bekommt NUR seine Gruppe.
Kein Agent ändert Dateien. Kein Agent schreibt Exploits.
Kein Agent darf ein anderes Modell verwenden.

## Hart (für jeden Agent, unverändert kopieren)

Repo: `/Users/arneleder/Development/gomemcached`  
Paket: `memcached/` (`livetest/` und `benchmark/` sind getrennte Module)

Modell: Grok 4.6 High Extra Fast (`cursor-grok-4.6-xhigh-fast`) — Pflicht.

Verbote:

- kein `unsafe`
- kein Alias von Conn-/bufio-/UDP-Scratch an den Caller nach Return
- gepoolte NS-Keys nie ungecopiert auf Dest legen
- VERSION/STAT/Opaque/Debug-Tokens nur per `append(dst[:0], token…)`
- keyBox: `adoptKeyBox` ODER `putBuilderBox` auf JEDEM Error-Pfad

Kontext (nicht neu beweisen):

- Dest-Reuse-Hot-Path (`Get`/`GetOne`/`Meta`/`Stats`/`Version`) ist ausgereizt, nicht
  theoretisch maximal. `ns/op` bleibt RTT. Zero-Alloc nur wenn `dst` + Value-Cap passen.
- Quiet (Text-`noreply`) wirft die Conn weg — das ist Spezifikation, kein Fix.
- Ketama/MD5 und Textprotokoll sind keine Obergrenze — nur melden, nicht ersetzen.
- Sicherheit sitzt in jeder öffentlichen Funktion (typisierte Builder, keine
  Rohbefehle). Das ersetzt kein TLS. Default bleibt Klartext-TCP.

Was zählt als echter Fund:

- Extra-Alloc auf dem Wiederverwendungs-Pfad obwohl Cap reicht
- `string(cmd)` / `string(key)` / `FormatUint` auf dem Hot-Path
- Closure/`afterRecv` das allokiert, obwohl ein named func + Response-Feld reicht
- `rawLines`-Parse NACH Conn-Return (Race) — das ist Bug, nicht „nice to have“
- Pool-Leak (Box nicht zurück)
- unnötiges `make`/`copy`/`map`/Goroutine auf Single-Node
- Dest-Aliasing (`Key`/`WireKey`/`Value` teilen Buffer mit unterschiedlicher Len)

Was NICHT zählt (als „must fix“):

- Watch/Metadump/MgDump: Channels + `map[string]string` sind Absicht
- `PoolStats` / `DeadServers` / `Servers`: Snapshot-Kopien
- Error-Wraps (`fmt.Errorf`)
- Multi-Node-Split (map + Goroutinen) wenn >1 Node
- One-Shot ohne Dest-Reuse
- Theoretische Binary-Protocol-/Ketama-Wechsel

Pro Funktion liefern:

1. Signatur
2. Hot-Path-Schritte (encode → dispatch → parse → dest)
3. Alloc-Quellen bei reused dest vs. one-shot
4. Urteil: **AUSGEREIZT** | **LECK** (mit `file:line` + warum) | **ABSICHT** (warum)
5. Wenn LECK: sicherer Fix in einem Satz, ohne `unsafe`, ohne Alias

## Agent 1 — Text Store / Mutate

Modell: `cursor-grok-4.6-xhigh-fast`

Dateien: `client_storage.go`, `client_delete.go`, `client_touch.go`,
`client_incr.go`, `protocol_storage.go`, `protocol_get.go` (nur soweit Store/Get sich teilen)

Funktionen:

- `Client.Store`
- `Client.Delete`
- `Client.Touch`
- `Client.Incr`
- `Client.Decr`

Varianten: alle `StoreMode`, Quiet/`noreply`, CAS, Namespace, UDP-Ablehnung.

## Agent 2 — Text Get + Pipeline

Modell: `cursor-grok-4.6-xhigh-fast`

Dateien: `client_get.go`, `client_pipeline.go`, `protocol_get.go`,
`protocol_response.go` (`readRetrieval`, `takeValueBuffer`)

Funktionen:

- `Client.Get`
- `Client.GetOne`
- `Client.Pipeline` / `Pipeline.Store` / `Delete` / `Touch` / `Run`

Varianten: 1 Key, N Keys, Cluster-Split, `GetOpts.CAS`/`Touch`, GetOne-Slot-Kopie,
Pipeline write-then-read, Wiederverwendung nach `Run`, Namespace (`wireGetKeys`).

## Agent 3 — Meta Einzelbefehle

Modell: `cursor-grok-4.6-xhigh-fast`

Dateien: `client_meta.go`, `client_meta_get.go` (nur Get, nicht Pipeline),
`client_meta_set.go`, `client_meta_delete.go`, `client_meta_arith.go`,
`client_meta_debug.go`, `client_meta_noop.go`,
`protocol_meta_*.go`, `protocol_builder.go` (`AddBase64`)

Funktionen:

- `Client.Meta`
- `MetaClient.Get` / `Set` / `Delete` / `Arithmetic` / `Noop` / `Debug`

Varianten: dest-reuse (Value/WireKey/Opaque/Fields), `ReturnKey`, BinaryKey+Base64,
Namespace (`nsKey`/`stripNS`), Quiet+`mn`, `afterRecv` vs. Parse-nach-Dispatch,
Key/WireKey-Aliasing.

## Agent 4 — Meta Pipeline + UDP

Modell: `cursor-grok-4.6-xhigh-fast`

Dateien: `client_meta_get.go` (Pipeline ab `encodeMetaPipelineOp`),
`protocol_udp.go`, `client_udp_test.go`, `protocol_udp_test.go`

Funktionen:

- `MetaClient.Pipeline`
- UDP-Pfade der erlaubten API: Get/`gets`, Version, `Meta.Get` (ohne `N`/`R`/`T`/`E`),
  `Meta.Noop`, `Meta.Debug`

Prüfe: Opaque-Match ohne string-Alloc, `fillMetaPipe` in dst, Quiet-Barrier `mn`,
`udpOut`/`udpIn`, `tot==1` in-place, Body-Kopie, Allow-List ohne `string(cmd)`.

## Agent 5 — Version / Stats / Cluster-Introspection

Modell: `cursor-grok-4.6-xhigh-fast`

Dateien: `client_version.go`, `protocol_version.go`, `client_stats.go`,
`protocol_stats.go`, `client.go` (`New`, `Close`, `SetServers`, `Servers`,
`PoolStats`, `ResolverErr`), `client_failover.go` (`DeadServers`)

Funktionen:

- `Client.Version` / `VersionAll`
- `Client.Stats` / `StatsAll`
- `Client.New` / `Close` / `SetServers` / `Servers` / `DeadServers` / `PoolStats` / `ResolverErr`
- `ParseServers`

Varianten: dest `*[]byte` / `*[]Stat` / `*[]Node*`, `afterRecv` vor Conn-Return,
Broadcast hält Conn, Namespace irrelevant außer `SetServers`.

## Agent 6 — Admin

Modell: `cursor-grok-4.6-xhigh-fast`

Dateien: `client_admin.go`, `client_admin_watch.go`, `client_flush.go`, `protocol_admin.go`

Funktionen:

- `Client.Admin`
- `Admin.On` / `All`
- `AdminNode`: `Flush`, `Verbosity`, `Shutdown`, `CacheMemLimit`, `Stats`, `StatsDetail`,
  `SlabsReassign`, `SlabsAutomove`, `LRUTune`, `LRUMode`, `LRUTempTTL`,
  `LRUCrawlerEnable`/`Sleep`/`ToCrawl`/`Crawl`,
  `Watch`, `LRUCrawlerMetaDump`, `LRUCrawlerMgDump`
- `AdminBroadcast`: `Flush`, `Verbosity`, `CacheMemLimit`

Quiet/`noreply`, Token-Validierung, Stream-Deadline. Watch/Dump: **ABSICHT** markieren
wenn Channel+map. Trotzdem: unnötige string-Splits, Extra-Kopien, `rawLines`-Race.

## Nach den 6 Agents (nur Orchestrator)

1. Dedupliziere. Ein Leak in dispatch/`read1` gilt einmal, nicht 6×.
2. Tabelle aller öffentlichen Funktionen: Urteil + eine Zeile Begründung.
3. Liste NUR echte LECKS, priorisiert (Hot-Path zuerst).
4. Keine Implementierung, außer ich sage danach „behebe“.
5. Kein „maximal“ — nur **AUSGEREIZT** / **LECK** / **ABSICHT**.
6. Findings in `docs/perf-findings.md` schreiben (nicht in diese Prompt-Datei).
7. Wenn nichts bleibt: eine Zeile „keine clientseitigen Leaks auf reused dest“.
