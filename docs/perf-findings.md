# Performance-Findings (öffentliche API)

Stand: 2026-09-14 (nach LECK 1–8). Nur **AUSGEREIZT** / **LECK** / **ABSICHT**.
Kein `unsafe`, kein Dest-Alias. Agents: Store/Mutate, Get/Pipeline, Meta-Einzelbefehle,
Meta-Pipeline/UDP, Version/Stats/Cluster, Admin.

Ein Leak in `read2`/`release` gilt **einmal**, nicht pro Aufrufer.

---

## Geschlossen (LECK 1–8)

### 1. Stats-Dest-Reuse — **AUSGEREIZT**

`read2` schreibt STAT/PREFIX während der gehaltenen Conn in `statsDst`
(`consumeStatsLine` + `appendStat`). Kein `bytes.Clone`, kein `rawLines`.
`rawLinesKeepCap == 64` wie der Stats-Hint. `StatsAll` setzt `statsDst` vor
`sendOnConn`. `StatsDetail(dump)` setzt `statsDetail`, damit PREFIX erhalten bleibt.

`AdminNode.Stats("reset")` und `StatsDetail(on/off)` bleiben `read1`.

### 2. Version-Frame — **AUSGEREIZT**

Interniertes `_VersionCRLFBytes` als `req.raw`. `putBuilderBuf` ist No-Op und
poolt unboxed Raw nicht — internierte Konstanten bleiben sicher.

### 3. Stats-`cmd` — **AUSGEREIZT**

Interniertes `_StatsCmdBytes` in `buildStatsCommand` / `buildStatsDetailCommand`.

### 4. BinaryKey `base64Decode` — **AUSGEREIZT**

`base64DecodeInto(dst, src)` nutzt Dest-Cap, wenn sie reicht und nicht mit
`WireKey` aliased (`sameByteArray` → neue Alloc). `copyMetaUserKey` /
Debug kopieren den User-Key danach in `keepUser` (nie WireKey-Alias).

### 5. Meta-Pipeline Opaque-Map — **AUSGEREIZT**

Duplikate per Scan (`duplicatePipelineOpaque`). `metaPipeByOpaque` bleibt nil.
Match weiter per Scan.

### 6. UDP-Assemble — **AUSGEREIZT**

Stack `[32]int` für Offsets/Längen. `make` nur wenn `tot > 32`. `tot==1`
in-place, `tot≤32` ohne Heap.

### 7. Stats-Args — **AUSGEREIZT**

`EqualFold` statt `ToLower`. `strings.Join` einmal vor Encode / Broadcast.

### 8. Watch/Dump-Reader — **AUSGEREIZT**

`var tmp [1]byte`. `adminStreamLineError([]byte)` ohne String-Pendeln.
Channel + `map[string]string` / `Line string` bleiben **ABSICHT**.

---

## Kein must-fix (geprüft, nicht als LECK führen)

- Quiet Text-`noreply` wirft die Conn weg — Spezifikation.
- Meta-Quiet hängt `mn` an, Conn bleibt.
- UDP lehnt Mutationen, `gat`/`gats`, Stats, Admin ab — Allow-List per `bytes.Equal`.
- Text-Incr/Decr ohne Quiet — Spezifikation (`Meta.Arithmetic` + `q`).
- Multi-Node: Map + Goroutinen — **ABSICHT**.
- One-Shot ohne Dest-Reuse, Error-Wraps, Ketama/MD5, Watch-Maps.
- `liveAddrs`-`make` nur bei `Failover.Eject` (Default aus).
- `parseMetaItem` nach Dispatch: Get/Set/Del/Arith setzen `metaItem` vor Send;
  Fill in `read1`/`read2` bei gehaltener Conn. Quiet-Miss: leere `rawLines`, kein
  bufio-Alias. Debug/Noop: named `afterRecv` vor Conn-Return.
- keyBox: `putBuilderBox` auf Validate-Fehlern, sonst `adoptKeyBox` /
  `putPreparedMeta`. Kein NS-Key ungecopiert auf Dest.
- `GetResult` speichert keinen Key. Meta `copyMetaUserKey` trennt Key von WireKey.
- `StatsAll`: inneres `[]Stat` pro Node lokal (Broadcast-Race); Dest-Reuse gilt
  für `[]NodeStats` und für `Client.Stats`.

---

## Tabelle öffentliche Funktionen

| Funktion | Urteil | Begründung |
|---|---|---|
| `Client.Store` | **AUSGEREIZT** | internierte Verben, `AddUint`, named `mapStorageReply`; Quiet/UDP **ABSICHT** |
| `Client.Delete` | **AUSGEREIZT** | wie Store; Quiet/UDP **ABSICHT** |
| `Client.Touch` | **AUSGEREIZT** | `AddInt64`; Quiet/UDP **ABSICHT** |
| `Client.Incr` / `Decr` | **AUSGEREIZT** | `arithValue` vor Return; kein Quiet, UDP **ABSICHT** |
| `Client.Get` | **AUSGEREIZT** | Fill in Dest während `recv`; Split **ABSICHT**; One-Shot-Value **ABSICHT** |
| `Client.GetOne` | **AUSGEREIZT** | in-place Slot, keyBox auf jedem Error-Pfad |
| `Client.Pipeline` | **ABSICHT** | Handle-Alloc; Pointer nach `Run` wiederverwenden |
| `Pipeline.Store` / `Delete` / `Touch` | **AUSGEREIZT** | named `afterRecv`, keyBox via `Run`→`release` |
| `Pipeline.Run` | **AUSGEREIZT** | write-then-read, 1 Node ohne Map; Multi-Node **ABSICHT** |
| `Client.Meta` | **AUSGEREIZT** | `&c.meta`, kein Heap |
| `MetaClient.Get` | **AUSGEREIZT** | `fillMetaItem` + `takeValueBuffer`; BinaryKey in Dest-Cap |
| `MetaClient.Set` | **AUSGEREIZT** | `finishMetaDest` / Fill; Body = Caller-Alias nur während Send |
| `MetaClient.Delete` | **AUSGEREIZT** | wie Set |
| `MetaClient.Arithmetic` | **AUSGEREIZT** | VA in Dest-Cap; Quiet+`mn` **ABSICHT** |
| `MetaClient.Noop` | **AUSGEREIZT** | named `mapMetaNoopReply`, kein Key |
| `MetaClient.Debug` | **AUSGEREIZT** | Tokens per `append(dst[:0], …)` vor Return; BinaryKey in Dest-Cap |
| `MetaClient.Pipeline` | **AUSGEREIZT** | Auto-O, Opaque-Dup per Scan, `fillMetaPipe` in dst |
| UDP `Get`/`gets` | **AUSGEREIZT** | tot=1 in-place, tot≤32 Stack; Touch **ABSICHT** Reject |
| UDP `Version` | **AUSGEREIZT** | internierter Frame; Payload owned, Token-Append |
| UDP `Meta.Get`/`Noop`/`Debug` | **AUSGEREIZT** | Allow-List ohne `string(cmd)` |
| `Client.Version` | **AUSGEREIZT** | internierter Frame; Parse/Dest |
| `Client.VersionAll` | **AUSGEREIZT** | wie Version; 1 Node ohne Go; N Nodes **ABSICHT** |
| `Client.Stats` | **AUSGEREIZT** | Fill in `read2`, internierter cmd, `EqualFold` |
| `Client.StatsAll` | **AUSGEREIZT** | wie Stats, Join einmal; N Nodes **ABSICHT** |
| `New` | **ABSICHT** | Konstruktion |
| `Client.Close` | **ABSICHT** | Teardown |
| `Client.SetServers` | **ABSICHT** | Membership |
| `Client.Servers` | **ABSICHT** | Snapshot |
| `Client.DeadServers` | **ABSICHT** | Snapshot |
| `Client.PoolStats` | **ABSICHT** | Snapshot |
| `Client.ResolverErr` | **AUSGEREIZT** | Getter, keine Extra-Alloc |
| `ParseServers` | **ABSICHT** | One-Shot-Parser |
| `Client.Admin` / `On` / `All` | **AUSGEREIZT** | Factory |
| `AdminNode.Flush` / `AdminBroadcast.Flush` | **AUSGEREIZT** | `mapStatusLine` vor Return; Quiet **ABSICHT**; N Nodes **ABSICHT** |
| `AdminNode.Verbosity` / `CacheMemLimit` / `Shutdown` | **AUSGEREIZT** | named `afterRecv`, kein Dest; Encode-Itoa One-Shot |
| `AdminBroadcast.Verbosity` / `CacheMemLimit` | **AUSGEREIZT** | 1 Node ohne Go; N **ABSICHT** |
| `AdminNode.SlabsReassign` / `SlabsAutomove` | **AUSGEREIZT** | `do`, numerische Tokens |
| `AdminNode.LRUTune` / `LRUMode` / `LRUTempTTL` | **AUSGEREIZT** | `do`; Float/Itoa One-Shot |
| `AdminNode.LRUCrawlerEnable` / `Sleep` / `ToCrawl` / `Crawl` | **AUSGEREIZT** | `do`; `Sprintf` One-Shot |
| `AdminNode.Stats` reset | **AUSGEREIZT** | `read1`, Dest `[:0]` |
| `AdminNode.Stats` sizes | **AUSGEREIZT** | wie Client.Stats, Fill in `read2` |
| `AdminNode.StatsDetail` on/off | **AUSGEREIZT** | 1-Zeilen-OK |
| `AdminNode.StatsDetail` dump | **AUSGEREIZT** | PREFIX/STAT in Dest während `read2` |
| `AdminNode.Watch` | **ABSICHT** | Channel+Map; Reader Stack-Byte |
| `AdminNode.LRUCrawlerMetaDump` | **ABSICHT** | Channel+Map; Reader Stack-Byte |
| `AdminNode.LRUCrawlerMgDump` | **ABSICHT** | Channel+`Line`; Reader Stack-Byte |
