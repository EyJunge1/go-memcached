# Auftrag: Öffentliche API — ist sie top gestaltet?

Copy **Gemeinsamer Kopf** plus genau einen **AGENT**-Block.

**Modell:** ausschließlich **Grok 4.6 extra high fast** (xhigh fast). Cursor-Slug: `cursor-grok-4.6-xhigh-fast`. Kein anderes Modell. Kein Grok 4.6 fast high, kein high max, kein Grok 4.5, kein Claude, kein GPT, kein Composer. Kein Fallback, „weil xhigh busy ist“.

Vier Agenten **parallel** (1–4), danach AGENT 5. Agenten 1–4 schreiben nur Arbeitsnotizen (Fragmente), damit sie sich nicht überschreiben. **Deliverable ist eine Datei:** `docs/audit/16-api-design.md`. Chat ist kein Deliverable.

Kein Anwendungscode ändern.

**Findings:** offiziell nur in `docs/audit/16-api-design.md`. Fragmente sind Rohmaterial für AGENT 5 und danach irrelevant. Im Chat nur: welche Dateien geschrieben wurden.

| Wer | Schreibt | Rolle |
| --- | --- | --- |
| AGENT 1 | `docs/audit/fragments/api-agent1-app.md` | Arbeitsnotiz App-Befehle (80 %-Pfad) |
| AGENT 2 | `docs/audit/fragments/api-agent2-meta.md` | Arbeitsnotiz Meta + Recache + Pipelines |
| AGENT 3 | `docs/audit/fragments/api-agent3-config.md` | Arbeitsnotiz New/Config/Typen/Cluster |
| AGENT 4 | `docs/audit/fragments/api-agent4-admin-idiom.md` | Arbeitsnotiz Admin + Go-Idiom + Godoc |
| AGENT 5 | **`docs/audit/16-api-design.md`** | **einziges Deliverable** |

Ordner anlegen falls fehlend. Dieser Lauf ersetzt den eigenen Agent-Output (neuer vollständiger Lauf). Alte `api-agent*.md` / `16-api-design.md` nicht fortschreiben — neu schreiben.

Fehler-Audit (`13-errors.md`), Features-Audit (`14-features.md`) und Legacy-Audit (`15-legacy.md`) nicht nachplappern. Hier nur: **ist die Fläche, die ein Package-Nutzer aufruft, maximal klar, konsistent und zeitgemäß?**

Sprache: Deutsch. Spec-Zitate, Wire-Tokens, Identifier bleiben Englisch.

---

## 0. Warum dieser Lauf existiert

Das Package **ist nicht veröffentlicht**. Kein Tag, kein Downstream, keine SemVer-Garantie, keine produktiven Importer.

Deshalb darf die öffentliche Fläche **komplett neu geschnitten** werden. Ziel ist nicht „was ist da und darf bleiben“, sondern:

> Würde ein erfahrener Go-Autor diese API **heute so** veröffentlichen — oder schämt man sich in zwei Jahren?

Vergleichsmaßstab ist **nicht** yeqown, gomemcache oder php-memcached. Vergleichsmaßstab ist eine **moderne Go-Library**, die sich anfühlt wie `database/sql`, `net/http`, `redis/go-redis` (v9) oder `google.golang.org/grpc`: eine offensichtliche Happy Path, Options nur wo nötig, Typen statt `any`, `context.Context` zuerst, `errors.Is`, Godoc das ein Beispiel trägt.

Nicht schreiben: „bestehende Nutzer würden brechen“, „für Kompatibilität behalten“, „so macht gomemcache das auch“.

Doch schreiben: **ideale Signatur**, was der Caller nach dem Schnitt tippt, warum die alte Form schlechter ist.

Legacy (`15`) fragt: *was ist doppelt / tot / Fork?*  
Dieser Lauf fragt: *was die eine Fläche ist — und ob sie sich gut anfasst.*

---

## 1. Was „API-Design“ hier heißt (und was nicht)

### 1.1 Rein — muss auf die Liste

| Klasse | Frage |
| --- | --- |
| **HAPPY-PATH** | Kann ein Erstnutzer `New` + `Set` + `Get` ohne Options-Wald und ohne Meta? Ist das der kürzeste, klarste Weg? |
| **EIN-WEG** | Gibt es pro Job genau eine offensichtliche Methode, oder raten Caller zwischen `Get`/`GetOne`/`GetMulti`, `Set`/`Store`, `Pipeline`/`MetaPipeline`? |
| **KONSISTENZ** | Gleiche Dinge gleiche Form: Key immer `[]byte`? Quiet immer ein Feld, nie Extra-Methode? Options immer letzte Arg, nie mitten drin? |
| **OPTIONAL** | Optionale Werte: Pointer, Zero-Value, oder eigener Typ (`Exptime`)? Ist `nil` vs `0` vs fehlendes Feld für denselben Job überall gleich? |
| **TYP** | Öffentliche Typen, die der Caller **nennen** muss (`StoreRequest`, `MetaGet`, `GetOpts`). Sind sie klein, dokumentiert, ohne `any`? |
| **RÜCKGABE** | Slice vs Map vs `*Item` vs `MetaResult`. Miss = weglassen vs `ErrNotFound` vs Status-Enum. Ein mentales Modell. |
| **NAMEN** | Go-Idiom: `Get` nicht `FetchItems`, `Close` nicht `ShutdownClient`. Kein `GetOrVivify` wenn der Spec-Name die Fläche erklärt. Kein Leak (`Addr`, `Picker`) in App-Namen. |
| **ENTDECKBAR** | Steht die richtige Methode in `go doc` oben? Sind Admin/Meta hinter klaren Typen, nicht als 40 Methoden auf `*Client`? |
| **ZERO** | Tut der Zero-Value das Richtige (`Config{}` ungültig ohne Servers — klar? `GetOpts{}` = plain get — klar? `Failover: nil` = an)? |
| **BEISPIEL** | Passt das README-Snippet zur idealen API, oder entschuldigt es eine schiefe Signatur? |

### 1.2 Nicht dieser Lauf

- Fehlende Spec-Befehle / fehlende Flags → `14-features.md`. Hier nur, wenn die **Form** der vorhandenen API die Spec verbiegt.
- Sentinel-Taxonomie, `errors.Is`-Matrix → `13-errors.md`. Hier nur, wenn die **Signatur** den Fehler falsch macht (z. B. Miss als `nil, nil` vs `ErrNotFound` inkonsistent).
- Kill-Liste für Shims/Fork-Reste → `15-legacy.md`. Hier die **Ziel-API danach**: wie sie heißen und sich anfühlen soll. Doppelt erwähnen nur, wenn das Design-Urteil ohne den Schnitt unverständlich ist — dann eine Zeile Querverweis, kein Legacy-Roman.
- Binary / SASL / TAP: out of scope, nicht als API-Lücke.
- Interne unexported Helper, Parser, Pool-Implementierung: nur wenn sie **durch** die öffentliche Fläche lecken (`Addr`, `Picker`, `Builder`).

Wenn unsicher: Finding schreiben, Status `UNKLAR`, zwei Schnitte vorschlagen. Nicht verschweigen.

---

## 2. Zuerst Inventar, dann Urteil

Nicht aus dem Gedächtnis. Reihenfolge Pflicht:

1. **Export-Fläche:** `go doc -all github.com/EyJunge1/go-memcached/memcached` (oder Workspace-Modulpfad). Jeder exportierte Typ, jede Methode, jede `func New*`, jede `var Err*` — aber **Err\*** nur listen, nicht neu taxonomisieren.
2. **Caller-Sicht:** Lies `README.md` Usage, `memcached/doc.go`, dann die Dateien, die ein Importer wirklich importiert:
   - Einstieg: `client.go`, `config.go`, `item.go`, `exptime.go`, `errors.go`
   - App: `client_get.go`, `client_storage.go`, `client_delete.go`, `client_touch.go`, `client_incr.go`, `client_pipeline.go`, `client_version.go`, `client_stats.go`
   - Meta: `meta.go`, `client_meta_*.go`, `client_recache.go`
   - Admin: `client_admin.go`, `client_admin_watch.go`, `client_flush.go`
   - Cluster-Exports: `client_cluster.go`, `client_cluster_ketama.go`, `addr.go`
3. **Godoc-Probe:** Für jede Kernmethode: reicht der Kommentar, um sie ohne Spec zu benutzen? Fehlt ein Example?
4. **Fremde APIs nur als Form-Inspiration** (nicht nachbauen):
   - `database/sql` — wenige Methoden, klare Errors, `Close`
   - https://github.com/redis/go-redis — Cmdable vs Cluster, Options-Struct, Pipeline
   - https://pkg.go.dev/github.com/bradfitz/gomemcache/memcache — nur um zu sehen, was wir **nicht** kopieren
   - https://docs.memcached.org/protocols/basic/ und https://docs.memcached.org/protocols/meta/ — nur wenn eine Signatur die Spec-Semantik versteckt oder verdoppelt

Jede Claim: `datei:zeile` plus exportierter Identifier. Ohne Zeile kein Finding.

---

## 3. Pflicht-Prüfungen (jede Zeile abhaken)

### 3.1 Happy Path — der App-Caller in den ersten fünf Minuten

Inventarisiere alle `func (c *Client)` die **kein** `Meta*` / `Admin` / `Pipeline` sind.

| Check | Muss beantwortet werden |
| --- | --- |
| Construct | Ist `New(Config{Servers})` der einzige Einstieg? Braucht man `ParseServers`? Ist `Config` zu groß für den Default-Fall? |
| Set | `Set(ctx, key, value, flag, expiry)` — vier Pflicht-Args nach ctx. Besser `Set(ctx, Item)` oder `Set(ctx, key, value, StoreRequest)`? Flags `0` immer tippen? |
| Get | `Get(ctx, GetOpts{}, keys...)` — Options **vor** Keys. Idiom in Go: Variadic zuletzt, Options oft last struct. Ist die Reihenfolge falsch? |
| GetOne / GetMulti | Drei Namen, drei Rückgaben (`[]*Item` / `*Item`+`ErrNotFound` / `map[string]*Item`+implizites CAS). Ein Modell oder drei APIs? |
| Store vs Set/Add/… | Generisch + sechs Kurzformen. Kurzformen ohne `Quiet`. Wer soll welchen Weg nehmen? |
| Delete / Touch | `MutateOpts` — ein Feld. Passt. |
| Incr / Decr | Dieselbe `MutateOpts`, aber `Quiet` wird **abgelehnt**; Extra `IncrQuiet`/`DecrQuiet`. Für den Caller: Bruch. Ein Modell vorschlagen. |
| Version / Ping / Stats | `Version` vs `VersionAll`, `Stats` vs `StatsAll` — ein Node vs alle. Namensregel klar? Warum hat Get kein `GetAll`? |
| Keys | Überall `[]byte`. Gut für Wire, nervig für Literale. Bleibt `[]byte` die eine Regel? Dann Godoc+Example. Nie heimlich `string` nur bei Map-Keys (`GetMulti`). |
| Context | Überall zuerst? Pipeline: `Pipeline()` ohne ctx, `Flush(ctx)` — ok oder inkonsistent zu `MetaPipeline(ctx, ops)`? |

Regel: Nach dem Schnitt gibt es **einen** Happy Path, den das README zeigt, und **einen** erweiterten Path (Options-Struct). Keine dritte Familie.

### 3.2 Meta, Recache, beide Pipelines

| Check | Muss beantwortet werden |
| --- | --- |
| Meta-Namen | `MetaGet`/`MetaSet`/… vs kürzer `mg` in Godoc. Bleibt das Präfix? Liegen 6 Meta-Methoden + 3 Recache-Helper + 2 Pipelines zu laut auf `*Client`? Eigener `Meta`-Handle analog `Admin()`? |
| Flag-Structs | `MetaGet`, `MetaSet`, `MetaDelete`, `MetaArithmetic`, `MetaDebug`, `MetaNoop` — parallel, gut. Pointer-Felder (`*Exptime`, `*uint64`) vs `MetaGet.Recache time.Duration` (Zero = weg). Ein Optional-Modell. |
| `MetaGet.Return` | Nested `MetaGetReturn` mit 8 Bools. Besser Bitset, oder so lassen weil explizit? |
| `MetaOp.Flags any` | Typ-Flucht. Generics, Union, oder getrennte Slice-Typen. `any` ist kein v1-Export. |
| Recache-Helper | `GetOrVivify` / `GetEarlyRecache` / `Invalidate` — drittes Rückgabe-`bool` (`won`) **und** `res.Item.Won`. Redundant. Gehören Helper auf `*Client` oder als Example auf `MetaGet`? |
| MetaDebug | Gibt `*MetaItemDebug` statt `MetaResult`. Bruch. |
| Text-`Pipeline` vs `MetaPipeline` | Builder+`Flush` vs `[]MetaOp` in einem Call. Zwei Batch-Mentalmodelle. Eine Fläche oder bewusst zwei Welten (text vs meta) — dann Godoc muss das in einem Satz sagen. |
| Pipeline-Item | Text-Pipeline nimmt `*Item` (Exp am Item). `Client.Set` nimmt lose Args. `Store` nimmt `StoreRequest`. Drei Schreib-Formen. |

### 3.3 Config, Typen, die der Importer schreiben muss

| Check | Muss beantwortet werden |
| --- | --- |
| `Config` | Ein Struct, Felder gruppiert (`Transport`, `Pool`, `Cluster`). Ist die Zerlegung klar oder muss man 20 Felder kennen? Functional Options vs ein Struct — Urteil, nicht Mode. |
| Zero-Tricks | `Failover *Failover`: `nil` = an, `&Failover{}` = aus. Das ist ein API-Fußangeln. Besser `Failover Failover` + `Disable bool` oder explizites `Failover: Failover{Enabled: true}`? |
| `*bool` TCPNoDelay | `nil` = on. Gleicher Trick. Ein Muster für alle „Default an“-Flags. |
| `Exptime` / `Rel` / `UnixExp` | Starke Typen — gut. `flush_all` Delay ist `time.Duration` **und** `Exp Exptime`. Zwei Uhren. |
| `Item` vs `StoreRequest` vs `MetaItem` vs `MetaResult` | Vier Träger für Key/Value/CAS/Flags. Kann der Caller mit einem `Item` leben und Meta-Felder optional? |
| `Item.String` | Debug, Exp fehlt. Öffentlich halten? |
| Cluster-Exports | `Builder`, `Picker`, `Resolver`, `Addr`, `NewAddr`, `NewCRC32HashPickBuilder`, `NewMurmur3HashPickBuilder`, `NewRendezvousHashPickBuilder`, `NewRendezvousHashPickBuilderWithHash`, `NewKetamaPickBuilder`. Wie viel davon muss ein App-Caller **sehen**? Reicht `Cluster.PickBuilder` + 3 Konstruktoren, Rest unexported? |
| `Server` vs `Addr` | Caller spricht `Server`. `Addr` in Godoc/Resolver. Ein öffentlicher Node-Typ. |
| `ParseServers` | Behalten als Convenience. Rückgabe `[]Server` — passt. |
| `Statistic` / `ParseGeneralStats` | Riesiger getypter STAT-Katalog. Braucht die App-API das, oder `map[string]string` + optionales Decode? |

### 3.4 Admin, Idiom, Godoc, Beispiele

| Check | Muss beantwortet werden |
| --- | --- |
| Admin-Split | `Admin()` + `On` + `All` — moderne, bewusste Fläche. Ist unbound `FlushAll` = broadcast noch überraschend? |
| Stats-Doppel | `Client.Stats` / `StatsAll` **und** `Admin().Stats` / `StatsDetail`. Wer darf `detail`? Godoc muss das ohne README lösen. |
| Watch / Dumps | `<-chan WatchEvent` — Go-idiomatisch. Deadline-Pflicht (`ErrDeadlineRequired`) — in der Signatur sichtbar? |
| Allow-Flags | `AllowAdmin`, `AllowFlushAll`, `AllowSetServers` — drei Booleans. Klar oder Ceremony? |
| Godoc-Rang | `go doc` aufs Package: stehen `New`, `Client`, `Config`, `Item` oben, oder `Addr`/`Statistic`/`WireHit`? Was muss unexported oder in intern-Docs, damit die Fläche klein wirkt? |
| Examples | `ExampleClient_Set`, `ExampleClient_Get`, `ExampleConfig` — fehlen sie? Ohne Example ist die API nicht fertig. |
| Vergleichs-Idiom | Nicht Features klauen. Nur Form: macht unsere Signatur das, was Go-Leute erwarten (`Close() error`, keine Init-Funktionen, keine globalen Defaults außer Zero-Value)? |
| Breaking-Budget | Package ungenutzt: **jede** schlechtere Form darf weg. Deprecated-Aliase verboten. |

---

## 4. Bekannte Verdachtsstellen (Start, nicht Ende)

Das ist **keine** Vorverurteilung. Jede Zeile am **aktuellen** Code belegen, dann Status setzen. Weiter suchen — diese Liste ist unvollständig.

- `Set`/`Add`/`Replace`/`Cas`: lose Args; `Append`/`Prepend` ohne Flags/Exp; `Store`+`StoreRequest`+`Quiet` ist der volle Weg — drei Stufen
- `Get(ctx, opts, keys...)`: Options vor Variadic; `GetOne` Miss-Semantik; `GetMulti` Map+string-Key+implizites CAS
- `Incr`/`Decr` lehnen `MutateOpts.Quiet` ab; `IncrQuiet`/`DecrQuiet` existieren extra
- `Pipeline` Builder vs `MetaPipeline([]MetaOp)`; Quiet nur in Meta-Pipeline
- `GetOrVivify` / `GetEarlyRecache` drittes `bool`; `Invalidate` nur `error`
- `MetaOp.Flags any`
- `MetaDebug` → `*MetaItemDebug`; Rest → `MetaResult`
- `FlushOpts.Delay` **und** `FlushOpts.Exp`
- `Config.Failover *Failover` nil-Trick; `Transport.TCPNoDelay *bool`
- `Item` / `StoreRequest` / `MetaItem` / `MetaResult` Überlappung
- `Addr`, `NewAddr`, `Picker`, `Builder`, `Resolver` auf der öffentlichen Fläche
- `Client.Stats` vs `Admin().Stats`; `Admin().FlushAll` unbound = broadcast
- README-Usage zeigt `Set`+`Get`+`GetOpts{CAS:true}` — ist das der Pfad, den wir zementieren wollen?
- Keys `[]byte` überall, `GetMulti` mappt auf `string`

---

## 5. Ausgabeformat

**Eine Findings-Datei.** AGENT 5 schreibt sie neu. Agenten 1–4 fassen `16-api-design.md` nicht an.

```text
docs/audit/fragments/api-agent1-app.md
docs/audit/fragments/api-agent2-meta.md
docs/audit/fragments/api-agent3-config.md
docs/audit/fragments/api-agent4-admin-idiom.md
docs/audit/16-api-design.md                 # Deliverable, nur AGENT 5
```

Kopf jeder Datei:

- Datum, Agent-Nummer, Modell **Grok 4.6 extra high fast**
- Wie die Export-Liste erzeugt wurde (`go doc` / manuell)
- Zählung im eigenen Scope: NEU-SCHNEIDEN / VEREINHEITLICHEN / UMBENENNEN / BEHALTEN / UNKLAR

Dann eine Tabelle im eigenen Scope:

| Identifier / Fläche | Klasse | Caller-Job | Heutige Form | Status | Code | Soll-Signatur |

Klasse nur: `HAPPY-PATH` | `EIN-WEG` | `KONSISTENZ` | `OPTIONAL` | `TYP` | `RÜCKGABE` | `NAMEN` | `ENTDECKBAR` | `ZERO` | `BEISPIEL`

Status nur: `NEU-SCHNEIDEN` | `VEREINHEITLICHEN` | `UMBENENNEN` | `BEHALTEN` | `UNKLAR`

Danach Findings, eines pro Fläche:

```
### <Titel>
- Status:
- Priorität: P0 | P1 | P2
- Kategorie: App | Meta | Config | Admin | Docs
- Klasse: HAPPY-PATH | EIN-WEG | KONSISTENZ | OPTIONAL | TYP | RÜCKGABE | NAMEN | ENTDECKBAR | ZERO | BEISPIEL
- Code: datei:zeile
- Ist: was der Importer heute tippt
- Soll: konkrete Go-Signatur(en) danach (copy-paste-fähig)
- Caller-Beispiel: 4–8 Zeilen idealer Nutzung
- Weg: was umbenannt / zusammengelegt / gelöscht wird
- Breaking: ja — warum das vor Release weg muss
- Nicht tun: kein zweiter Name, kein Deprecated-Wrapper, kein „Options-Pattern on top“
```

Prio:

- **P0**: Happy Path ist umständlich oder mehrdeutig (`Get`/`GetOne`/`GetMulti`, Set-Args vs Store, Quiet-Bruch). Oder ein öffentlicher Typ, den jeder Importer sieht und der nach v1 nicht mehr einfangbar ist (`any`, `Addr`, nil-Trick).
- **P1**: Zwei Mentalmodelle (Pipeline vs MetaPipeline, Item vs StoreRequest), Optional-Inkonsistenz, Admin/Stats-Entdeckbarkeit.
- **P2**: Godoc-Rang, fehlende Examples, Stringer, Namensfeinschliff.

AGENT 5 hängt an `16-api-design.md` zusätzlich:

- Master-Tabelle aller Identifier
- **Ziel-Fläche** (nur Namen + eine Zeile Signatur, kein Roman):
  - Construct: `New`, `Config`, `ParseServers`, `Close`
  - App-Methoden
  - Meta (Handle oder Präfix — eine Entscheidung)
  - Admin
  - Cluster, das der App-Caller noch sehen darf
- **Schnitt-Liste** (heute → morgen, eine Zeile pro Identifier)
- **Behalten mit Begründung** (klare Semantik, nicht Gewohnheit)
- **Godoc/Example-Pflicht** nach dem Schnitt (`ExampleClient_*`)
- Was README/`doc.go` nach dem Schnitt als *den* Happy Path zeigen muss

Querverweis nur: Legacy-Kill in `15-legacy.md`; Fehler-Sentinels in `13-errors.md`; fehlende Befehle in `14-features.md`. Hier keine neuen Wire-Features erfinden.

---

## 6. Gemeinsamer Kopf

```text
Repo: Workspace-Root go-memcached. Package memcached/.
Modul: github.com/EyJunge1/go-memcached

Modell: ausschließlich Grok 4.6 extra high fast (xhigh fast).
Cursor-Slug: cursor-grok-4.6-xhigh-fast.
Kein anderes Modell. Kein Grok 4.6 fast high. Kein high max. Kein Grok 4.5.
Kein Claude, kein GPT, kein Composer, kein inherit-ausweichen.

Auftrag: API-Design-Audit der öffentlichen Fläche (was Importer aufrufen).
Keine Commits, in diesem Lauf kein Anwendungscode ändern.
Agent 1–4: Findings nur ins eigene Fragment (Rohmaterial).
AGENT 5: alle Findings in EINE Datei docs/audit/16-api-design.md.
Kein Chat-only-Ergebnis. Kein „siehe oben, Datei spare ich mir“.
Chat: nur welche Dateien geschrieben/ergänzt wurden.

WICHTIG — pre-release, nicht veröffentlicht, nicht in Benutzung:
Keine API-Stabilität. Kein SemVer. Breaking ist der Default.
Rückwärtskompatibilität ist kein Argument.
Deprecated-Wrapper sind verboten.
yeqown / gomemcache sind kein Vorbild für Form.
Maßstab: moderne Go-Library (klar, klein, ein Weg pro Job).

Zuerst:
1) go doc -all auf das Package (Export-Inventar)
2) README.md Usage + memcached/doc.go
3) Dateien der öffentlichen Fläche (client_*.go, config.go, item.go, meta.go, exptime.go)
4) Form-Inspiration, nicht Kopie:
   database/sql, redis/go-redis v9
   https://pkg.go.dev/github.com/bradfitz/gomemcache/memcache
5) Spec nur wenn eine Signatur Semantik versteckt:
   https://docs.memcached.org/protocols/basic/
   https://docs.memcached.org/protocols/meta/

Jede Aussage: Datei:Zeile + Identifier.
Status: NEU-SCHNEIDEN | VEREINHEITLICHEN | UMBENENNEN | BEHALTEN | UNKLAR
Klasse: HAPPY-PATH | EIN-WEG | KONSISTENZ | OPTIONAL | TYP | RÜCKGABE | NAMEN | ENTDECKBAR | ZERO | BEISPIEL
Prio: P0 | P1 | P2
Kategorie: App | Meta | Config | Admin | Docs

Deutsch. Eintrags-Template aus dem Prompt verwenden.
13-errors / 14-features / 15-legacy nicht nachplappern — am aktuellen Code neu belegen.
Nicht „Kompatibilität behalten“. Idealzustand + konkrete Soll-Signatur.
```

---

## AGENT 1 — App-Befehle (Happy Path)

```text
Rolle: Erstnutzer-Anwalt. Gemeinsamer Kopf gilt. Abschnitte 1, 3.1, 4 (App).

Alle exportierten Methoden auf *Client außer Meta*, Admin(), Pipeline/MetaPipeline.
Construct nur soweit Set/Get ihn braucht (New+Config kurz; Details = Agent 3).

Set/Store-Familie, Get/GetOne/GetMulti, Delete/Touch, Incr-Quiet-Bruch,
Version/Ping/Stats-Namensregel, Key-[]byte, Arg-Reihenfolge, Context.

Schreiben:
- docs/audit/fragments/api-agent1-app.md
```

---

## AGENT 2 — Meta, Recache, Pipelines

```text
Rolle: Fortgeschrittene Fläche. Gemeinsamer Kopf gilt. Abschnitte 1, 3.2, 4 (Meta).

MetaGet/Set/Delete/Arithmetic/Noop/Debug, Flag-Structs, MetaResult vs MetaItemDebug,
MetaOp.Flags any, Recache-Helper, Pipeline vs MetaPipeline, Item vs StoreRequest vs MetaItem.

Eine Entscheidung: Meta auf *Client lassen oder Client.Meta()-Handle analog Admin().

Schreiben:
- docs/audit/fragments/api-agent2-meta.md
```

---

## AGENT 3 — Config, Typen, Cluster-Exports

```text
Rolle: Was der Importer deklarieren muss. Gemeinsamer Kopf gilt. Abschnitte 1, 3.3, 4 (Typen).

go doc -all: jeder exportierte Typ/Konstruktor außer den Methoden aus Agent 1/2
(Identifier trotzdem listen, wenn der Typ die App-API aufbläht).

Config/Transport/Pool/Cluster/Failover/Auth, Exptime, Item-Familie,
Addr/NewAddr, Builder/Picker/Resolver, Hash-Konstruktoren,
Statistic/ParseGeneralStats, PoolStat, Zero-Tricks (nil Failover, *bool).

Schreiben:
- docs/audit/fragments/api-agent3-config.md
```

---

## AGENT 4 — Admin, Go-Idiom, Godoc, Beispiele

```text
Rolle: Entdeckbarkeit und Handwerk. Gemeinsamer Kopf gilt. Abschnitte 1, 3.4.

Admin()/On/All, FlushAll-Überraschung, Stats-Doppel App vs Admin,
Watch/Dump-Kanäle, Allow-Flags.
go doc Package-Ansicht: was zu weit oben steht.
Fehlende Example* Funktionen.
Form gegen database/sql und go-redis — nur Idiom, keine Feature-Liste.

Nicht die Feature-Vollständigkeit von Admin neu auditieren (05 / 14).
Nur: ist die Form so, dass ein Ops-Caller sie ohne README trifft.

Schreiben:
- docs/audit/fragments/api-agent4-admin-idiom.md
```

---

## AGENT 5 — Synthese (erst nach 1–4)

```text
Rolle: Chefredakteur. Gemeinsamer Kopf gilt.

Nur die vier Fragment-Dateien lesen, nicht neu spekulieren.
Duplikate zusammenziehen. Widerspruch: eine klare Caller-Form > Spec-Abbildung > aktueller Code.
Interop und Fork verlieren immer gegen eine kleinere, konsistentere Fläche.

Schreiben:
- docs/audit/16-api-design.md   ← einziges Deliverable
  (Kopf mit Modell, Master-Tabelle, Ziel-Fläche mit Signaturen,
   Schnitt-Liste, Keep-Liste, Godoc/Example-Pflicht, README-Happy-Path)
- Fragmente nicht löschen (Rohmaterial), aber nichts nur dort lassen
```

---

## 7. Ablauf

1. Vier Agenten **parallel**, jeder mit Kopf + seinem AGENT-Block. Modell nur **Grok 4.6 extra high fast** (`cursor-grok-4.6-xhigh-fast`).
2. AGENT 5 (erst nach 1–4, gleiches Modell) liest die vier Fragmente und schreibt **eine** Datei `docs/audit/16-api-design.md`.
3. Fertig = `docs/audit/16-api-design.md` vollständig. Fragmente dürfen liegen bleiben, zählen aber nicht. In diesem Lauf kein Patch am Client.

Nicht tun:

- Anwendungscode patchen
- Deprecated-Aliase vorschlagen („alte Funktion bleibt“)
- Binary/SASL als Missing Feature
- „für gomemcache-Kompatibilität die Signatur behalten“
- Error-, Feature- oder Legacy-Audit abschreiben
- ein anderes Modell wählen, „weil xhigh busy ist“
- Findings nur im Chat oder nur in Fragmenten lassen
- mehrere gleichrangige Audit-MDs statt einer `16-api-design.md`
