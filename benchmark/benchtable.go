//go:build ignore

// Aggregates go test -bench output: median/mean/min/max and a package comparison table.
//
//	go run benchtable.go [-append] results/bench-all.txt
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

const summaryMarker = "=== BENCHMARK SUMMARY ==="

var knownPackages = []string{"ours", "yeqown", "bradfitz", "rainycape"}

type metrics struct {
	nsOp     float64
	bOp      float64
	allocs   float64
	hasB     bool
	hasAlloc bool
}

type sample struct {
	nsOp   []float64
	bOp    []float64
	allocs []float64
}

type stats struct {
	n      int
	nsMed  float64
	nsMean float64
	nsMin  float64
	nsMax  float64
	bMed   float64
	aMed   float64
	hasB   bool
	hasA   bool
}

func main() {
	appendOut := flag.Bool("append", false, "rewrite file with summary appended")
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: go run benchtable.go [-append] bench.txt")
		os.Exit(2)
	}
	inPath := flag.Arg(0)

	raw, err := os.ReadFile(inPath)
	if err != nil {
		fatal(err)
	}
	body := stripSummary(raw)
	samples := parseBench(body)
	if len(samples) == 0 {
		fatal(fmt.Errorf("no benchmark lines found in %s", inPath))
	}

	var buf bytes.Buffer
	writeSummary(&buf, samples)
	summary := buf.Bytes()
	fmt.Print(string(summary))

	if *appendOut {
		out := append(append(body, '\n'), summary...)
		if err := os.WriteFile(inPath, out, 0o644); err != nil {
			fatal(err)
		}
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "benchtable:", err)
	os.Exit(1)
}

func stripSummary(raw []byte) []byte {
	if i := bytes.Index(raw, []byte(summaryMarker)); i >= 0 {
		raw = bytes.TrimRight(raw[:i], "\n")
		return append(raw, '\n')
	}
	if !bytes.HasSuffix(raw, []byte("\n")) {
		raw = append(raw, '\n')
	}
	return raw
}

func parseBench(raw []byte) map[string]*sample {
	out := make(map[string]*sample)
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		name, m, ok := parseLine(sc.Text())
		if !ok {
			continue
		}
		s := out[name]
		if s == nil {
			s = &sample{}
			out[name] = s
		}
		s.nsOp = append(s.nsOp, m.nsOp)
		if m.hasB {
			s.bOp = append(s.bOp, m.bOp)
		}
		if m.hasAlloc {
			s.allocs = append(s.allocs, m.allocs)
		}
	}
	return out
}

func parseLine(line string) (name string, m metrics, ok bool) {
	if !strings.HasPrefix(line, "Benchmark") {
		return "", metrics{}, false
	}
	fields := strings.Fields(line)
	if len(fields) < 3 {
		return "", metrics{}, false
	}
	name = stripGOMAXPROCS(fields[0])
	for i := 2; i+1 < len(fields); i += 2 {
		val, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			continue
		}
		switch fields[i+1] {
		case "ns/op":
			m.nsOp = val
			ok = true
		case "B/op":
			m.bOp = val
			m.hasB = true
		case "allocs/op":
			m.allocs = val
			m.hasAlloc = true
		}
	}
	return name, m, ok
}

func stripGOMAXPROCS(name string) string {
	if i := strings.LastIndexByte(name, '-'); i > 0 {
		if _, err := strconv.Atoi(name[i+1:]); err == nil {
			return name[:i]
		}
	}
	return name
}

func summarize(s *sample) stats {
	st := stats{n: len(s.nsOp)}
	st.nsMed, st.nsMean, st.nsMin, st.nsMax = agg(s.nsOp)
	if len(s.bOp) > 0 {
		st.hasB = true
		st.bMed, _, _, _ = agg(s.bOp)
	}
	if len(s.allocs) > 0 {
		st.hasA = true
		st.aMed, _, _, _ = agg(s.allocs)
	}
	return st
}

func agg(vals []float64) (med, mean, min, max float64) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	cp := append([]float64(nil), vals...)
	sort.Float64s(cp)
	min, max = cp[0], cp[len(cp)-1]
	var sum float64
	for _, v := range cp {
		sum += v
	}
	mean = sum / float64(len(cp))
	mid := len(cp) / 2
	if len(cp)%2 == 0 {
		med = (cp[mid-1] + cp[mid]) / 2
	} else {
		med = cp[mid]
	}
	return med, mean, min, max
}

type rowKey struct {
	bench string
	rest  string
}

func splitClient(full string) (pkg, bench, rest string, ok bool) {
	parts := strings.Split(full, "/")
	if len(parts) < 2 {
		return "", "", "", false
	}
	bench = parts[0]
	for _, p := range knownPackages {
		if parts[1] == p {
			return p, bench, strings.Join(parts[2:], "/"), true
		}
	}
	return "", "", "", false
}

func writeSummary(w io.Writer, samples map[string]*sample) {
	statsByName := make(map[string]stats, len(samples))
	for name, s := range samples {
		statsByName[name] = summarize(s)
	}

	type combo struct {
		key  rowKey
		pkgs map[string]stats
	}
	combos := map[string]*combo{}
	var other []string

	for name := range statsByName {
		pkg, bench, rest, ok := splitClient(name)
		if !ok {
			other = append(other, name)
			continue
		}
		key := bench
		if rest != "" {
			key = bench + "/" + rest
		}
		c := combos[key]
		if c == nil {
			c = &combo{key: rowKey{bench: bench, rest: rest}, pkgs: map[string]stats{}}
			combos[key] = c
		}
		c.pkgs[pkg] = statsByName[name]
	}

	keys := make([]string, 0, len(combos))
	for k := range combos {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	sort.Strings(other)

	fmt.Fprintln(w, summaryMarker)
	fmt.Fprintln(w, "median / mean / min / max over -count runs; lower ns/op is better")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "--- package comparison (median ns/op) ---")
	header := fmt.Sprintf("%-42s %12s %12s %12s %12s %10s", "benchmark", "ours", "yeqown", "bradfitz", "rainycape", "best")
	fmt.Fprintln(w, header)
	fmt.Fprintln(w, strings.Repeat("-", len(header)))

	wins := map[string]int{}
	for _, k := range keys {
		c := combos[k]
		cells := make([]string, len(knownPackages))
		bestPkg := ""
		bestNS := math.Inf(1)
		for i, p := range knownPackages {
			st, ok := c.pkgs[p]
			if !ok {
				cells[i] = "-"
				continue
			}
			cells[i] = formatNS(st.nsMed)
			if st.nsMed < bestNS {
				bestNS = st.nsMed
				bestPkg = p
			}
		}
		if bestPkg != "" {
			wins[bestPkg]++
		}
		label := strings.TrimPrefix(c.key.bench, "Benchmark")
		if c.key.rest != "" {
			label += "/" + c.key.rest
		}
		fmt.Fprintf(w, "%-42s %12s %12s %12s %12s %10s\n",
			truncate(label, 42), cells[0], cells[1], cells[2], cells[3], bestPkg)
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, "wins (lowest median ns/op):")
	for _, p := range knownPackages {
		fmt.Fprintf(w, "  %-10s %d\n", p, wins[p])
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, "--- detail: median [min..max] mean | B/op | allocs/op ---")
	for _, k := range keys {
		c := combos[k]
		label := strings.TrimPrefix(c.key.bench, "Benchmark")
		if c.key.rest != "" {
			label += "/" + c.key.rest
		}
		fmt.Fprintf(w, "\n%s\n", label)
		for _, p := range knownPackages {
			st, ok := c.pkgs[p]
			if !ok {
				continue
			}
			fmt.Fprintf(w, "  %-10s %s\n", p, formatDetail(st))
		}
	}

	if len(other) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "--- non-comparison ---")
		for _, name := range other {
			label := strings.TrimPrefix(name, "Benchmark")
			fmt.Fprintf(w, "%-54s %s\n", truncate(label, 54), formatDetail(statsByName[name]))
		}
	}
	fmt.Fprintln(w)
}

func formatDetail(st stats) string {
	s := fmt.Sprintf("ns/op median=%s mean=%s min=%s max=%s (n=%d)",
		formatNS(st.nsMed), formatNS(st.nsMean), formatNS(st.nsMin), formatNS(st.nsMax), st.n)
	if st.hasB {
		s += fmt.Sprintf(" | %.0f B/op", st.bMed)
	}
	if st.hasA {
		s += fmt.Sprintf(" | %.0f allocs/op", st.aMed)
	}
	return s
}

func formatNS(v float64) string {
	switch {
	case v >= 1_000_000:
		return fmt.Sprintf("%.2fms", v/1_000_000)
	case v >= 10_000:
		return fmt.Sprintf("%.1fµs", v/1_000)
	case v >= 1_000:
		return fmt.Sprintf("%.2fµs", v/1_000)
	default:
		return fmt.Sprintf("%.0fns", v)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 3 {
		return s[:n]
	}
	return s[:n-3] + "..."
}
