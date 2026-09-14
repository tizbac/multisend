package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	Reset     = "\033[0m"
	Bold      = "\033[1m"
	Red       = "\033[31m"
	Green     = "\033[32m"
	Yellow    = "\033[33m"
	Blue      = "\033[34m"
	Magenta   = "\033[35m"
	Cyan      = "\033[36m"
	White     = "\033[37m"
	BoldRed   = "\033[1;31m"
	BoldGreen = "\033[1;32m"
	BoldCyan  = "\033[1;36m"
	BoldWhite = "\033[1;37m"
)

const refreshInterval = time.Second

type SpeedTracker struct {
	mu         sync.Mutex
	start      time.Time
	lastBytes  uint64
	lastTime   time.Time
	totalBytes uint64
}

func NewSpeedTracker() *SpeedTracker {
	now := time.Now()
	return &SpeedTracker{start: now, lastTime: now}
}

func (st *SpeedTracker) Add(n uint64) {
	st.mu.Lock()
	st.totalBytes += n
	st.mu.Unlock()
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func formatSpeed(bytesPerSec float64) string {
	if bytesPerSec < 1024 {
		return fmt.Sprintf("%.0f B/s", bytesPerSec)
	}
	if bytesPerSec < 1024*1024 {
		return fmt.Sprintf("%.2f KiB/s", bytesPerSec/1024)
	}
	if bytesPerSec < 1024*1024*1024 {
		return fmt.Sprintf("%.2f MiB/s", bytesPerSec/(1024*1024))
	}
	return fmt.Sprintf("%.2f GiB/s", bytesPerSec/(1024*1024*1024))
}

func (st *SpeedTracker) Speed() float64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	elapsed := time.Since(st.lastTime).Seconds()
	if elapsed < 0.05 {
		return 0
	}
	instant := float64(st.totalBytes-st.lastBytes) / elapsed
	st.lastBytes = st.totalBytes
	st.lastTime = time.Now()
	return instant
}

func (st *SpeedTracker) Total() uint64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.totalBytes
}

// PerStream tracks the byte count of each stream over time so we can derive
// a per-stream speed.
type PerStream struct {
	mu         sync.Mutex
	counts     []uint64
	rates      []float64
	lastCounts []uint64
	lastTime   time.Time
}

func NewPerStream(n int) *PerStream {
	return &PerStream{
		counts:     make([]uint64, n),
		rates:      make([]float64, n),
		lastCounts: make([]uint64, n),
		lastTime:   time.Now(),
	}
}

// AddStreamBytes records that stream i transferred n more bytes.
func (ps *PerStream) AddStreamBytes(i int, n uint64) {
	ps.mu.Lock()
	ps.counts[i] += n
	ps.mu.Unlock()
}

// Speeds computes the per-stream transfer speed (bytes/sec) since last call.
func (ps *PerStream) Speeds() []float64 {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(ps.lastTime).Seconds()
	if elapsed < 0.05 {
		return append([]float64(nil), ps.rates...)
	}
	for i := range ps.counts {
		ps.rates[i] = float64(ps.counts[i]-ps.lastCounts[i]) / elapsed
		ps.lastCounts[i] = ps.counts[i]
	}
	ps.lastTime = now
	return append([]float64(nil), ps.rates...)
}

type ProgressDisplay struct {
	mu       sync.Mutex
	mode     string
	streams  int
	tracker  *SpeedTracker
	perStr   *PerStream
	enabled  bool
	lastDraw time.Time
	drawn    bool
	boxShown bool // a box has been fully rendered to stderr
	done     bool
	out      io.Writer
}

func NewProgressDisplay(mode string, streams, chunkSz int, enabled bool) *ProgressDisplay {
	return &ProgressDisplay{
		mode:    mode,
		streams: streams,
		tracker: NewSpeedTracker(),
		perStr:  NewPerStream(streams),
		enabled: enabled,
		out:     os.Stderr,
	}
}

func (pd *ProgressDisplay) AddBytes(n uint64)              { pd.tracker.Add(n) }
func (pd *ProgressDisplay) AddStreamBytes(i int, n uint64) { pd.perStr.AddStreamBytes(i, n) }

// Tick redraws the progress report, at most once per second.
func (pd *ProgressDisplay) Tick(totalBytes uint64, extra string) {
	if !pd.enabled {
		return
	}
	pd.mu.Lock()
	defer pd.mu.Unlock()
	if pd.done {
		return
	}
	now := time.Now()
	if pd.drawn && now.Sub(pd.lastDraw) < refreshInterval {
		return
	}
	pd.lastDraw = now
	pd.drawn = true

	speed := pd.tracker.Speed()
	elapsed := time.Since(pd.tracker.start).Seconds()
	rates := pd.perStr.Speeds()

	if !stderrIsTTY() {
		pd.plainLine(totalBytes, speed, elapsed, rates, extra)
		return
	}
	cols, rows := terminalSize()
	pd.renderBox(pd.buildLines(totalBytes, speed, elapsed, rates, extra, false), cols, rows)
}

func (pd *ProgressDisplay) Final(totalBytes uint64, chunks uint64) {
	if !pd.enabled {
		return
	}
	pd.mu.Lock()
	defer pd.mu.Unlock()
	pd.done = true

	elapsed := time.Since(pd.tracker.start).Seconds()
	avg := float64(0)
	if elapsed > 0 {
		avg = float64(totalBytes) / elapsed
	}
	rates := pd.perStr.Speeds()
	extra := fmt.Sprintf("chunks=%d media=%.3fs", chunks, elapsed)

	if !stderrIsTTY() {
		pd.plainDone(totalBytes, chunks, avg, elapsed)
		return
	}
	cols, rows := terminalSize()
	pd.renderBox(pd.buildLines(totalBytes, avg, elapsed, rates, extra, true), cols, rows)
	fmt.Fprintln(pd.out)
}

// buildLines constructs the box content: a header with total size/speed, one
// row per connection with its own speed, and a footer with extra info.
func (pd *ProgressDisplay) buildLines(total uint64, speed float64, elapsed float64, rates []float64, extra string, done bool) []string {
	modeLabel := "SEND"
	if pd.mode == "receiver" {
		modeLabel = "RECV"
	}
	if done {
		modeLabel = "DONE"
	}

	lines := []string{fmt.Sprintf("%s  %s  @ %s  %6.1fs",
		modeLabel, formatBytes(total), formatSpeed(speed), elapsed)}
	for i, r := range rates {
		lines = append(lines, fmt.Sprintf(" cons %2d  %s", i+1, formatSpeed(r)))
	}
	if len(extra) > 0 {
		lines = append(lines, extra)
	} else {
		lines = append(lines, " ")
	}
	return lines
}

// plainLine is the fallback used when stderr is not a terminal: one plain
// line per second, newline terminated (safe for logs/pipes).
func (pd *ProgressDisplay) plainLine(total uint64, speed float64, elapsed float64, rates []float64, extra string) {
	modeLabel := "SEND"
	if pd.mode == "receiver" {
		modeLabel = "RECV"
	}
	var streamsStr strings.Builder
	for _, r := range rates {
		fmt.Fprintf(&streamsStr, " %s%s%s", Blue, formatStreamSpeed(r), Reset)
	}
	if extra != "" {
		extra = " " + extra
	}
	fmt.Fprintf(pd.out, "%s%s %s %s %s %s%.1fs%s%s%s\n",
		BoldWhite, modeLabel, formatBytes(total), formatSpeed(speed),
		streamsStr.String(), Yellow, elapsed, Reset, extra, Reset)
}

func (pd *ProgressDisplay) plainDone(total uint64, chunks uint64, avg float64, elapsed float64) {
	color := BoldCyan
	if pd.mode == "receiver" {
		color = BoldGreen
	}
	fmt.Fprintf(pd.out,
		"\n%s%s %s total=%s chunks=%d elapsed=%.1fs avg=%s%s\n",
		color, Bold, "DONE:", formatBytes(total), chunks, elapsed, formatSpeed(avg), Reset)
}

// renderBox draws a centered box on a terminal. Each refresh rewrites the
// same rows (moving the cursor back to the box top) instead of appending new
// lines, so the terminal scroll buffer is never exhausted by the report.
func (pd *ProgressDisplay) renderBox(lines []string, cols, rows int) {
	if cols <= 0 {
		cols = 80
	}
	inner := 0
	for _, l := range lines {
		if w := utf8.RuneCountInString(l); w > inner {
			inner = w
		}
	}
	boxW := inner + 2
	if boxW > cols {
		boxW = cols
	}
	inner = boxW - 2
	pad := 0
	if cols > boxW {
		pad = (cols - boxW) / 2
	}

	top := "┌" + strings.Repeat("─", inner) + "┐"
	bottom := "└" + strings.Repeat("─", inner) + "┘"
	rowsBox := make([]string, 0, len(lines)+2)
	rowsBox = append(rowsBox, top)
	for _, l := range lines {
		rowsBox = append(rowsBox, "│"+padCell(l, inner)+"│")
	}
	rowsBox = append(rowsBox, bottom)

	if rows > 0 && len(rowsBox) > rows {
		// Terminal too small for a box: degrade to plain lines.
		for _, l := range rowsBox {
			fmt.Fprintln(pd.out, l)
		}
		return
	}

	height := len(rowsBox)
	col := pad + 1
	if !pd.boxShown {
		// First render: print rows from the current cursor position.
		for i, r := range rowsBox {
			if i > 0 {
				fmt.Fprint(pd.out, "\n")
			}
			fmt.Fprintf(pd.out, "\r\033[%dG%s\033[K", col, r)
		}
		pd.boxShown = true
		// Park the cursor at the top-left of the box for future refreshes.
		fmt.Fprintf(pd.out, "\r\033[%dA\033[%dG", height-1, col)
		return
	}

	// In-place refresh: walk back up to the box top, rewrite each row,
	// then return the cursor to the top again. No output is appended, so
	// the terminal buffer does not grow.
	fmt.Fprintf(pd.out, "\r\033[%dA", height-1)
	for i, r := range rowsBox {
		if i > 0 {
			fmt.Fprint(pd.out, "\033[1B")
		}
		fmt.Fprintf(pd.out, "\r\033[%dG%s\033[K", col, r)
	}
	fmt.Fprintf(pd.out, "\r\033[%dA\033[%dG", height-1, col)
}

// padCell right-pads (or truncates) s to exactly w visible runes, ignoring
// any ANSI colour escape sequences embedded in s.
func padCell(s string, w int) string {
	visible := visibleRunes(s)
	if len(visible) > w {
		visible = visible[:w]
	}
	return string(visible) + strings.Repeat(" ", w-len(visible))
}

// visibleRunes returns the printable runes of s, skipping ANSI escape
// sequences like "\033[3;1m".
func visibleRunes(s string) []rune {
	var out []rune
	for i := 0; i < len(s); {
		if s[i] == 0x1b && i+1 < len(s) && s[i+1] == '[' {
			for i < len(s) && s[i] != 'm' {
				i++
			}
			if i < len(s) {
				i++
			}
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		out = append(out, r)
		i += size
	}
	return out
}

func formatStreamSpeed(bytesPerSec float64) string {
	if bytesPerSec < 1024*1024 {
		return fmt.Sprintf("%.1fK", bytesPerSec/1024)
	}
	return fmt.Sprintf("%.1fM", bytesPerSec/(1024*1024))
}
