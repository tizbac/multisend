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
	BoldYellow = "\033[1;33m"
	BoldCyan  = "\033[1;36m"
	BoldWhite = "\033[1;37m"

	// Box colours: white background, black text inside the box.
	boxFg    = "\033[47;30m"
	shadowBg = "\033[100m" // bright-black background for cast shadow
)

const refreshInterval = time.Second

// ─────────────────────────── Speed tracker ────────────────────────────────

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

// ──────────────────────────── Per-stream rates ────────────────────────────

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

func (ps *PerStream) Add(i int, n uint64) {
	ps.mu.Lock()
	ps.counts[i] += n
	ps.mu.Unlock()
}

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

// ──────────────────────── ProgressDisplay ─────────────────────────────────

type ProgressDisplay struct {
	mu      sync.Mutex
	streams int
	enabled bool

	// send half
	sTrk *SpeedTracker
	sPer *PerStream
	// recv half
	rTrk *SpeedTracker
	rPer *PerStream

	connTmo time.Duration

	retransmits []int32
	pending     int
	buffered    int
	missing     int
	pendingWm   int
	bufferedWm  int
	missingWm   int
	streamDown  []bool
	streamLast  []time.Time // last time we saw traffic per stream
	sentChunks  []uint64    // chunks sent over each stream (weight bar)

	events   []string // ring of last few Log lines (plain text for box)
	lastDraw time.Time
	drawn    bool
	boxShown bool

	// Last box geometry for Final to move the cursor below it.
	boxCol       int
	boxTotalRows int
	boxInner     int // inner width (visible runes); the box only grows, never shrinks

	done bool
	out  io.Writer
}

func NewProgressDisplay(streams int, enabled bool, connTmo time.Duration) *ProgressDisplay {
	return &ProgressDisplay{
		streams:     streams,
		sTrk:        NewSpeedTracker(),
		sPer:        NewPerStream(streams),
		rTrk:        NewSpeedTracker(),
		rPer:        NewPerStream(streams),
		connTmo:     connTmo,
		streamLast:  make([]time.Time, streams),
		retransmits: make([]int32, streams),
		streamDown:  make([]bool, streams),
		sentChunks:  make([]uint64, streams),
		enabled:     enabled,
		out:         os.Stderr,
	}
}

// ── out-flow helpers ──

func (pd *ProgressDisplay) AddBytes(n uint64)               { pd.sTrk.Add(n) }
func (pd *ProgressDisplay) AddStreamBytes(i int, n uint64)   { pd.sPer.Add(i, n) }

// ── in-flow helpers ──

func (pd *ProgressDisplay) AddRecvBytes(n uint64)            { pd.rTrk.Add(n) }
func (pd *ProgressDisplay) AddRecvStreamBytes(i int, n uint64) { pd.rPer.Add(i, n) }

func (pd *ProgressDisplay) RecordRetransmit(i int) {
	pd.mu.Lock()
	pd.retransmits[i]++
	pd.mu.Unlock()
}

// AddSentChunk records that one chunk left over stream i. Feeds the per-stream
// weight bar shown next to each conn line.
func (pd *ProgressDisplay) AddSentChunk(i int) {
	pd.mu.Lock()
	pd.sentChunks[i]++
	pd.mu.Unlock()
}

func (pd *ProgressDisplay) SetPending(n int) {
	pd.mu.Lock()
	pd.pending = n
	pd.mu.Unlock()
}

func (pd *ProgressDisplay) SetBuffer(buffered, missing int) {
	pd.mu.Lock()
	pd.buffered = buffered
	pd.missing = missing
	pd.mu.Unlock()
}

func (pd *ProgressDisplay) SetStreamDown(i int, down bool) {
	pd.mu.Lock()
	pd.streamDown[i] = down
	pd.mu.Unlock()
}

// Log records a one-off event. On a TTY it is kept in an internal ring
// buffer that the progress box displays on its next refresh so it never
// corrupts the box. On a non-TTY it is emitted straight away as a plain
// line, which is safe because there is no box to corrupt.
func (pd *ProgressDisplay) Log(format string, args ...any) {
	if !pd.enabled {
		return
	}
	msg := fmt.Sprintf(format, args...)
	pd.mu.Lock()
	defer pd.mu.Unlock()
	pd.events = append(pd.events, stripANSI(msg))
	if len(pd.events) > 5 {
		pd.events = pd.events[len(pd.events)-5:]
	}
	if !stderrIsTTY() {
		fmt.Fprintln(pd.out, msg)
	}
}

// Tick redraws the progress report (throttled to ~1 Hz).
func (pd *ProgressDisplay) Tick(sendTotal, recvTotal uint64) {
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

	sSpeed := pd.sTrk.Speed()
	rSpeed := pd.rTrk.Speed()
	elapsed := time.Since(pd.sTrk.start).Seconds()
	sRates := pd.sPer.Speeds()
	rRates := pd.rPer.Speeds()

	// Update last activity per stream: if there's send or recv rate > 0,
	// mark this stream as having recent traffic.
	for i := range pd.streamLast {
		if i < len(sRates) && sRates[i] > 0 {
			pd.streamLast[i] = now
		}
		if i < len(rRates) && rRates[i] > 0 {
			pd.streamLast[i] = now
		}
	}

	if !stderrIsTTY() {
		pd.plainLine(sendTotal, recvTotal, sSpeed, rSpeed, elapsed, sRates, rRates)
		return
	}
	cols, rows := terminalSize()
	pd.renderBox(pd.buildLines(sendTotal, recvTotal, sSpeed, rSpeed, elapsed,
		sRates, rRates, false), cols, rows)
}

func (pd *ProgressDisplay) Final(sendTotal, sendChunks, recvTotal, recvChunks uint64) {
	if !pd.enabled {
		return
	}
	pd.mu.Lock()
	defer pd.mu.Unlock()
	pd.done = true

	elapsed := time.Since(pd.sTrk.start).Seconds()
	sAvg := float64(0)
	rAvg := float64(0)
	if elapsed > 0 {
		sAvg = float64(sendTotal) / elapsed
		rAvg = float64(recvTotal) / elapsed
	}
	sRates := pd.sPer.Speeds()
	rRates := pd.rPer.Speeds()

	if !stderrIsTTY() {
		pd.plainDone(sendTotal, sendChunks, recvTotal, recvChunks, sAvg, rAvg, elapsed)
		return
	}
	cols, rows := terminalSize()
	pd.renderBox(pd.buildLines(sendTotal, recvTotal, sAvg, rAvg, elapsed,
		sRates, rRates, true), cols, rows)
	// Move cursor below the box and print a final summary line.
	if pd.boxTotalRows > 0 {
		fmt.Fprintf(pd.out, "\r\033[%dB\033[K", pd.boxTotalRows-1)
		fmt.Fprintln(pd.out)
		fmt.Fprintf(pd.out,
			"%s%s DONE: sent=%s chunks=%d recv=%s chunks=%d elapsed=%.1fs%s\n",
			BoldCyan, Bold, formatBytes(sendTotal), sendChunks,
			formatBytes(recvTotal), recvChunks, elapsed, Reset)
	}
}

// ──────────────────────── Build / draw ────────────────────────────────────

func (pd *ProgressDisplay) buildLines(sendTotal, recvTotal uint64,
	sendSpeed, recvSpeed, elapsed float64,
	sRates, rRates []float64, done bool) []string {

	state := "RUN"
	if done {
		state = "DONE"
	}
	lines := []string{
		fmt.Sprintf("%s  SEND %s @ %s    RECV %s @ %s    %5.1fs",
			state, formatBytes(sendTotal), formatSpeed(sendSpeed),
			formatBytes(recvTotal), formatSpeed(recvSpeed), elapsed),
	}

	totalSent := uint64(0)
	for i := 0; i < pd.streams; i++ {
		totalSent += pd.sentChunks[i]
	}
	for i := 0; i < pd.streams; i++ {
		stColor, stText := BoldGreen, "OK"
		if pd.streamDown[i] {
			stColor, stText = BoldRed, "DISCONN"
		} else if pd.connTmo > 0 && time.Since(pd.streamLast[i]) > pd.connTmo/5 {
			stColor, stText = BoldYellow, "STL"
		}
		bar, pct := weightBar(pd.sentChunks[i], totalSent, 10)
		extra := ""
		if pd.retransmits[i] > 0 {
			extra = fmt.Sprintf(" %srt=%d%s", Magenta, pd.retransmits[i], Reset)
		}
		lines = append(lines, fmt.Sprintf(" conn %2d  %s%-7s%s  %s%s %3d%%  S %s  R %s%s",
			i+1, stColor, stText, Reset, boxFg, bar, pct,
			formatStreamSpeed(sRates[i]), formatStreamSpeed(rRates[i]), extra))
	}

	lines = append(lines, pd.barLineColor(" pend", &pd.pending, &pd.pendingWm, Magenta))
	lines = append(lines, pd.barLineColor(" buff", &pd.buffered, &pd.bufferedWm, Green))
	lines = append(lines, pd.barLineColor(" miss", &pd.missing, &pd.missingWm, Red))

	for _, e := range pd.events {
		lines = append(lines, e)
	}
	return lines
}

func (pd *ProgressDisplay) barLineColor(label string, cur *int, wm *int, color string) string {
	val := *cur
	if val > *wm {
		*wm = val
	}
	const width = 14
	fill := 0
	if *wm > 0 {
		fill = int(float64(val) / float64(*wm) * float64(width))
	}
	if fill > width {
		fill = width
	}
	bar := strings.Repeat("\u2588", fill) + strings.Repeat("\u2591", width-fill)
	return fmt.Sprintf("%s %s%s%s %d", label, color, bar, Reset, val)
}

// weightBar renders a stream's share of the total chunks sent as a fixed-width
// bar plus that share in percent.
func weightBar(val, total uint64, width int) (string, int) {
	pct := 0
	if total > 0 {
		pct = int(float64(val) * 100 / float64(total))
	}
	fill := pct * width / 100
	if fill > width {
		fill = width
	}
	return strings.Repeat("\u2588", fill) + strings.Repeat("\u2591", width-fill), pct
}

// ────────────────────────── Box renderer ──────────────────────────────────

func (pd *ProgressDisplay) renderBox(lines []string, cols, rows int) {
	if cols <= 0 {
		cols = 80
	}

	// Compute content width (visible runes only; ANSI escapes don't count).
	inner := 0
	for _, l := range lines {
		if w := len(visibleRunes(l)); w > inner {
			inner = w
		}
	}
	if inner < pd.boxInner {
		inner = pd.boxInner // grow-only: the box never shrinks
	}
	boxW := inner + 2 // borders
	if boxW > cols {
		boxW = cols
		inner = boxW - 2
	}
	if inner < 1 {
		inner = 1
		boxW = inner + 2
	}
	if inner > pd.boxInner {
		pd.boxInner = inner
	}
	pad := 0
	if cols > boxW {
		pad = (cols - boxW) / 2
	}
	col := pad + 1

	// Build the box rows: top border, data rows, bottom border.
	top := boxFg + "\u250c" + strings.Repeat("\u2500", inner) + "\u2510" + Reset + shadowBg + " " + Reset
	bottom := boxFg + "\u2514" + strings.Repeat("\u2500", inner) + "\u2518" + Reset + shadowBg + " " + Reset

	boxRows := []string{top}
	for _, l := range lines {
		boxRows = append(boxRows, pd.boxRow(l, inner))
	}
	boxRows = append(boxRows, bottom)

	// totalRows includes the shadow row below the box.
	totalRows := len(boxRows) + 1
	if rows > 0 && totalRows > rows {
		for _, l := range boxRows {
			fmt.Fprintln(pd.out, stripANSI(l))
		}
		return
	}

	if !pd.boxShown {
		// First render – print from the current cursor position.
		for i, r := range boxRows {
			if i > 0 {
				fmt.Fprint(pd.out, "\n")
			}
			fmt.Fprintf(pd.out, "\r\033[%dG%s\033[K", col, r)
		}
		// Shadow row
		fmt.Fprintf(pd.out, "\n\r\033[%dG%s%s\033[K", col+1, shadowBg,
			strings.Repeat(" ", boxW-1)+Reset)
		pd.boxShown = true
		// Park the cursor at the box top so the next refresh overwrites in place.
		fmt.Fprintf(pd.out, "\r\033[%dA\033[%dG", totalRows-1, col)
		pd.boxCol = col
		pd.boxTotalRows = totalRows
		return
	}

	// In-place refresh: cursor is already at the box top (from park).
	for i, r := range boxRows {
		if i > 0 {
			fmt.Fprint(pd.out, "\033[1B")
		}
		fmt.Fprintf(pd.out, "\r\033[%dG%s\033[K", col, r)
	}
	fmt.Fprint(pd.out, "\033[1B")
	fmt.Fprintf(pd.out, "\r\033[%dG%s%s\033[K", col+1, shadowBg,
		strings.Repeat(" ", boxW-1)+Reset)
	fmt.Fprintf(pd.out, "\r\033[%dA\033[%dG", totalRows-1, col)
}

// boxRow renders one data row inside the white box. Unlike a plain visible-
// rune dump, SGR colour sequences in the content are preserved (so the conn
// statuses, weight bar and pend/buff/miss colours actually show up inside the
// box); only cursor sequences and the like are dropped. Padding is forced to
// the box background colour so a Reset inside the content string cannot leave
// terminal-default gaps before the right border.
func (pd *ProgressDisplay) boxRow(s string, inner int) string {
	var b strings.Builder
	b.WriteString(boxFg)
	b.WriteString("\u2502") // left border
	width := 0
	for i := 0; i < len(s); {
		if s[i] == 0x1b && i+1 < len(s) && s[i+1] == '[' {
			j := strings.IndexByte(s[i:], 'm')
			if j < 0 {
				break
			}
			b.WriteString(s[i : i+j+1])
			i += j + 1
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if width >= inner {
			break
		}
		b.WriteRune(r)
		width += 1
		i += size
	}
	// Padding spaces in the box background colour.
	if pad := inner - width; pad > 0 {
		b.WriteString(Reset)
		b.WriteString(boxFg)
		b.WriteString(strings.Repeat(" ", pad))
	}
	b.WriteString("\u2502") // right border
	b.WriteString(Reset)
	b.WriteString(shadowBg + " " + Reset)
	return b.String()
}

// ────────────────────── Fallback (non-TTY) ────────────────────────────────

func (pd *ProgressDisplay) plainLine(sendTotal, recvTotal uint64,
	sendSpeed, recvSpeed, elapsed float64, sRates, rRates []float64) {

	var sStr, rStr strings.Builder
	for i, r := range sRates {
		fmt.Fprintf(&sStr, " %s%s%s x%d", Red, formatStreamSpeed(r), Reset, pd.sentChunks[i])
	}
	for _, r := range rRates {
		fmt.Fprintf(&rStr, " %s%s%s", Green, formatStreamSpeed(r), Reset)
	}
	fmt.Fprintf(pd.out,
		"%sSEND %s @ %s%s %sRECV %s @ %s%s %s%5.1fs%s\n",
		BoldWhite, formatBytes(sendTotal), formatSpeed(sendSpeed), sStr.String(),
		BoldWhite, formatBytes(recvTotal), formatSpeed(recvSpeed), rStr.String(),
		Yellow, elapsed, Reset)
}

func (pd *ProgressDisplay) plainDone(sendTotal, sendChunks, recvTotal, recvChunks uint64,
	sAvg, rAvg, elapsed float64) {
	fmt.Fprintf(pd.out,
		"\n%s%s DONE: sent=%s chunks=%d recv=%s chunks=%d elapsed=%.1fs avgS=%s avgR=%s%s\n",
		BoldCyan, Bold, formatBytes(sendTotal), sendChunks,
		formatBytes(recvTotal), recvChunks, elapsed,
		formatSpeed(sAvg), formatSpeed(rAvg), Reset)
}

// ──────────────────────────── Helpers ─────────────────────────────────────

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

func formatStreamSpeed(bytesPerSec float64) string {
	if bytesPerSec < 1024*1024 {
		return fmt.Sprintf("%.1fK", bytesPerSec/1024)
	}
	return fmt.Sprintf("%.1fM", bytesPerSec/(1024*1024))
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

// stripANSI removes escape sequences from a string so it can be displayed
// inside the box without leaking colour state.
func stripANSI(s string) string {
	var b strings.Builder
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
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}