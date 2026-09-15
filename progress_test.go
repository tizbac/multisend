package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func renderBoxToBuffer(t *testing.T, pd *ProgressDisplay, lines []string, cols, rows int) string {
	t.Helper()
	var buf bytes.Buffer
	pd.out = &buf
	pd.renderBox(lines, cols, rows)
	return buf.String()
}

func TestRenderBoxFirstDrawCentered(t *testing.T) {
	pd := NewProgressDisplay(3, true, 30*time.Second)
	var buf bytes.Buffer
	pd.out = &buf
	pd.renderBox([]string{"SEND 1.00 MiB @ 1.50 MiB/s 1.0s", " conn  1  S 0.5K  R 0.5K", " conn  2  S 0.5K  R 0.5K", " conn  3  S 0.5K  R 0.5K"}, 40, 24)

	out := buf.String()
	if !strings.Contains(out, "\u250c") || !strings.Contains(out, "\u2510") ||
		!strings.Contains(out, "\u2514") || !strings.Contains(out, "\u2518") {
		t.Fatalf("box borders missing:\n%q", out)
	}
	// White background + black text inside the box.
	if !strings.Contains(out, "\033[47;30m") {
		t.Fatalf("expected white box background:\n%q", out)
	}
	// Gray cast shadow (bright-black background cells).
	if !strings.Contains(out, "\033[100m") {
		t.Fatalf("expected gray shadow:\n%q", out)
	}
	if !strings.Contains(out, "SEND 1.00 MiB") {
		t.Fatalf("header content missing:\n%q", out)
	}
	if !strings.Contains(out, "conn  1") {
		t.Fatalf("per-connection line missing:\n%q", out)
	}
	// First draw must park the cursor at box top so the next refresh
	// overwrites in place. 6 box rows + 1 shadow row = 7; park moves up 6.
	if !strings.HasSuffix(out, "\r\033[6A\033[4G") {
		t.Fatalf("expected cursor parked at box top, got suffix %q", out[len(out)-16:])
	}
}

func TestRenderBoxRefreshOverwritesSameLines(t *testing.T) {
	pd := NewProgressDisplay(3, true, 30*time.Second)
	var buf bytes.Buffer
	pd.out = &buf

	pd.renderBox([]string{"SEND 1.00 MiB @ 1.50 MiB/s 1.0s", " conn  1  S 0.5K  R 0.5K", " conn  2  S 0.5K  R 0.5K", " conn  3  S 0.5K  R 0.5K"}, 40, 24)
	buf.Reset()

	pd.renderBox([]string{"SEND 2.00 MiB @ 2.50 MiB/s 1.0s", " conn  1  S 0.8K  R 0.8K", " conn  2  S 0.8K  R 0.8K", " conn  3  S 0.9K  R 0.9K"}, 40, 24)
	second := buf.String()

	if !strings.Contains(second, "2.00 MiB") {
		t.Fatalf("refresh did not update content: %q", second)
	}
	// Refresh must not emit newlines (buffer keeps fixed height).
	if strings.Contains(second, "\n") {
		t.Fatalf("refresh must not emit newlines: %q", second)
	}
	if !strings.HasSuffix(second, "\r\033[6A\033[4G") {
		t.Fatalf("refresh must re-park cursor at box top: %q", second)
	}
	if strings.Count(second, "\033[") < 8 {
		t.Fatalf("expected ANSI cursor/colour moves in refresh: %q", second)
	}
}

func TestRenderBoxDegradesWhenTerminalTooShort(t *testing.T) {
	pd := NewProgressDisplay(4, true, 30*time.Second)
	var buf bytes.Buffer
	pd.out = &buf
	lines := []string{"RUN", " conn  1", " conn  2", " conn  3", " conn  4", " pend", " buff", " miss"}
	pd.renderBox(lines, 40, 3)

	out := buf.String()
	// A box taller than the terminal degrades to simple plain lines.
	if !strings.Contains(out, "\u250c") {
		t.Fatalf("expected plain degrade output (no box borders), got: %q", out)
	}
	if pd.boxShown {
		t.Fatalf("box must not be marked shown after degrade")
	}
}

func TestBoxRowPadsInBoxColour(t *testing.T) {
	pd := NewProgressDisplay(2, true, 30*time.Second)
	row := pd.boxRow(Red+"hi"+Reset, 8)
	// Padding must be issued in box colour (after the content's escapes are
	// stripped) so no white gaps appear before the right border.
	if strings.Count(row, "\033[47;30m") < 2 {
		t.Fatalf("expected padding in box colour, got: %q", row)
	}
	if len(visibleRunes(row)) != 8+2+1 { // bordered content(8) + 2 borders + shadow cell
		t.Fatalf("unexpected visible width: %q", row)
	}
}

func TestVisibleRunesSkipsEscapeSequences(t *testing.T) {
	got := visibleRunes("\033[1;31mceleste\033[0m")
	if string(got) != "celeste" {
		t.Fatalf("got %q want celeste", string(got))
	}
}

func TestStripANSIRemovesEscapes(t *testing.T) {
	got := stripANSI(Red + "foo" + Blue + "bar" + Reset)
	if got != "foobar" {
		t.Fatalf("got %q want foobar", got)
	}
}

func TestLogStoresPlainEvent(t *testing.T) {
	pd := NewProgressDisplay(2, true, 30*time.Second)
	pd.out = &bytes.Buffer{}
	pd.Log("%s%s stream 1: connection lost%s", Red, Bold, Reset)
	if len(pd.events) != 1 {
		t.Fatalf("expected 1 event, got %d: %q", len(pd.events), pd.events)
	}
	if strings.Contains(pd.events[0], "\033") {
		t.Fatalf("event must be stored plain (no ANSI), got %q", pd.events[0])
	}
	if pd.events[0] != " stream 1: connection lost" {
		t.Fatalf("unexpected event text %q", pd.events[0])
	}
}

func TestWeightBarShare(t *testing.T) {
	bar, pct := weightBar(2, 3, 10)
	if pct != 66 {
		t.Fatalf("want 66%%, got %d", pct)
	}
	if len(visibleRunes(bar)) != 10 {
		t.Fatalf("bar must be 10 cells wide, got %q", bar)
	}
	if strings.Count(bar, "\u2588") != 6 { // 66% of 10 fills 6 cells
		t.Fatalf("unexpected fill in %q", bar)
	}
	if b, p := weightBar(0, 0, 10); p != 0 || strings.Count(b, "\u2591") != 10 {
		t.Fatalf("empty total must give 0%% and an empty bar, got %q/%d", b, p)
	}
}

func TestConnLineWeightBar(t *testing.T) {
	pd := NewProgressDisplay(2, true, 30*time.Second)
	pd.AddSentChunk(0)
	pd.AddSentChunk(0)
	pd.AddSentChunk(1)
	lines := pd.buildLines(100, 100, 0, 0, 1, []float64{0, 0}, []float64{0, 0}, false)
	joined := strings.Join(lines, "\n")
	for _, want := range []string{"66%", "33%", "conn  1", "conn  2", "STL"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("conn line missing %q in:\n%s", want, joined)
		}
	}
}

func TestBoxGrowsOnly(t *testing.T) {
	pd := NewProgressDisplay(2, true, 30*time.Second)
	var buf bytes.Buffer
	pd.out = &buf
	short := []string{"a", "b"}
	long := []string{strings.Repeat("a", 25), "b"}

	pd.renderBox(short, 80, 24)
	first := buf.String()
	buf.Reset()
	pd.renderBox(long, 80, 24)
	second := buf.String()
	buf.Reset()
	pd.renderBox(short, 80, 24)
	third := buf.String()

	border := func(s string) int { return strings.Count(s, "\u2500") }
	if border(second) < border(first) {
		t.Fatalf("box did not grow: %d -> %d", border(first), border(second))
	}
	if border(third) != border(second) {
		t.Fatalf("box shrank back: %d -> %d", border(second), border(third))
	}
}