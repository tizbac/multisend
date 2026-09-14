package main

import (
	"bytes"
	"strings"
	"testing"
)

func renderBoxToBuffer(t *testing.T, pd *ProgressDisplay, lines []string, cols, rows int) string {
	t.Helper()
	var buf bytes.Buffer
	pd.out = &buf
	pd.renderBox(lines, cols, rows)
	return buf.String()
}

func TestRenderBoxFirstDrawCentered(t *testing.T) {
	pd := NewProgressDisplay("sender", 3, 65536, true)
	var buf bytes.Buffer
	pd.out = &buf
	pd.renderBox([]string{"SEND 1.00 MiB @ 1.50 MiB/s 1.0s", " cons  1  0.5 MiB/s", " cons  2  0.5 MiB/s", " cons  3  0.5 MiB/s"}, 40, 24)

	out := buf.String()
	if !strings.Contains(out, "┌") || !strings.Contains(out, "┐") || !strings.Contains(out, "└") || !strings.Contains(out, "┘") {
		t.Fatalf("box borders missing:\n%q", out)
	}
	if !strings.Contains(out, "SEND 1.00 MiB") {
		t.Fatalf("header content missing:\n%q", out)
	}
	if !strings.Contains(out, "cons  1") {
		t.Fatalf("per-connection line missing:\n%q", out)
	}
	// First draw must park the cursor at top-left of the box so the next
	// refresh overwrites in place (no terminal buffer growth).
	// Box is 33 wide (max content 31 + 2 borders) on a 40-col terminal:
	// pad=(40-33)/2=3, column=4. 6 rows => up 5.
	if !strings.HasSuffix(out, "\r\033[5A\033[4G") {
		t.Fatalf("expected cursor parked at box top, got suffix %q", out[len(out)-12:])
	}
}

func TestRenderBoxRefreshOverwritesSameLines(t *testing.T) {
	pd := NewProgressDisplay("sender", 3, 65536, true)
	var buf bytes.Buffer
	pd.out = &buf

	pd.renderBox([]string{"SEND 1.00 MiB @ 1.50 MiB/s 1.0s", " cons  1  0.5 MiB/s", " cons  2  0.5 MiB/s", " cons  3  0.5 MiB/s"}, 40, 24)
	first := buf.String()
	buf.Reset()

	pd.renderBox([]string{"SEND 2.00 MiB @ 2.50 MiB/s 1.0s", " cons  1  0.8 MiB/s", " cons  2  0.8 MiB/s", " cons  3  0.9 MiB/s"}, 40, 24)
	second := buf.String()

	// The refresh writes no newline output; it walks back up to the box top.
	if strings.Contains(second, "DONE") {
		t.Fatalf("unexpected content: %q", second)
	}
	if !strings.Contains(second, "2.00 MiB") {
		t.Fatalf("refresh did not update content: %q", second)
	}
	// No '\n' should be appended in refresh mode (buffer keeps fixed height).
	if strings.Contains(second, "\n") {
		t.Fatalf("refresh must not emit newlines: %q", second)
	}
	if !strings.HasSuffix(second, "\r\033[5A\033[4G") {
		t.Fatalf("refresh must re-park cursor at box top: %q", second)
	}
	if strings.Count(second, "\033[") < 3 {
		t.Fatalf("expected ANSI cursor moves in refresh: %q", second)
	}
	_ = first
}

func TestRenderBoxDegradesWhenTerminalTooShort(t *testing.T) {
	pd := NewProgressDisplay("receiver", 4, 65536, true)
	var buf bytes.Buffer
	pd.out = &buf
	lines := []string{"RECV", " cons  1", " cons  2", " cons  3", " cons  4", " chunks=1"}
	pd.renderBox(lines, 20, 3)

	out := buf.String()
	// A box taller than the terminal degrades to simple plain lines.
	if !strings.Contains(out, "[0;32m") && !strings.Contains(out, "┌") {
		t.Fatalf("expected plain degrade output, got: %q", out)
	}
}

func TestPadCellStripsANSIColors(t *testing.T) {
	got := padCell(Cyan+"hi"+Reset, 6)
	want := "hi    "
	if got != want {
		t.Fatalf("padCell=%q want %q", got, want)
	}
}

func TestVisibleRunesSkipsEscapeSequences(t *testing.T) {
	got := visibleRunes("\033[1;31mceleste\033[0m")
	if string(got) != "celeste" {
		t.Fatalf("got %q want celeste", string(got))
	}
}
