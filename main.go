package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

var version = "1.1.0"

func main() {
	listenAddr := flag.String("listen", "", "Listen address (server role), e.g. :9000")
	connectAddr := flag.String("connect", "", "Connect address (client role), e.g. 192.168.0.1:9000")

	singlePort := flag.Bool("single-port", true,
		"Use a single port for all N streams (default true). With --single-port=false the listen/connect port is treated as a base and each stream uses port+i.")
	numStreams := flag.Int("streams", 4, "Number of parallel TCP streams")
	chunkSize := flag.Int("chunk-size", 64*1024, "Chunk size in bytes, sender only (default 64 KiB)")
	resendTimeout := flag.Duration("resend-timeout", 3*time.Second, "Timeout before requesting retransmission of a missing chunk")
	connTimeout := flag.Duration("conn-timeout", 30*time.Second,
		"Idle timeout per connection; a stream with no traffic for this long is deemed dead and re-established (0 to disable)")
	progressEnabled := flag.Bool("progress", true, "Show live progress box on stderr (set false to disable)")
	queueMode := flag.String("queue-mode", "round-robin",
		"Chunk queueing mode: round-robin (default) or least-unacked (send the next chunk to the stream with the fewest unacked chunks)")
	showVersion := flag.Bool("version", false, "Show version and exit")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `%s - Parallel bidirectional TCP stream multiplexer

%s
   Reads data from stdin, splits into numbered chunks, and distributes them
   across N parallel TCP streams (round-robin, or least-unacked with
   --queue-mode=least-unacked). The peer reassembles chunks in order and
   writes them to stdout. Data may flow in both directions at the same time.

  Each side runs both a sender and a receiver half, so no --sender or
  --receiver flag is needed: one side listens and the other connects, and
  the data direction is determined solely by which side's stdin is piped.

  Wire format per message:
    [1-byte type][8-byte seq (big-endian)][4-byte datalen (big-endian)][data...]

  Message types: DATA(1), ACK(2), RESEND(3), DONE(4), HELLO(5), STREAMID(6)

%s
  Machine A (sender, listening):
    zfs send pool/dataset | multisend --listen :9000

  Machine B (receiver, connecting):
    multisend --connect machineA:9000 | zfs recv pool/dataset

  Both machines may transmit simultaneously (true bidirectional).
  When a side's stdin is a terminal it immediately signals DONE so the
  other side knows that direction is finished.

  Local test (two separate pipelines to avoid a stdin/stdout feedback loop):
    (dd if=/dev/urandom bs=1M count=100 | multisend --listen :9000 > /dev/null) &
    multisend --connect 127.0.0.1:9000 < /dev/null > /tmp/out.bin && md5sum /tmp/out.bin

%s
  --listen ADDR       Listen address (server role); choose one of --listen/--connect
  --connect ADDR      Connect address (client role)
  --single-port       Use one port for all N streams (default: true).
                      With --single-port=false each stream uses base_port + stream.
  --streams N         Number of parallel TCP streams (default: 4)
  --chunk-size N      Chunk size in bytes (default: 65536 = 64 KiB)
  --resend-timeout D  Timeout before requesting resend (default: 3s)
   --conn-timeout D    Idle timeout per connection; 0 to disable (default: 30s)
   --queue-mode M      Chunk queueing mode: round-robin or least-unacked
   --progress          Show live progress box on stderr (default: true)
   --version           Show version
`, Bold+"multisend"+Reset,
			Bold+"DESCRIPTION"+Reset,
			Bold+"EXAMPLES"+Reset,
			Bold+"OPTIONS"+Reset)
	}

	flag.Parse()

	if *showVersion {
		fmt.Fprintf(os.Stderr, "multisend %s\n", version)
		os.Exit(0)
	}

	if *listenAddr != "" && *connectAddr != "" {
		fmt.Fprintf(os.Stderr, "%s%sError: specify only one of --listen or --connect%s\n", Red, Bold, Reset)
		flag.Usage()
		os.Exit(1)
	}
	if *listenAddr == "" && *connectAddr == "" {
		fmt.Fprintf(os.Stderr, "%s%sError: specify --listen or --connect%s\n", Red, Bold, Reset)
		flag.Usage()
		os.Exit(1)
	}
	if *queueMode != "round-robin" && *queueMode != "least-unacked" {
		fmt.Fprintf(os.Stderr, "%s%sError: invalid --queue-mode %q (want round-robin or least-unacked)%s\n",
			Red, Bold, *queueMode, Reset)
		os.Exit(1)
	}

	if *listenAddr != "" {
		host, port, err := parseAddr(*listenAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s%sError: invalid listen address: %v%s\n", Red, Bold, err, Reset)
			os.Exit(1)
		}
		n := NewNode(host, port, true, *singlePort, *numStreams, *chunkSize, *connTimeout, *resendTimeout, *progressEnabled, *queueMode)
		if err := n.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "%s%sError: %v%s\n", Red, Bold, err, Reset)
			os.Exit(1)
		}
	} else {
		host, port, err := parseAddr(*connectAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s%sError: invalid connect address: %v%s\n", Red, Bold, err, Reset)
			os.Exit(1)
		}
		n := NewNode(host, port, false, *singlePort, *numStreams, *chunkSize, *connTimeout, *resendTimeout, *progressEnabled, *queueMode)
		if err := n.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "%s%sError: %v%s\n", Red, Bold, err, Reset)
			os.Exit(1)
		}
	}
}