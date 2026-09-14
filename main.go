package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

var version = "1.0.0"

func main() {
	senderMode := flag.Bool("sender", false, "Run in sender mode (reads stdin, distributes across TCP streams)")
	receiverMode := flag.Bool("receiver", false, "Run in receiver mode (reassembles from TCP streams, writes stdout)")

	listenAddr := flag.String("listen", "0.0.0.0:9000", "Address to listen on (sender mode). Port is base for N streams")
	connectAddr := flag.String("connect", "", "Address to connect to (receiver mode). Port is base for N streams")

	numStreams := flag.Int("streams", 4, "Number of parallel TCP streams")
	chunkSize := flag.Int("chunk-size", 64*1024, "Chunk size in bytes (default 64 KiB)")
	resendTimeout := flag.Duration("resend-timeout", 3*time.Second, "Timeout before requesting retransmission of a missing chunk")
	connTimeout := flag.Duration("conn-timeout", 30*time.Second, "Idle timeout per connection; if a stream sees no traffic this long it is deemed dead and re-established")
	progressEnabled := flag.Bool("progress", true, "Show live progress box on stderr (set false to disable)")
	showVersion := flag.Bool("version", false, "Show version and exit")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `%s - Parallel TCP stream multiplexer for piped data

%s
  Reads data from stdin, splits into numbered chunks, and distributes them
  across N parallel TCP streams in round-robin order. The receiver reassembles
  chunks in order and writes them to stdout. Designed for piping between
  commands like zfs send/recv.

  Wire format per message:
    [1-byte type][8-byte seq (big-endian)][4-byte datalen (big-endian)][data...]

  Message types: DATA(1), ACK(2), RESEND(3), DONE(4)

  Features:
    - Round-robin distribution across N TCP streams
    - 64-bit chunk sequence numbering
    - In-memory chunk buffer with ACK-based eviction
    - Automatic retransmission requests for missing chunks
    - Connection reestablishment on drops
    - Real-time speed display with ANSI colors on stderr

%s
  Machine A (sender):
    zfs send pool/dataset | multisend --sender --listen :9000 --streams 4

  Machine B (receiver):
    multisend --receiver --connect machineA:9000 --streams 4 | zfs recv pool/dataset

  Local test:
    dd if=/dev/urandom bs=1M count=100 | multisend --sender --listen :9000 | multisend --receiver --connect 127.0.0.1:9000 | md5sum

%s
  --sender            Run in sender mode
  --receiver          Run in receiver mode
  --listen ADDR       Listen address for sender (default: 0.0.0.0:9000)
  --connect ADDR      Connect address for receiver (required in receiver mode)
  --streams N         Number of parallel TCP streams (default: 4)
  --chunk-size N      Chunk size in bytes, sender only (default: 65536 = 64 KiB).
                      Advertised to the receiver on the wire; not used by --receiver.
  --resend-timeout D  Timeout before requesting resend (default: 3s)
  --conn-timeout D    Idle timeout per connection; a stream with no traffic for
                      this long is deemed dead and killed + re-established
                      (default: 30s, use 0 to disable)
  --progress          Show live progress box on stderr
                      (default: true; set to false to disable)
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

	if !*senderMode && !*receiverMode {
		fmt.Fprintf(os.Stderr, "%s%sError: must specify --sender or --receiver%s\n", Red, Bold, Reset)
		flag.Usage()
		os.Exit(1)
	}
	if *senderMode && *receiverMode {
		fmt.Fprintf(os.Stderr, "%s%sError: cannot be both --sender and --receiver%s\n", Red, Bold, Reset)
		os.Exit(1)
	}

	if *senderMode {
		_, port, err := parseAddr(*listenAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s%sError: invalid listen address: %v%s\n", Red, Bold, err, Reset)
			os.Exit(1)
		}
		s := NewSender(port, *numStreams, *chunkSize, *connTimeout, *resendTimeout, *progressEnabled)
		if err := s.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "%s%sError: %v%s\n", Red, Bold, err, Reset)
			os.Exit(1)
		}
	} else {
		if *connectAddr == "" {
			fmt.Fprintf(os.Stderr, "%s%sError: --connect is required in receiver mode%s\n", Red, Bold, Reset)
			os.Exit(1)
		}
		host, port, err := parseAddr(*connectAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s%sError: invalid connect address: %v%s\n", Red, Bold, err, Reset)
			os.Exit(1)
		}
		r := NewReceiver(host, port, *numStreams, *connTimeout, *resendTimeout, *progressEnabled)
		if err := r.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "%s%sError: %v%s\n", Red, Bold, err, Reset)
			os.Exit(1)
		}
	}
}
