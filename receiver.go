package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Receiver struct {
	senderHost string
	basePort   int
	numStreams int
	chunkSize  int64 // learned from sender's Hello, atomic
	connTmo    time.Duration
	resendTmo  time.Duration

	conns   []net.Conn
	writers []*MsgWriter
	prog    *ProgressDisplay

	receiveMu   sync.Mutex
	expectedSeq uint64
	maxSeen     uint64
	seen        map[uint64]bool
	missing     map[uint64]time.Time // seq -> when first detected missing
	buffer      map[uint64]*Chunk    // out-of-order chunks awaiting their turn
	totalRecv   uint64
	doneCount   int32
	allDone     chan struct{}

	// test hooks (set via MULTISEND_TEST_DROP_SEQS env, never set in production)
	dropSeqs map[uint64]bool
}

func NewReceiver(senderHost string, basePort, numStreams int, connTmo, resendTmo time.Duration, progress ...bool) *Receiver {
	enabled := true
	if len(progress) > 0 {
		enabled = progress[0]
	}
	r := &Receiver{
		senderHost: senderHost,
		basePort:   basePort,
		numStreams: numStreams,
		connTmo:    connTmo,
		resendTmo:  resendTmo,
		conns:      make([]net.Conn, numStreams),
		writers:    make([]*MsgWriter, numStreams),
		prog:       NewProgressDisplay("receiver", numStreams, 0, enabled),
		seen:       make(map[uint64]bool),
		missing:    make(map[uint64]time.Time),
		buffer:     make(map[uint64]*Chunk),
		allDone:    make(chan struct{}),
	}
	r.dropSeqs = parseDropSeqs()
	return r
}

// parseDropSeqs reads MULTISEND_TEST_DROP_SEQS (comma-separated seq numbers)
// used only for automated tests to exercise the resend path.
func parseDropSeqs() map[uint64]bool {
	env := os.Getenv("MULTISEND_TEST_DROP_SEQS")
	if env == "" {
		return nil
	}
	m := map[uint64]bool{}
	for _, part := range strings.Split(env, ",") {
		var n uint64
		cnt, _ := fmt.Sscanf(strings.TrimSpace(part), "%d", &n)
		if cnt == 1 {
			m[n] = true
		}
	}
	return m
}

func (r *Receiver) Run() error {
	for i := 0; i < r.numStreams; i++ {
		if err := r.connectStream(i); err != nil {
			return err
		}
	}

	for i := 0; i < r.numStreams; i++ {
		go r.recvLoop(i)
	}
	go r.resendLoop()
	go r.writeLoop()

	<-r.allDone

	r.receiveMu.Lock()
	r.drain()
	r.receiveMu.Unlock()

	chunkSize := atomic.LoadInt64(&r.chunkSize)
	if chunkSize < 1 {
		chunkSize = 1
	}
	r.prog.Final(atomic.LoadUint64(&r.totalRecv), atomic.LoadUint64(&r.totalRecv)/uint64(chunkSize))

	for i := 0; i < r.numStreams; i++ {
		if r.conns[i] != nil {
			r.conns[i].Close()
		}
	}
	return nil
}

func (r *Receiver) connectStream(i int) error {
	port := r.basePort + i
	addr := net.JoinHostPort(r.senderHost, fmt.Sprintf("%d", port))
	for attempt := 0; ; attempt++ {
		conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
		if err == nil {
			r.conns[i] = conn
			r.writers[i] = NewMsgWriter(conn)
			fmt.Fprintf(os.Stderr, "%s%s stream %d connected to %s%s\n",
				Green, Bold, i+1, addr, Reset)
			return nil
		}
		if attempt >= 5 {
			return fmt.Errorf("stream %d: failed to connect after %d attempts: %w", i+1, attempt+1, err)
		}
		fmt.Fprintf(os.Stderr, "%sstream %d: connect failed, retrying (%d/5)...%s\n",
			Yellow, i+1, attempt+1, Reset)
		time.Sleep(time.Duration(attempt+1) * time.Second)
	}
}

func (r *Receiver) reconnectStream(i int) error {
	if r.conns[i] != nil {
		r.conns[i].Close()
	}
	return r.connectStream(i)
}

func (r *Receiver) handleHello(streamIdx int, msg *Message) {
	if len(msg.Data) != HelloSizeLen {
		fmt.Fprintf(os.Stderr, "%s%s stream %d: bad hello payload length %d%s\n",
			Red, Bold, streamIdx+1, len(msg.Data), Reset)
		return
	}
	sz := int64(binary.BigEndian.Uint64(msg.Data))
	if atomic.LoadInt64(&r.chunkSize) == 0 && sz > 0 {
		atomic.StoreInt64(&r.chunkSize, sz)
		fmt.Fprintf(os.Stderr, "%s%s sender advertised chunk size %d bytes%s\n",
			Green, Bold, sz, Reset)
	}
}

func (r *Receiver) recvLoop(streamIdx int) {
	for {
		select {
		case <-r.allDone:
			return
		default:
		}
		msg, err := readMsgDeadline(r.conns[streamIdx], r.connTmo)
		if err != nil {
			if isTimeout(err) {
				// Peer went silent beyond the timeout: kill and reconnect.
				fmt.Fprintf(os.Stderr, "%s%s stream %d: idle for %v, deemed dead, reconnecting...%s\n",
					Yellow, Bold, streamIdx+1, r.connTmo, Reset)
			} else if err == io.EOF || isClosedError(err) {
				return
			} else {
				fmt.Fprintf(os.Stderr, "%s%s stream %d: read error: %v, reconnecting...%s\n",
					Yellow, Bold, streamIdx+1, err, Reset)
			}
			if rerr := r.reconnectStream(streamIdx); rerr != nil {
				fmt.Fprintf(os.Stderr, "%s%s stream %d: reconnect failed: %v%s\n",
					Red, Bold, streamIdx+1, rerr, Reset)
				return
			}
			continue
		}

		// Hello (chunk size advertisement) may appear at any point, including
		// right after a reconnect, before any data messages.
		if msg.Type == MsgHello {
			r.handleHello(streamIdx, msg)
			continue
		}

		switch msg.Type {
		case MsgData:
			// test-only: drop the chunk one time so the missing detection +
			// resend + sender retransmit path can be exercised.
			if r.dropSeqs[msg.Seq] {
				delete(r.dropSeqs, msg.Seq)
				fmt.Fprintf(os.Stderr, "%s%s TEST: dropping seq %d on stream %d%s\n",
					BoldRed, Bold, msg.Seq, streamIdx+1, Reset)
				continue
			}
			r.receiveMu.Lock()
			if msg.Seq >= r.expectedSeq {
				if !r.seen[msg.Seq] {
					r.seen[msg.Seq] = true
					r.buffer[msg.Seq] = &Chunk{Seq: msg.Seq, Data: msg.Data}
					delete(r.missing, msg.Seq)
					if msg.Seq > r.maxSeen {
						// mark the gap between expectedSeq and msg.Seq as missing
						for s := r.maxSeen + 1; s < msg.Seq; s++ {
							if !r.seen[s] {
								if _, ok := r.missing[s]; !ok {
									r.missing[s] = time.Now()
								}
							}
						}
						r.maxSeen = msg.Seq
					}
				}
			}
			r.receiveMu.Unlock()

			r.prog.AddBytes(uint64(len(msg.Data)))
			r.prog.AddStreamBytes(streamIdx, uint64(len(msg.Data)))
			atomic.AddUint64(&r.totalRecv, uint64(len(msg.Data)))

			if wErr := r.writers[streamIdx].WriteMsg(&Message{Type: MsgACK, Seq: msg.Seq}); wErr != nil {
				fmt.Fprintf(os.Stderr, "%s%s stream %d: ack write error: %v%s\n",
					Red, Bold, streamIdx+1, wErr, Reset)
			}

		case MsgDONE:
			count := atomic.AddInt32(&r.doneCount, 1)
			if count == int32(r.numStreams) {
				// signal end; given time for stragglers to be flushed
				go func() {
					time.Sleep(r.resendTmo + time.Second)
					close(r.allDone)
				}()
			}
		}
	}
}

// drain writes all buffered contiguous chunks to stdout, advancing expectedSeq.
// Caller must hold receiveMu.
func (r *Receiver) drain() {
	for {
		chk, ok := r.buffer[r.expectedSeq]
		if !ok {
			break
		}
		os.Stdout.Write(chk.Data)
		delete(r.buffer, r.expectedSeq)
		delete(r.seen, r.expectedSeq)
		r.expectedSeq++
	}
}

// writeLoop periodically drains the buffer to stdout and updates progress.
func (r *Receiver) writeLoop() {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.allDone:
			return
		case <-ticker.C:
			r.receiveMu.Lock()
			r.drain()
			outOfOrder := len(r.buffer)
			missingCount := len(r.missing)
			r.receiveMu.Unlock()
			var extra string
			if outOfOrder > 0 || missingCount > 0 {
				extra = fmt.Sprintf("%s buffered=%d missing=%d%s", Yellow, outOfOrder, missingCount, Reset)
			} else {
				extra = ""
			}
			r.prog.Tick(atomic.LoadUint64(&r.totalRecv), extra)
		}
	}
}

// resendLoop periodically checks if any missing chunk has been missing
// longer than resendTmo and requests retransmission.
func (r *Receiver) resendLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.allDone:
			return
		case <-ticker.C:
			now := time.Now()
			var toRequest []uint64
			r.receiveMu.Lock()
			for seq, since := range r.missing {
				if now.Sub(since) > r.resendTmo {
					toRequest = append(toRequest, seq)
					// reset timer so we don't spam
					r.missing[seq] = now
				}
			}
			r.receiveMu.Unlock()
			for _, seq := range toRequest {
				r.requestResend(seq)
			}
		}
	}
}

func (r *Receiver) requestResend(seq uint64) {
	for i := 0; i < r.numStreams; i++ {
		if r.conns[i] != nil {
			if err := r.writers[i].WriteMsg(&Message{Type: MsgRESEND, Seq: seq}); err == nil {
				fmt.Fprintf(os.Stderr, "%s%s requested resend seq %d on stream %d%s\n",
					Magenta, Bold, seq, i+1, Reset)
				return
			}
		}
	}
}
