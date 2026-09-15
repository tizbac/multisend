package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// inFlow is the receiving half of a bidirectional node. It reassembles the
// peer's numbered chunks in sequence order, writes them to stdout, and drives
// retransmission: missing chunks are detected from gaps, requested on any
// live stream after resendTmo, and acknowledged as they arrive.
type inFlow struct {
	conns     []*Conn
	resendTmo time.Duration
	prog      *ProgressDisplay

	receiveMu   sync.Mutex
	chunkSize   atomic.Int64
	expectedSeq uint64
	maxSeen     uint64
	seen        map[uint64]bool
	missing     map[uint64]time.Time // seq -> when first detected missing
	buffer      map[uint64]*Chunk    // out-of-order chunks awaiting their turn
	recvTotal   atomic.Uint64
	recvChunks  atomic.Uint64
	doneCount   int32
	doneOnce    sync.Once
	done        chan struct{}
	dropSeqs    map[uint64]bool
}

func newInFlow(conns []*Conn, resendTmo time.Duration, prog *ProgressDisplay) *inFlow {
	return &inFlow{
		conns:     conns,
		resendTmo: resendTmo,
		prog:      prog,
		seen:      make(map[uint64]bool),
		missing:   make(map[uint64]time.Time),
		buffer:    make(map[uint64]*Chunk),
		done:      make(chan struct{}),
		dropSeqs:  parseDropSeqs(),
	}
}

// parseDropSeqs reads MULTISEND_TEST_DROP_SEQS (comma-separated seq numbers),
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

func (r *inFlow) onHello(msg *Message) {
	if len(msg.Data) != HelloSizeLen {
		r.prog.Log("%s%s peer advertised a malformed hello%s", Red, Bold, Reset)
		return
	}
	sz := int64(binary.BigEndian.Uint64(msg.Data))
	if r.chunkSize.Load() == 0 && sz > 0 {
		r.chunkSize.Store(sz)
		r.prog.Log("%s%s peer advertised chunk size %d bytes%s", Green, Bold, sz, Reset)
	}
}

func (r *inFlow) onData(streamIdx int, msg *Message) {
	if r.dropSeqs[msg.Seq] {
		delete(r.dropSeqs, msg.Seq)
		r.prog.Log("%s%s TEST: dropping seq %d on stream %d%s", BoldRed, Bold, msg.Seq, streamIdx+1, Reset)
		return
	}

	r.receiveMu.Lock()
	if msg.Seq >= r.expectedSeq {
		if !r.seen[msg.Seq] {
			r.seen[msg.Seq] = true
			r.buffer[msg.Seq] = &Chunk{Seq: msg.Seq, Data: msg.Data}
			delete(r.missing, msg.Seq)
			if msg.Seq > r.maxSeen {
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

	r.recvTotal.Add(uint64(len(msg.Data)))
	r.recvChunks.Add(1)
	r.prog.AddRecvBytes(uint64(len(msg.Data)))
	r.prog.AddRecvStreamBytes(streamIdx, uint64(len(msg.Data)))

	// Acknowledge on the same stream the chunk arrived on.
	if err := r.conns[streamIdx].send(&Message{Type: MsgACK, Seq: msg.Seq}); err != nil {
		r.prog.Log("%s%s stream %d: ack failed: %v%s", Red, Bold, streamIdx+1, err, Reset)
	}
}

// onDONE counts one finished out-stream of the peer; when every stream has
// delivered its DONE the receive half completes after a grace period that
// lets stragglers and retransmits arrive.
func (r *inFlow) onDONE() {
	count := atomic.AddInt32(&r.doneCount, 1)
	if count == int32(len(r.conns)) {
		r.doneOnce.Do(func() {
			go func() {
				time.Sleep(r.resendTmo + time.Second)
				close(r.done)
			}()
		})
	}
}

// drain writes all buffered contiguous chunks to stdout, advancing
// expectedSeq. Caller must hold receiveMu.
func (r *inFlow) drain() {
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

// writeLoop periodically drains the reordered buffer to stdout and refreshes
// the receive side of the progress display.
func (r *inFlow) writeLoop() {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
			r.receiveMu.Lock()
			r.drain()
			buffered := len(r.buffer)
			missing := len(r.missing)
			r.receiveMu.Unlock()
			r.prog.SetBuffer(buffered, missing)
			r.prog.Tick(0, r.recvTotal.Load())
		}
	}
}

// flushFinal drains whatever is still buffered when the node is shutting
// down (late arrivals that raced the DONE grace period).
func (r *inFlow) flushFinal() {
	r.receiveMu.Lock()
	r.drain()
	r.receiveMu.Unlock()
}

// resendLoop requests retransmission of chunks that have been missing longer
// than resendTmo.
func (r *inFlow) resendLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
			now := time.Now()
			var toRequest []uint64
			r.receiveMu.Lock()
			for seq, since := range r.missing {
				if now.Sub(since) > r.resendTmo {
					toRequest = append(toRequest, seq)
					r.missing[seq] = now // reset timer so we don't spam
				}
			}
			r.receiveMu.Unlock()
			for _, seq := range toRequest {
				r.requestResend(seq)
			}
		}
	}
}

func (r *inFlow) requestResend(seq uint64) {
	for _, c := range r.conns {
		if c.isConnected() {
			if err := c.send(&Message{Type: MsgRESEND, Seq: seq}); err == nil {
				return
			}
		}
	}
}