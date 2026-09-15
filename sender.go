package main

import (
	"bufio"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// outFlow is the transmitting half of a bidirectional node. It reads stdin,
// splits the stream into numbered chunks, and distributes them across the
// connected streams. The queue mode determines which stream gets the next
// chunk:
//   - "round-robin" (default): simple round-robin over connected streams,
//     skipping down streams.
//   - "least-unacked": sends the next chunk to the connected stream with the
//     fewest unacked (sent-but-not-yet-ACKed) chunks, so a stream that falls
//     behind automatically receives less new data.
// Chunks that cannot be handed to a live stream sit in `unsent` (and in
// `pending`) until a stream is restored, then they are flushed before new
// data, so nothing is lost when all connections drop at once.
type outFlow struct {
	conns     []*Conn
	chunkSize int
	resendTmo time.Duration
	noInput   bool
	prog      *ProgressDisplay
	mode      string // "round-robin" or "least-unacked"

	pending    sync.Map // seq -> *Chunk, kept until ACKed
	chunkStream sync.Map // seq -> int (stream idx the chunk was sent on)
	unsent     []*Chunk
	unsentMu   sync.Mutex
	nextSeq    uint64
	rr         uint64
	numChunks  uint64
	sendTotal  atomic.Uint64
	acked      atomic.Uint64
	eof        atomic.Bool
	done       chan struct{}

	// per-stream unacked-chunk counters (least-unacked mode). Written by the
	// Run goroutine and by the reader goroutines on ACK; hence atomic.
	pendingPerCnt []atomic.Int32
}

func newOutFlow(conns []*Conn, chunkSize int, resendTmo time.Duration, prog *ProgressDisplay, mode string) *outFlow {
	return &outFlow{
		conns:         conns,
		chunkSize:     chunkSize,
		resendTmo:     resendTmo,
		prog:          prog,
		mode:          mode,
		done:          make(chan struct{}),
		pendingPerCnt: make([]atomic.Int32, len(conns)),
	}
}

// nextConn picks the stream for the next chunk according to the queue mode.
func (o *outFlow) nextConn() *Conn {
	if o.mode == "least-unacked" {
		return o.nextConnLeastUnacked()
	}
	return o.nextConnRoundRobin()
}

// nextConnRoundRobin returns the next connected stream in round-robin order,
// or nil if every stream is currently down (the caller then queues the chunk
// instead of attempting a doomed write).
func (o *outFlow) nextConnRoundRobin() *Conn {
	start := o.rr
	for i := 0; i < len(o.conns); i++ {
		idx := int(o.rr % uint64(len(o.conns)))
		o.rr++
		if o.conns[idx].isConnected() {
			return o.conns[idx]
		}
	}
	o.rr = start
	return nil
}

// nextConnLeastUnacked returns the connected stream with the fewest unacked
// chunks; among equal counts the first one encountered (scanning round-robin
// from the current pointer) wins, and nil if no stream is connected.
func (o *outFlow) nextConnLeastUnacked() *Conn {
	n := len(o.conns)
	bestIdx := -1
	var bestCount int32
	for checked := 0; checked < n; checked++ {
		idx := int(o.rr % uint64(n))
		o.rr++
		if !o.conns[idx].isConnected() {
			continue
		}
		cnt := o.pendingPerCnt[idx].Load()
		if bestIdx < 0 || cnt < bestCount {
			bestCount = cnt
			bestIdx = idx
		}
	}
	if bestIdx >= 0 {
		return o.conns[bestIdx]
	}
	return nil
}

// markSent records that a chunk left on stream idx: bump its unacked count,
// remember which stream owns it (so the ACK can be attributed), and update
// the progress weight counter.
func (o *outFlow) markSent(c *Conn, chk *Chunk) {
	o.chunkStream.Store(chk.Seq, c.idx)
	o.pendingPerCnt[c.idx].Add(1)
	o.prog.AddSentChunk(c.idx)
}

// flushUnsent hands every queued chunk to one specific stream, preserving
// order; chunks that fail to send are re-queued at the head.
func (o *outFlow) flushUnsent(c *Conn) int {
	o.unsentMu.Lock()
	if len(o.unsent) == 0 {
		o.unsentMu.Unlock()
		return 0
	}
	q := o.unsent
	o.unsent = nil
	o.unsentMu.Unlock()

	n := 0
	for i, chk := range q {
		o.prog.AddStreamBytes(c.idx, uint64(len(chk.Data)))
		if err := c.send(&Message{Type: MsgData, Seq: chk.Seq, Data: chk.Data}); err != nil {
			o.unsentMu.Lock()
			o.unsent = append(o.unsent, q[i:]...)
			o.unsentMu.Unlock()
			break
		}
		o.markSent(c, chk)
		n++
	}
	return n
}

func (o *outFlow) flushUnsentToAnyConnected() {
	for _, c := range o.conns {
		if c.isConnected() {
			o.flushUnsent(c)
		}
	}
}

func (o *outFlow) pendingCount() int {
	count := 0
	o.pending.Range(func(_, _ any) bool { count++; return true })
	return count
}

// onConnEstablished is called whenever a stream's socket is (re)established:
// queued chunks are flushed, and if stdin already reached EOF a DONE marker
// is sent on the fresh socket so the peer's receive half can complete even
// though that stream's original DONE was lost with the dead socket.
func (o *outFlow) onConnEstablished(c *Conn) {
	o.flushUnsent(c)
	if o.eof.Load() {
		o.prog.Log("%s%s stream %d resumed after end-of-stream, confirming DONE%s",
			Cyan, Bold, c.idx+1, Reset)
		_ = c.send(&Message{Type: MsgDONE})
	}
}

func (o *outFlow) sendDoneOnAll() {
	for _, c := range o.conns {
		if c.isConnected() {
			_ = c.send(&Message{Type: MsgDONE})
		}
	}
}

// waitForDrain blocks until every pending chunk is ACKed. It never gives up
// while the peer might still be alive: if a stream is restored the queued
// chunks are flushed, and if the peer is really gone the process waits until
// it returns (mirroring the "stay up until the other side finishes" contract).
func (o *outFlow) waitForDrain() {
	grace := time.Now().Add(o.resendTmo * 2)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	warned := false
	for {
		o.flushUnsentToAnyConnected()
		if o.pendingCount() == 0 {
			return
		}
		<-ticker.C
		if time.Now().After(grace) && !warned {
			warned = true
			o.prog.Log("%s%s %d chunks still unacknowledged; staying up for the peer%s",
				Yellow, Bold, o.pendingCount(), Reset)
		}
	}
}

// Run reads stdin until EOF, then sends DONE on every stream, waits for the
// outstanding chunks to be acknowledged, and closes `done`.
func (o *outFlow) Run() {
	defer close(o.done)
	if o.noInput {
		o.prog.Log("%s%s stdin is a terminal; nothing to transmit%s", Yellow, Bold, Reset)
		o.eof.Store(true)
		o.sendDoneOnAll()
		return
	}

	reader := bufio.NewReaderSize(os.Stdin, o.chunkSize*2)
	for {
		buf := make([]byte, o.chunkSize)
		n, err := io.ReadFull(reader, buf)
		if n > 0 {
			o.enqueue(buf[:n])
			o.prog.Tick(o.sendTotal.Load(), 0)
		}
		if err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				o.prog.Log("%s%s stdin read error: %v%s", Red, Bold, err, Reset)
			}
			break
		}
	}

	o.eof.Store(true)
	o.sendDoneOnAll()
	o.waitForDrain()
	o.prog.SetPending(0)
}

// enqueue stores and transmits one chunk, flushing any backlog of unsent
// chunks on the same stream first when one is available. The queue mode
// determines which stream gets the chunk.
func (o *outFlow) enqueue(data []byte) {
	seq := atomic.AddUint64(&o.nextSeq, 1) - 1
	chk := &Chunk{Seq: seq, Data: data}
	o.pending.Store(seq, chk)
	o.numChunks++
	o.sendTotal.Add(uint64(len(data)))
	o.prog.AddBytes(uint64(len(data)))
	o.prog.SetPending(o.pendingCount())

	o.unsentMu.Lock()
	queued := len(o.unsent)
	o.unsentMu.Unlock()

	if c := o.nextConn(); c != nil {
		if queued > 0 {
			// new data must never overtake chunks that were queued while
			// every stream was down
			o.flushUnsent(c)
		}
		o.sendChunk(c, chk)
	} else {
		o.unsentMu.Lock()
		o.unsent = append(o.unsent, chk)
		o.unsentMu.Unlock()
	}
}

func (o *outFlow) sendChunk(c *Conn, chk *Chunk) bool {
	o.prog.AddStreamBytes(c.idx, uint64(len(chk.Data)))
	if err := c.send(&Message{Type: MsgData, Seq: chk.Seq, Data: chk.Data}); err != nil {
		o.unsentMu.Lock()
		o.unsent = append(o.unsent, chk)
		o.unsentMu.Unlock()
		o.prog.Log("%s%s stream %d: send failed: %v (queued)%s", Red, Bold, c.idx+1, err, Reset)
		return false
	}
	o.markSent(c, chk)
	return true
}

// decrPending atomically decrements a stream's unacked-chunk counter, never
// below zero.
func (o *outFlow) decrPending(idx int) {
	if idx < 0 || idx >= len(o.pendingPerCnt) {
		return
	}
	for {
		cur := o.pendingPerCnt[idx].Load()
		if cur <= 0 {
			return
		}
		if o.pendingPerCnt[idx].CompareAndSwap(cur, cur-1) {
			return
		}
	}
}

func (o *outFlow) handleACK(seq uint64) {
	o.pending.Delete(seq)
	o.acked.Add(1)
	if v, ok := o.chunkStream.LoadAndDelete(seq); ok {
		if idx, ok := v.(int); ok {
			o.decrPending(idx)
		}
	}
	o.prog.SetPending(o.pendingCount())
}

func (o *outFlow) handleRESEND(seq uint64, from *Conn) {
	val, ok := o.pending.Load(seq)
	if !ok || from == nil {
		return
	}
	chk := val.(*Chunk)
	o.prog.RecordRetransmit(from.idx)
	if err := from.send(&Message{Type: MsgData, Seq: seq, Data: chk.Data}); err != nil {
		o.prog.Log("%s%s stream %d: retransmit failed: %v%s", Red, Bold, from.idx+1, err, Reset)
		return
	}
	o.prog.AddSentChunk(from.idx)
	o.reassign(seq, from)
}

// reassign moves the unacked-chunk ownership of seq to the stream that now
// physically carries it after a retransmission.
func (o *outFlow) reassign(seq uint64, to *Conn) {
	fromIdx := -1
	if v, ok := o.chunkStream.LoadAndDelete(seq); ok {
		fromIdx, _ = v.(int)
	}
	o.chunkStream.Store(seq, to.idx)
	if fromIdx == to.idx {
		return
	}
	o.decrPending(fromIdx)
	if to.idx >= 0 && to.idx < len(o.pendingPerCnt) {
		o.pendingPerCnt[to.idx].Add(1)
	}
}
