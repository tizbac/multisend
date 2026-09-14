package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type StreamState struct {
	mu      sync.Mutex
	conn    net.Conn
	writer  *MsgWriter
	bytesIn uint64 // received from this peer (during this stream lifetime)
}

func (ss *StreamState) setConn(c net.Conn) {
	ss.mu.Lock()
	ss.conn = c
	ss.writer = NewMsgWriter(c)
	ss.mu.Unlock()
}

func (ss *StreamState) getWriter() *MsgWriter {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.writer
}

func (ss *StreamState) getConn() net.Conn {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.conn
}

func (ss *StreamState) closeConn() {
	ss.mu.Lock()
	if ss.conn != nil {
		ss.conn.Close()
	}
	ss.mu.Unlock()
}

type Sender struct {
	basePort   int
	numStreams int
	chunkSize  int
	connTmo    time.Duration
	resendTmo  time.Duration

	listeners []net.Listener
	streams   []*StreamState
	// round-robin cursor
	rr uint64

	pending   sync.Map // seq -> *Chunk
	nextSeq   uint64
	totalSent uint64
	acked     uint64

	prog *ProgressDisplay
	done chan struct{}
}

func NewSender(basePort, numStreams, chunkSize int, connTmo, resendTmo time.Duration, progress ...bool) *Sender {
	enabled := true
	if len(progress) > 0 {
		enabled = progress[0]
	}
	return &Sender{
		basePort:   basePort,
		numStreams: numStreams,
		chunkSize:  chunkSize,
		connTmo:    connTmo,
		resendTmo:  resendTmo,
		listeners:  make([]net.Listener, numStreams),
		streams:    make([]*StreamState, numStreams),
		prog:       NewProgressDisplay("sender", numStreams, chunkSize, enabled),
		done:       make(chan struct{}),
	}
}

// sendHello advertises the chunk size on the given stream's current connection.
func (s *Sender) sendHello(streamIdx int) error {
	hello := make([]byte, HelloSizeLen)
	binary.BigEndian.PutUint64(hello, uint64(s.chunkSize))
	w := s.streams[streamIdx].getWriter()
	if w == nil {
		return fmt.Errorf("stream %d has no writer", streamIdx)
	}
	return w.WriteMsg(&Message{Type: MsgHello, Seq: 0, Data: hello})
}

func (s *Sender) Run() error {
	for i := 0; i < s.numStreams; i++ {
		addr := fmt.Sprintf(":%d", s.basePort+i)
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("listen on %s: %w", addr, err)
		}
		s.listeners[i] = ln
		s.streams[i] = &StreamState{}
		fmt.Fprintf(os.Stderr, "%s%s listening on %s (stream %d/%d)%s\n",
			BoldCyan, Bold, addr, i+1, s.numStreams, Reset)
	}

	// Wait for initial connections from all N streams
	for i := 0; i < s.numStreams; i++ {
		conn, err := s.listeners[i].Accept()
		if err != nil {
			return fmt.Errorf("accept stream %d: %w", i+1, err)
		}
		s.streams[i].setConn(conn)
		fmt.Fprintf(os.Stderr, "%s%s stream %d connected from %s%s\n",
			Green, Bold, i+1, conn.RemoteAddr(), Reset)

		// Advertise the chunk size on every connection.
		if err := s.sendHello(i); err != nil {
			return fmt.Errorf("send hello on stream %d: %w", i+1, err)
		}
	}

	// One goroutine handles read-loop + re-accept for each stream
	for i := 0; i < s.numStreams; i++ {
		go s.manageStream(i)
	}

	err := s.readStdin()
	<-s.done
	return err
}

// manageStream keeps the stream alive: reads ACKs/RESENDs, and when the
// connection drops, accepts a replacement on the listener.
func (s *Sender) manageStream(streamIdx int) {
	for {
		conn := s.streams[streamIdx].getConn()
		if conn == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		msg, err := readMsgDeadline(conn, s.connTmo)
		if err != nil {
			s.streams[streamIdx].closeConn()
			switch {
			case isTimeout(err):
				fmt.Fprintf(os.Stderr, "%s%s stream %d: idle for %v, deemed dead, re-establishing...%s\n",
					Yellow, Bold, streamIdx+1, s.connTmo, Reset)
			default:
				fmt.Fprintf(os.Stderr, "%s%s stream %d: connection dropped: %v, re-establishing...%s\n",
					Yellow, Bold, streamIdx+1, err, Reset)
			}
		} else {
			s.handleControl(msg, streamIdx)
			// loop back to read the next message on the same connection
			continue
		}

		// Re-accept a new connection on this listener
		for {
			if !s.alive() {
				return
			}
			conn, aerr := s.listeners[streamIdx].Accept()
			if aerr != nil {
				if isClosedError(aerr) {
					return
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}
			s.streams[streamIdx].setConn(conn)
			fmt.Fprintf(os.Stderr, "%s%s stream %d reconnected from %s%s\n",
				Green, Bold, streamIdx+1, conn.RemoteAddr(), Reset)
			if err := s.sendHello(streamIdx); err != nil {
				fmt.Fprintf(os.Stderr, "%s%s stream %d: hello write failed: %v%s\n",
					Red, Bold, streamIdx+1, err, Reset)
			}
			break
		}
	}
}

func (s *Sender) handleControl(msg *Message, streamIdx int) {
	switch msg.Type {
	case MsgACK:
		s.pending.Delete(msg.Seq)
		atomic.AddUint64(&s.acked, 1)
	case MsgRESEND:
		go s.retransmit(msg.Seq, streamIdx)
	}
}

func (s *Sender) alive() bool {
	select {
	case <-s.done:
		return false
	default:
		return true
	}
}

// waitForDrain blocks until all pending chunks are ACKed or a timeout expires.
func (s *Sender) waitForDrain() {
	deadline := time.After(s.resendTmo * 2)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		count := 0
		s.pending.Range(func(_, _ any) bool { count++; return true })
		if count == 0 {
			return
		}
		select {
		case <-deadline:
			count = 0
			s.pending.Range(func(_, _ any) bool { count++; return true })
			fmt.Fprintf(os.Stderr, "%s%s warning: %d chunks still pending after timeout, proceeding%s\n",
				Yellow, Bold, count, Reset)
			return
		case <-ticker.C:
		}
	}
}

func (s *Sender) retransmit(seq uint64, streamIdx int) {
	val, ok := s.pending.Load(seq)
	if !ok {
		return
	}
	chk := val.(*Chunk)
	fmt.Fprintf(os.Stderr, "%s%s stream %d: retransmit seq %d (%d bytes)%s\n",
		Magenta, Bold, streamIdx+1, seq, len(chk.Data), Reset)
	w := s.streams[streamIdx].getWriter()
	if w == nil {
		return
	}
	if err := w.WriteMsg(&Message{Type: MsgData, Seq: seq, Data: chk.Data}); err != nil {
		fmt.Fprintf(os.Stderr, "%s%s stream %d: retransmit write error: %v%s\n",
			Red, Bold, streamIdx+1, err, Reset)
	}
}

// sendChunkTo sends a chunk to stream streamIdx (round-robin assignment).
func (s *Sender) sendChunkTo(streamIdx int, chk *Chunk) error {
	w := s.streams[streamIdx].getWriter()
	if w == nil {
		return fmt.Errorf("stream %d has no writer", streamIdx)
	}
	s.prog.AddStreamBytes(streamIdx, uint64(len(chk.Data)))
	return w.WriteMsg(&Message{Type: MsgData, Seq: chk.Seq, Data: chk.Data})
}

func (s *Sender) readStdin() error {
	reader := bufio.NewReaderSize(os.Stdin, s.chunkSize*2)

	for {
		buf := make([]byte, s.chunkSize)
		n, err := io.ReadFull(reader, buf)
		if n > 0 {
			seq := atomic.AddUint64(&s.nextSeq, 1) - 1
			chk := &Chunk{Seq: seq, Data: buf[:n]}

			s.pending.Store(seq, chk)
			s.prog.AddBytes(uint64(n))
			atomic.AddUint64(&s.totalSent, uint64(n))

			idx := int(s.rr % uint64(s.numStreams))
			s.rr++
			if werr := s.sendChunkTo(idx, chk); werr != nil {
				// Will be retransmitted if the receiver requests it.
				fmt.Fprintf(os.Stderr, "%s%s stream %d: write error: %v (queued for resend)%s\n",
					Red, Bold, idx+1, werr, Reset)
			}

			pending := atomic.LoadUint64(&s.totalSent) - atomic.LoadUint64(&s.acked)
			extra := fmt.Sprintf("%s pending=%s%s", Yellow, formatBytes(pending), Reset)
			if s.streams[idx].getConn() == nil {
				extra += fmt.Sprintf("%s stream%d=DOWN%s", BoldRed, idx+1, Reset)
			}
			s.prog.Tick(atomic.LoadUint64(&s.totalSent), extra)
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return fmt.Errorf("stdin read: %w", err)
		}
	}

	// Flush DONE markers on all streams
	var wg sync.WaitGroup
	for i := 0; i < s.numStreams; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			w := s.streams[idx].getWriter()
			if w != nil {
				_ = w.WriteMsg(&Message{Type: MsgDONE, Seq: 0})
			}
		}(i)
	}
	wg.Wait()

	// Wait for all pending chunks to be ACKed, or for a generous timeout
	// that gives the receiver time to detect missing chunks and request resend.
	s.waitForDrain()
	s.prog.Final(atomic.LoadUint64(&s.totalSent), atomic.LoadUint64(&s.acked))
	close(s.done)

	for i := 0; i < s.numStreams; i++ {
		s.streams[i].closeConn()
		s.listeners[i].Close()
	}
	return nil
}

func isClosedError(err error) bool {
	if err == nil {
		return false
	}
	return fmt.Sprintf("%v", err) == "use of closed network connection"
}
