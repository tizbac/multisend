package main

import (
	"fmt"
	"net"
	"time"
)

// Node is the whole process on one machine. It runs a transmitting half
// (outFlow: stdin -> numbered chunks) and a receiving half (inFlow:
// incoming chunks -> stdout) at the same time over the same set of TCP
// streams, so the tool is fully bidirectional and has no concept of a
// "sender" or "receiver" machine.
//
// Connection roles are independent of data flow: one node listens and the
// other connects; data may flow in either or both directions. By default all
// N streams share a single port (the connecting side dials it N times, tagged
// with a stream index, and the listening side routes each socket to its
// slot). With --single-port=false each stream uses basePort+i as before.
type Node struct {
	host       string
	basePort   int
	singlePort bool
	listenMode bool
	numStreams int
	chunkSize  int
	connTmo    time.Duration
	resendTmo  time.Duration
	progress   bool
	queueMode  string
	lifetime   time.Duration // 0 = keep connections forever (client side only)
	start      time.Time

	conns     []*Conn
	listeners []net.Listener
	prog      *ProgressDisplay
	out       *outFlow
	in        *inFlow
	done      chan struct{}
}

func NewNode(host string, basePort int, listenMode, singlePort bool, numStreams, chunkSize int,
	connTmo, resendTmo time.Duration, progress bool, queueMode string, lifetime time.Duration) *Node {
	n := &Node{
		host:       host,
		basePort:   basePort,
		singlePort: singlePort,
		listenMode: listenMode,
		numStreams: numStreams,
		chunkSize:  chunkSize,
		connTmo:    connTmo,
		resendTmo:  resendTmo,
		progress:   progress,
		queueMode:  queueMode,
		lifetime:   lifetime,
		start:      time.Now(),
		done:       make(chan struct{}),
	}
	n.conns = make([]*Conn, numStreams)
	for i := 0; i < numStreams; i++ {
		n.conns[i] = NewConn(i, connTmo)
	}
	return n
}

func (n *Node) listenAddr(i int) string {
	port := n.basePort
	if !n.singlePort {
		port += i
	}
	return net.JoinHostPort(n.host, fmt.Sprintf("%d", port))
}

func (n *Node) connectAddr(i int) string {
	port := n.basePort
	if !n.singlePort {
		port += i
	}
	return net.JoinHostPort(n.host, fmt.Sprintf("%d", port))
}

func (n *Node) closed() bool {
	select {
	case <-n.done:
		return true
	default:
		return false
	}
}

func (n *Node) Run() error {
	n.prog = NewProgressDisplay(n.numStreams, n.progress, n.connTmo)
	n.out = newOutFlow(n.conns, n.chunkSize, n.resendTmo, n.prog, n.queueMode)
	n.out.noInput = stdinIsTTY()
	n.in = newInFlow(n.conns, n.resendTmo, n.prog)

	var err error
	if n.listenMode {
		err = n.setupListener()
	} else {
		err = n.setupConnector()
	}
	if err != nil {
		return err
	}

	for i := 0; i < n.numStreams; i++ {
		go n.reader(i)
	}
	go n.in.writeLoop()
	go n.in.resendLoop()
	go n.out.Run()
	if n.lifetime > 0 && !n.listenMode {
		go n.rotateLoop()
	}

	// Stay up until BOTH directions have finished: the local stdin is fully
	// transmitted and acknowledged, and the peer has finished transmitting
	// to us too. If only one side's stdin reaches EOF the node keeps running
	// for the other direction.
	allFinished := make(chan struct{})
	go func() {
		<-n.out.done
		<-n.in.done
		close(allFinished)
	}()
	<-allFinished
	close(n.done)

	n.in.flushFinal()
	n.prog.Final(n.out.sendTotal.Load(), n.out.numChunks, n.in.recvTotal.Load(), n.in.recvChunks.Load())

	for i := range n.conns {
		n.conns[i].closeConn()
	}
	for _, ln := range n.listeners {
		ln.Close()
	}
	return nil
}

// setupListener binds the listening port(s) and blocks until every stream
// slot has been claimed by an incoming connection.
func (n *Node) setupListener() error {
	if n.singlePort {
		ln, err := net.Listen("tcp", n.listenAddr(0))
		if err != nil {
			return fmt.Errorf("listen %s: %w", n.listenAddr(0), err)
		}
		n.listeners = append(n.listeners, ln)
		go n.acceptLoop(ln)
	} else {
		for i := 0; i < n.numStreams; i++ {
			ln, err := net.Listen("tcp", n.listenAddr(i))
			if err != nil {
				return fmt.Errorf("listen %s: %w", n.listenAddr(i), err)
			}
			n.listeners = append(n.listeners, ln)
			go n.acceptLoop(ln)
		}
	}

	for i := 0; i < n.numStreams; i++ {
		for !n.conns[i].isConnected() {
			if n.closed() {
				return nil
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	return nil
}

// acceptLoop accepts connections and routes each one to its stream slot based
// on the connector's stream-id frame.
func (n *Node) acceptLoop(ln net.Listener) {
	for {
		nc, err := ln.Accept()
		if err != nil {
			if n.closed() || isClosedError(err) {
				return
			}
			n.prog.Log("%s%s accept error: %v%s", Yellow, Bold, err, Reset)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		idx, err := readStreamID(nc)
		if err != nil || idx < 0 || idx >= n.numStreams {
			n.prog.Log("%s%s dropped connection with unreadable stream id%s", Yellow, Bold, Reset)
			nc.Close()
			continue
		}
		n.establish(idx, nc)
	}
}

// establish installs a socket on a stream slot and performs the local side of
// the (re)connection: advertise our chunk size and give the out half a chance
// to flush queued chunks / resend DONE.
func (n *Node) establish(idx int, nc net.Conn) {
	n.conns[idx].set(nc)
	_ = n.conns[idx].send(&Message{Type: MsgHello, Data: helloFor(n.chunkSize)})
	n.out.onConnEstablished(n.conns[idx])
	n.prog.SetStreamDown(idx, false)
	n.prog.Log("%s%s stream %d connected%s", Green, Bold, idx+1, Reset)
}

// setupConnector dials every stream, tagging each socket with its stream id.
func (n *Node) setupConnector() error {
	for i := 0; i < n.numStreams; i++ {
		if err := n.dialAndSet(i); err != nil {
			return err
		}
	}
	return nil
}

// dialAndSet connects one stream and performs the connector side handshake.
// It retries up to a fixed number of attempts for the initial connection.
func (n *Node) dialAndSet(i int) error {
	addr := n.connectAddr(i)
	for attempt := 0; ; attempt++ {
		nc, err := net.DialTimeout("tcp", addr, 10*time.Second)
		if err != nil {
			if attempt >= 5 {
				return fmt.Errorf("stream %d: failed to connect to %s: %w", i+1, addr, err)
			}
			n.prog.Log("%sstream %d: connect to %s failed (%d/5), retrying...%s",
				Yellow, i+1, addr, attempt+1, Reset)
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}
		if err := writeStreamID(nc, i); err != nil {
			nc.Close()
			continue
		}
		n.establish(i, nc)
		return nil
	}
}

// redial keeps a stream restored after its socket died.
func (n *Node) redial(slot int) {
	for {
		err := n.dialAndSet(slot)
		if err == nil {
			return
		}
		select {
		case <-n.done:
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// rotateLoop implements the client-side --conn-lifetime feature. Every
// lifetime period it force-closes every stream socket regardless of whether
// the connection is alive; each stream's reader goroutine then observes the
// break through the normal error path and re-establishes the stream.
func (n *Node) rotateLoop() {
	ticker := time.NewTicker(n.lifetime)
	defer ticker.Stop()
	rot := uint64(0)
	for {
		select {
		case <-n.done:
			return
		case <-ticker.C:
			if n.closed() {
				return
			}
			rot++
			n.prog.Log("%s%s rotation %d: closing and re-establishing %d stream(s)%s",
				Cyan, Bold, rot, n.numStreams, Reset)
			for i := range n.conns {
				n.conns[i].forceClose()
			}
		}
	}
}

// reader owns one stream: it dispatches incoming messages to the right half
// and, when the socket dies, marks the stream disconnected and restores it
// (redialing when we are the connecting side, or waiting for the accept loop
// to route a replacement when we are the listening side).
func (n *Node) reader(slot int) {
	for {
		c := n.conns[slot]
		msg, nc, err := c.readMsg()
		if err != nil {
			if n.closed() {
				return
			}
			if !c.closeIfCurrent(nc) {
				// The socket was already replaced by a re-establishment;
				// keep reading the fresh one.
				continue
			}
			if isTimeout(err) {
				n.prog.Log("%s%s stream %d: idle for %v, deemed dead, restoring...%s",
					Yellow, Bold, slot+1, n.connTmo, Reset)
			} else {
				n.prog.Log("%s%s stream %d: connection lost: %v, restoring...%s",
					Yellow, Bold, slot+1, err, Reset)
			}
			n.prog.SetStreamDown(slot, true)

			if !n.listenMode {
				n.redial(slot)
			} else {
				for !n.conns[slot].isConnected() {
					if n.closed() {
						return
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
			continue
		}
		n.dispatch(slot, msg)
	}
}

// dispatch routes a message by type. DATA/DONE/HELLO concern data we receive
// (the peer's out-stream); ACK/RESEND concern data we sent (our out-stream;
// the wire seq space is owned by whoever sent the message, so no direction
// byte is needed).
func (n *Node) dispatch(slot int, m *Message) {
	switch m.Type {
	case MsgData:
		n.in.onData(slot, m)
	case MsgACK:
		n.out.handleACK(m.Seq)
	case MsgRESEND:
		n.out.handleRESEND(m.Seq, n.conns[slot])
	case MsgDONE:
		n.in.onDONE()
	case MsgHello:
		n.in.onHello(m)
	}
}