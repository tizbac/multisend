package main

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// Conn is one TCP stream shared by both the out (send) and the in (receive)
// half of a node. Writes are serialized by the underlying MsgWriter; the
// socket can be swapped when the stream dies and is re-established, so every
// caller must operate on the CURRENT socket rather than caching one.
//
// The `down` flag is what the round-robin sender consults: a stream that is
// disconnected is skipped until it is restored, instead of hammering a closed
// socket and spamming "use of closed network connection" errors.
type Conn struct {
	idx     int
	connTmo time.Duration
	mu      sync.RWMutex
	nc      net.Conn
	w       *MsgWriter
	down    bool
}

func NewConn(idx int, connTmo time.Duration) *Conn {
	return &Conn{idx: idx, connTmo: connTmo}
}

// set installs a fresh socket on the stream, closing any previous one. It
// clears the disconnected flag.
func (c *Conn) set(nc net.Conn) {
	c.mu.Lock()
	if c.nc != nil {
		c.nc.Close()
	}
	c.nc = nc
	c.w = NewMsgWriter(nc)
	c.down = false
	c.mu.Unlock()
}

// closeConn drops the socket and marks the stream disconnected.
func (c *Conn) closeConn() {
	c.mu.Lock()
	if c.nc != nil {
		c.nc.Close()
	}
	c.nc = nil
	c.w = nil
	c.down = true
	c.mu.Unlock()
}

// closeIfCurrent closes nc only if it is still the stream's active socket (it
// was not already replaced by a re-establishment). Reports whether it closed.
func (c *Conn) closeIfCurrent(nc net.Conn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nc == nc {
		c.nc.Close()
		c.nc = nil
		c.w = nil
		c.down = true
		return true
	}
	return false
}

func (c *Conn) isConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nc != nil
}

// send writes one message on the stream's current socket (no-op style error
// when disconnected).
func (c *Conn) send(m *Message) error {
	c.mu.RLock()
	w := c.w
	c.mu.RUnlock()
	if w == nil {
		return fmt.Errorf("stream %d: not connected", c.idx+1)
	}
	return w.WriteMsg(m)
}

// readMsg reads one message on the stream's current socket and also returns
// the socket it read from, so the caller can tell whether it got replaced
// between the read and any error handling.
func (c *Conn) readMsg() (*Message, net.Conn, error) {
	c.mu.RLock()
	nc := c.nc
	c.mu.RUnlock()
	if nc == nil {
		return nil, nil, fmt.Errorf("stream %d: not connected", c.idx+1)
	}
	msg, err := readMsgDeadline(nc, c.connTmo)
	return msg, nc, err
}

// writeStreamID sends the connector's stream index as the very first frame on
// a fresh socket. The listener reads this to route the socket to the right
// stream slot (essential with single-port multiplexing where every stream
// shares one listening port).
func writeStreamID(nc net.Conn, idx int) error {
	return NewMsgWriter(nc).WriteMsg(&Message{Type: MsgStreamID, Seq: uint64(idx)})
}

// readStreamID reads the connector's stream index frame. It applies a short
// deadline so a bogus client that connects and sends nothing cannot wedge the
// accept path.
func readStreamID(nc net.Conn) (int, error) {
	msg, err := readMsgDeadline(nc, 10*time.Second)
	if err != nil {
		return 0, err
	}
	if msg.Type != MsgStreamID {
		return 0, fmt.Errorf("expected stream-id frame, got type %d", msg.Type)
	}
	return int(msg.Seq), nil
}

func isClosedError(err error) bool {
	if err == nil {
		return false
	}
	return fmt.Sprintf("%v", err) == "use of closed network connection"
}