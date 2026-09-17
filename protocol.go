package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const (
	MsgData    byte = 1
	MsgACK     byte = 2
	MsgRESEND  byte = 3
	MsgDONE    byte = 4
	MsgHello   byte = 5
	MsgStreamID byte = 6

	MsgHdrSize = 1 + 8 + 4 // type(1) + seq(8) + datalen(4) = 13
)

// Hello carries the sender's chunk size (as 8-byte big-endian) so the
// receiver learns it from the wire instead of a CLI flag.
const HelloSizeLen = 8

// helloFor encodes the chunk size as an 8-byte big-endian Hello payload.
func helloFor(chunkSize int) []byte {
	b := make([]byte, HelloSizeLen)
	binary.BigEndian.PutUint64(b, uint64(chunkSize))
	return b
}

type Message struct {
	Type byte
	Seq  uint64
	Data []byte
}

type Chunk struct {
	Seq  uint64
	Data []byte
}

type MsgWriter struct {
	w  io.Writer
	nc net.Conn
	mu sync.Mutex
}

func NewMsgWriter(w io.Writer) *MsgWriter {
	var nc net.Conn
	if c, ok := w.(net.Conn); ok {
		nc = c
	}
	return &MsgWriter{w: w, nc: nc}
}

func (mw *MsgWriter) WriteMsg(m *Message) error {
	return mw.write(m, 0)
}

// WriteMsgDeadline writes one message, arming a write deadline on the
// underlying socket first so a peer that stops reading cannot wedge this
// goroutine forever. A tmo <= 0 disables the deadline.
func (mw *MsgWriter) WriteMsgDeadline(m *Message, tmo time.Duration) error {
	return mw.write(m, tmo)
}

func (mw *MsgWriter) write(m *Message, tmo time.Duration) error {
	datalen := uint32(len(m.Data))
	buf := make([]byte, MsgHdrSize+len(m.Data))
	buf[0] = m.Type
	binary.BigEndian.PutUint64(buf[1:9], m.Seq)
	binary.BigEndian.PutUint32(buf[9:13], datalen)
	copy(buf[13:], m.Data)

	mw.mu.Lock()
	defer mw.mu.Unlock()
	if tmo > 0 && mw.nc != nil {
		if err := mw.nc.SetWriteDeadline(time.Now().Add(tmo)); err != nil {
			return err
		}
	}
	_, err := mw.w.Write(buf)
	if err == nil && tmo > 0 && mw.nc != nil {
		// Clear the deadline so it doesn't leak into later messages.
		mw.nc.SetWriteDeadline(time.Time{})
	}
	return err
}

func ReadMsg(r io.Reader) (*Message, error) {
	hdr := make([]byte, MsgHdrSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	m := &Message{
		Type: hdr[0],
		Seq:  binary.BigEndian.Uint64(hdr[1:9]),
	}
	datalen := binary.BigEndian.Uint32(hdr[9:13])
	if datalen > 0 {
		m.Data = make([]byte, datalen)
		if _, err := io.ReadFull(r, m.Data); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// readMsgDeadline reads one message but fails the underlying read if the
// connection stays silent for the given timeout. A value <= 0 disables the
// deadline. This is how both sides detect a dead connection: if the peer
// stops producing traffic, we close the socket and re-establish it.
func readMsgDeadline(conn net.Conn, tmo time.Duration) (*Message, error) {
	if tmo > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(tmo)); err != nil {
			return nil, err
		}
	}
	return ReadMsg(conn)
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	if ne, ok := err.(net.Error); ok {
		return ne.Timeout()
	}
	return false
}

func parseAddr(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid address %q: %w", addr, err)
	}
	var port int
	_, err = fmt.Sscanf(portStr, "%d", &port)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port %q: %w", portStr, err)
	}
	return host, port, nil
}
