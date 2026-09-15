package main

import (
	"os"
	"syscall"
	"unsafe"
)

const (
	ioctlTCGETS     = 0x5401 // termios GET, Linux
	ioctlTIOCGWINSZ = 0x5413 // window size, Linux
)

// stderrIsTTY reports whether stderr is an interactive terminal.
func stderrIsTTY() bool {
	fd := os.Stderr.Fd()
	var termios syscall.Termios
	_, _, errno := syscall.Syscall6(syscall.SYS_IOCTL, fd, ioctlTCGETS, uintptr(unsafe.Pointer(&termios)), 0, 0, 0)
	return errno == 0
}

// stdinIsTTY reports whether stdin is an interactive terminal (not a pipe).
// When it is, the bidirectional node treats it as "no input" so it sends
// nothing instead of blocking forever waiting for terminal input.
func stdinIsTTY() bool {
	fd := os.Stdin.Fd()
	var termios syscall.Termios
	_, _, errno := syscall.Syscall6(syscall.SYS_IOCTL, fd, ioctlTCGETS, uintptr(unsafe.Pointer(&termios)), 0, 0, 0)
	return errno == 0
}

type winsize struct {
	Row, Col       uint16
	Xpixel, Ypixel uint16
}

// terminalSize returns the stderr terminal size in (columns, rows).
// Returns 0,0 when not a terminal or the ioctl fails.
func terminalSize() (int, int) {
	fd := os.Stderr.Fd()
	var ws winsize
	_, _, errno := syscall.Syscall6(syscall.SYS_IOCTL, fd, ioctlTIOCGWINSZ, uintptr(unsafe.Pointer(&ws)), 0, 0, 0)
	if errno != 0 {
		return 0, 0
	}
	return int(ws.Col), int(ws.Row)
}
