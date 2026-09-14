# multisend

Parallel TCP stream multiplexer for piped data.

`multisend` reads data from stdin, splits it into numbered chunks, and
distributes them across **N parallel TCP streams** in round-robin order. The
receiver reassembles the chunks in order and writes them to stdout. Designed
for piping between commands like `zfs send`/`zfs recv`.

On one end of the pipe:

```sh
zfs send pool/dataset | multisend --sender --listen :9000 --streams 4
```

On the other:

```sh
multisend --receiver --connect machineA:9000 --streams 4 | zfs recv pool/dataset
```

## Features

- Round-robin distribution across N TCP streams
- 64-bit chunk sequence numbering
- In-memory chunk buffer with ACK-based eviction
- Automatic retransmission requests for missing chunks
- Automatic connection reestablishment on drops
- **Live reporting, refreshed once per second**: speed of every connection
  plus total speed, shown in a **box centered on the terminal width**; on each
  refresh the cursor returns to the top of the box and the previous content is
  rewritten in place, so the terminal scroll-back buffer is never exhausted,
  even on very long transfers
- **Optional progress UI**: turn it on or off with the `--progress` flag

## Wire protocol

Every on-the-wire message has the shape:

```
[1 byte type][8 byte seq (big-endian)][4 byte datalen (big-endian)][data...]
```

Message types: `DATA(1)`, `ACK(2)`, `RESEND(3)`, `DONE(4)`, `HELLO(5)`.

The `HELLO` message, sent by the sender when each connection opens (including a
re-established one after a drop), advertises the chunk size chosen on the
sender side; the receiver uses it to compute the total chunk count.

## Installation

Minimum Go version: **1.21**.

```sh
go build -o multisend .
# alternatively, install into GOPATH/bin
go install .
```

## Usage

```
multisend --sender --listen [ADDR:BASE_PORT] [--streams N] [--chunk-size N] \
          [--resend-timeout D] [--conn-timeout D] [--progress]
multisend --receiver --connect [HOST:BASE_PORT] [--streams N] \
          [--resend-timeout D] [--conn-timeout D] [--progress]
```

The sender listens on ports `base..base+N-1`; the receiver connects to each of
them. The port given to `--listen`/`--connect` is the **base** for the N
streams.

Real-world use (two machines):

```sh
# Machine A (sender)
zfs send pool/dataset | multisend --sender --listen :9000 --streams 4

# Machine B (receiver)
multisend --receiver --connect machineA:9000 --streams 4 | zfs recv pool/dataset
```

Local test:

```sh
dd if=/dev/urandom bs=1M count=100 \
  | multisend --sender --listen :9000 \
  | multisend --receiver --connect 127.0.0.1:9000 \
  | md5sum
```

## Options

| Flag | Description |
| --- | --- |
| `--sender` | Sender mode (reads stdin, distributes across TCP streams) |
| `--receiver` | Receiver mode (reassembles from streams, writes stdout) |
| `--listen ADDR` | Sender listen address (default: `0.0.0.0:9000`) |
| `--connect ADDR` | Address to connect to as receiver (required in receiver mode) |
| `--streams N` | Number of parallel TCP streams (default: 4) |
| `--chunk-size N` | Chunk size in bytes, sender only (default: 65536 = 64 KiB). Advertised to the receiver via HELLO; ignored by `--receiver` |
| `--resend-timeout D` | Timeout before requesting retransmission of a missing chunk (default: 3s) |
| `--conn-timeout D` | Idle timeout per connection; a stream with no traffic for this long is deemed dead and killed + re-established (default: 30s, `0` to disable) |
| `--progress` | Show live progress box on stderr (default: `true`; `--progress=false` to disable) |
| `--version` | Show version and exit |

## Progress UI

- Refreshed **once per second**.
- Shows the **speed of every connection** and the **total speed**, together
  with the transferred bytes and elapsed time.
- When stderr is a terminal, the output is drawn as a **centered box** whose
  width fits the terminal size. On every refresh the cursor is returned to the
  top of the box and the rows are rewritten in place: the terminal scroll-back
  buffer does not grow, even for very long transfers.
- When stderr is **not** a terminal (e.g. redirected into a file), a single
  plain text line is printed each second, safe for logs and pipes.
- With `--progress=false` the UI is completely silent.

### Terminal appearance example (with `--streams 4`)

```
            ┌────────────────────────────────────────────┐
            │ SEND  2.00 MiB  @ 2.50 MiB/s       1.0s    │
            │  cons  1  0.8 MiB/s                        │
            │  cons  2  0.8 MiB/s                        │
            │  cons  3  0.8 MiB/s                        │
            │  cons  4  0.9 MiB/s                        │
            │  pending=2.00 MiB                          │
            └────────────────────────────────────────────┘
```

## Reliability

- Every sent chunk is kept in a buffer until the receiver's ACK is received.
- Out-of-order chunks are held by the receiver until the gap is filled, then
  written in sequence to stdout.
- If a chunk is missing for longer than `--resend-timeout`, the receiver sends
  `RESEND`; the sender retransmits the chunk on the stream that carried the
  request.
- If a connection drops or stays idle beyond `--conn-timeout`, it is closed and
  re-established on both sides; the sender repeats `HELLO` on the new
  connection.

## Automated testing note

The receiver supports the `MULTISEND_TEST_DROP_SEQS` test hook (comma-separated
sequence numbers): the listed chunks are dropped once (no storage, no ACK) to
exercise the retransmission path. It is meant exclusively for automated tests.

```sh
MULTISEND_TEST_DROP_SEQS=200 multisend --receiver --connect 127.0.0.1:9000
```

## Documentation in other languages

- [Italiano (Italian)](README.IT.md)