# multisend

Parallel TCP stream multiplexer for piped data.

`multisend` reads data from stdin, splits it into numbered chunks, and
distributes them across **N parallel TCP streams** in round-robin order. The
receiver reassembles the chunks in order and writes them to stdout. Designed
for piping between commands like `zfs send`/`zfs recv`. The system is
**bidirectional**: each endpoint runs both a transmit half (stdin→network) and
a receive half (network→stdout). The node stays up as long as either direction
has data; it only exits when both directions have completed (the "stay-up"
contract).

## Features

- Bidirectional data flow: every node runs `outFlow` (stdin→network) and
  `inFlow` (network→stdout) over the same connections
- Chunk distribution across N TCP streams in `round-robin` order (default) or
  `least-unacked` mode (next chunk goes to the stream with the fewest
  unacked chunks); down streams are always skipped
- 64-bit chunk sequence numbering with STREAMID framing for per-stream routing
- In-memory chunk buffer with ACK-based eviction
- Automatic retransmission requests for missing chunks
- Automatic connection reestablishment on drops; streams marked down are
  skipped and restored after idle timeout
- **Live reporting, refreshed once per second**: speed of every connection
  plus total speed, shown in a **box centered on the terminal width**; on each
  refresh the cursor returns to the top of the box and the previous content is
  rewritten in place, so the terminal scroll-back buffer is never exhausted,
  even on very long transfers
- **Optional progress UI**: turn it on or off with the `--progress` flag
- **Single-port mode** (default true): all N streams share one listening/connect
  port; multiplexed by `STREAMID` frames. Use `--single-port=false` for legacy
  per-stream base+i ports

## Wire protocol

Every on-the-wire message has the shape:

```
[1 byte type][8 byte seq (big-endian)][4 byte datalen (big-endian)][data...]
```

Message types: `DATA(1)`, `ACK(2)`, `RESEND(3)`, `DONE(4)`, `HELLO(5)`,
`STREAMID(6)`.

The `STREAMID` message carries the stream index (in the seq field) and is the
very first frame a connecting node sends on a fresh socket; the listener uses
it to route the socket to the right stream slot (this is what makes single-port
multiplexing work).

The `HELLO` message, sent by a node when each connection opens (including a
re-established one after a drop), advertises the chunk size chosen on the
sending side; the receiver uses it to compute the total chunk count.

## Installation

Minimum Go version: **1.21**.

```sh
go build -o multisend .
# alternatively, install into GOPATH/bin
go install .
```

## Usage

```
multisend --listen ADDR [--streams N] [--chunk-size N] \
          [--resend-timeout D] [--conn-timeout D] [--single-port bool] [--queue-mode M] [--progress]
multisend --connect ADDR [--streams N] [--chunk-size N] \
          [--resend-timeout D] [--conn-timeout D] [--single-port bool] [--queue-mode M] [--progress]
```

Exactly one of `--listen` or `--connect` must be given. `--single-port` defaults to
`true` (all N streams share one port, identified by `STREAMID` frames); use
`--single-port=false` for legacy per-stream base+i ports.

Real-world use (two machines):

```sh
# Machine A (transmit half)
zfs send pool/dataset | multisend --listen :9000 --streams 4

# Machine B (receive half)
multisend --connect machineA:9000 --streams 4 | zfs recv pool/dataset
```

Local test (two separate pipelines; do NOT chain `dd | A | B | md5sum`
because the bidirectional feedback loop hangs):

```sh
# Pipeline A
dd if=/dev/urandom bs=1M count=100 \
  | multisend --listen :9000 --streams 1 >/dev/null 2>/tmp/ms_A.log &

# Pipeline B
multisend --connect 127.0.0.1:9000 --streams 1 </dev/null >/tmp/ms_B.out 2>/tmp/ms_B.log
# verify: md5sum /dev/urandom-bs1M-count100.bin /tmp/ms_B.out
```

## Options

| Flag | Description |
| --- | --- |
| `--listen ADDR` | Listen address (server role); exactly one of `--listen`/`--connect` |
| `--connect ADDR` | Address to connect to (client role) |
| `--single-port` | Use one port for all N streams (default: `true`); with `false`, each stream uses base port + stream index |
| `--streams N` | Number of parallel TCP streams (default: 4) |
| `--chunk-size N` | Chunk size in bytes (default: 65536 = 64 KiB). Advertised to the peer via HELLO |
| `--resend-timeout D` | Timeout before requesting retransmission of a missing chunk (default: 3s) |
| `--conn-timeout D` | Idle timeout per connection; a stream with no traffic for this long is deemed dead and re-established (default: 30s, `0` to disable) |
| `--queue-mode M` | Chunk queueing mode: `round-robin` (default) or `least-unacked` (next chunk goes to the stream with the fewest unacked chunks) |
| `--progress` | Show live progress box on stderr (default: `true`; `--progress=false` to disable) |
| `--version` | Show version and exit |

## Progress UI

- Refreshed **once per second**.
- Shows the **speed of every connection** and the **total speed**, together
  with the transferred bytes and elapsed time.
- When stderr is a terminal, the output is drawn as a **centered white box**
  (black text) that **only grows, never shrinks**: its width is the widest
  content ever shown plus the borders. On every refresh the cursor is returned
  to the top of the box and the rows are rewritten in place: the terminal
  scroll-back buffer does not grow, even for very long transfers.
- Each connection line shows a **status** (`OK` bold green = traffic within
  `--conn-timeout/5`, `STL` bold yellow = idle longer, `DISCONN` bold red =
  down/being restored) and a **weight bar**: the stream's share of the total
  chunks sent over all streams, with the percentage.
- The `pend` (magenta), `buff` (green) and `miss` (red) bars are drawn in
  their own colours; per-stream send rates are red, receive rates green.
- When stderr is **not** a terminal (e.g. redirected into a file), a single
  plain text line is printed each second (per-stream send rates include the
  per-stream chunk count `xN`), safe for logs and pipes.
- With `--progress=false` the UI is completely silent.

### Terminal appearance example (with `--streams 2`, least-unacked mode)

```
            ┌────────────────────────────────────────────────────────────┐
            │ RUN  SEND 2.00 MiB @ 2.50 MiB/s    RECV 0 B @ 0 B/s    1.0s │
            │  conn  1  OK       ▓▓▓▓▓▓▓░░░  66%  S 1.25M  R 0.0K       │
            │  conn  2  STL      ▓▓▓░░░░░░░  33%  S 0.62M  R 0.0K       │
            │  pend ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ 24                                  │
            │  buff ░░░░░░░░░░░░░░ 0                                     │
            │  miss ░░░░░░░░░░░░░░ 0                                     │
            └────────────────────────────────────────────────────────────┘
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

## Encrypting the stream with OpenSSL

`multisend` treats its input/output as opaque bytes, so the simplest way to
keep a transfer confidential end-to-end is to pipe the data through
`openssl enc` — before it enters the sender and again after it leaves the
receiver — using a fixed shared passphrase:

```sh
# Machine A (listening)
zfs send pool/dataset \
  | openssl enc -aes-256-cbc -pbkdf2 -iter 100000 -k 'your shared passphrase' \
  | multisend --listen :9000 --streams 4

# Machine B (connecting)
multisend --connect machineA:9000 --streams 4 \
  | openssl enc -d -aes-256-cbc -pbkdf2 -iter 100000 -k 'your shared passphrase' \
  | zfs recv pool/dataset
```

Notes:

- Use the **same algorithm, passphrase and `-iter` value** on both sides.
  `-pbkdf2` (OpenSSL 1.1.1+) derives the key with a salted KDF instead of the
  legacy `EVP_BytesToKey`, and requires the same flag when decrypting.
- The salt is generated fresh on every encryption run and embedded in the
  output header, so identical input still produces different ciphertext, and
  decryption needs no extra coordination.
- The passphrase is never transmitted by `multisend`: encryption happens on the
  byte stream *before* multiplexing, so only ciphertext travels over the TCP
  streams. The internal chunk numbering does not leak the plaintext.
- Local verification (two separate pipelines; chaining listen and connect in
  one pipeline would create a bidirectional feedback loop and hang):

  ```sh
  dd if=/dev/urandom bs=1M count=100 > /tmp/plain.bin
  openssl enc -aes-256-cbc -pbkdf2 -iter 100000 -k 'secret' -in /tmp/plain.bin -out /tmp/cipher.bin
  (multisend --listen :9000 < /tmp/cipher.bin > /dev/null) &
  multisend --connect 127.0.0.1:9000 </dev/null > /tmp/cipher.out
  openssl enc -d -aes-256-cbc -pbkdf2 -iter 100000 -k 'secret' -in /tmp/cipher.out -out /tmp/plain.out
  cmp /tmp/plain.bin /tmp/plain.out
  ```

- Security notes: with a fixed shared passphrase, security depends on the
  passphrase strength and the KDF cost — raise `-iter` (default 10000, e.g.
  `100000` or higher) on slow machines accordingly. A passphrase passed as
  `-k` is visible in the process list (`ps`); prefer `-pass env:PASS` (from an
  env var) or `-pass file:<path>` where that matters, so the passphrase is not
  part of the command line. There is no plaintext on the wire, but this does
  not authenticate the endpoints: it protects confidentiality only.

## Automated testing note

The receiver supports the `MULTISEND_TEST_DROP_SEQS` test hook (comma-separated
sequence numbers): the listed chunks are dropped once (no storage, no ACK) to
exercise the retransmission path. It is meant exclusively for automated tests.

```sh
MULTISEND_TEST_DROP_SEQS=200 multisend --connect 127.0.0.1:9000
```

## Documentation in other languages

- [Italiano (Italian)](README.IT.md)