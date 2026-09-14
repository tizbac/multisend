# multisend

Multiplexer parallelo di stream TCP per dati su pipe.

`multisend` legge dati da stdin, li suddivide in chunk numerati e li distribuisce
su **N stream TCP paralleli** in ordine round-robin. Il ricevitore riassembla i
chunk in ordine e li scrive su stdout. Progettato per essere usato in pipe tra
comandi come `zfs send`/`zfs recv`.

Da un lato della pipe:

```sh
zfs send pool/dataset | multisend --sender --listen :9000 --streams 4
```

Dall'altro:

```sh
multisend --receiver --connect macchinaA:9000 --streams 4 | zfs recv pool/dataset
```

## Caratteristiche

- Distribuzione round-robin su N stream TCP
- Numerazione di sequenza dei chunk a 64 bit
- Buffer in memoria con eviction basata su ACK
- Richiesta automatica di ritrasmissione dei chunk mancanti
- Ristabilimento automatico delle connessioni in caso di caduta
- **Reporting live a 1 intervallo di 1 secondo**: velocità di ogni connessione
  e velocità totale, mostrate in una **finestra centrata sulla larghezza del
  terminale**; a ogni aggiornamento il cursore torna all'inizio della finestra
  e il vecchio contenuto viene riscritto al suo posto, senza riempire il buffer
  di scorrimento del terminale
- **Interfaccia di avanzamento opzionale**: la si può attivare o disattivare
  con il flag `--progress`

## Formato del protocollo

Ogni messaggio in transito ha la forma:

```
[1 byte tipo][8 byte seq (big-endian)][4 byte datalen (big-endian)][dati...]
```

Tipi di messaggio: `DATA(1)`, `ACK(2)`, `RESEND(3)`, `DONE(4)`, `HELLO(5)`.

Il messaggio `HELLO`, inviato dal mittente all'apertura di ogni connessione
(anche quella ripristinata dopo una caduta), pubblicizza la dimensione dei
chunk scelta dal lato mittente; il ricevitore la utilizza per calcolare il
numero totale di chunk.

## Installazione

Versione minima di Go: **1.21**.

```sh
go build -o multisend .
# in alternativa, installa nel GOPATH/bin
go install .
```

## Utilizzo

```
multisend --sender --listen [ADDR:PORTA_BASE] [--streams N] [--chunk-size N] \
          [--resend-timeout D] [--conn-timeout D] [--progress]
multisend --receiver --connect [HOST:PORTA_BASE] [--streams N] \ 
          [--resend-timeout D] [--conn-timeout D] [--progress]
```

Il mittente mette in ascolto le porte `base..base+N-1`; il ricevitore si
connette a ciascuna di esse. La porta passata a `--listen`/`--connect` è la
**base** per gli N stream.

Caso d'uso reale (due macchine):

```sh
# Macchina A (mittente)
zfs send pool/dataset | multisend --sender --listen :9000 --streams 4

# Macchina B (ricevitore)
multisend --receiver --connect macchinaA:9000 --streams 4 | zfs recv pool/dataset
```

Test locale:

```sh
dd if=/dev/urandom bs=1M count=100 \
  | multisend --sender --listen :9000 \
  | multisend --receiver --connect 127.0.0.1:9000 \
  | md5sum
```

## Opzioni

| Flag | Descrizione |
| --- | --- |
| `--sender` | Modalità mittente (legge stdin, distribuisce sugli stream TCP) |
| `--receiver` | Modalità ricevitore (riassembla dagli stream, scrive stdout) |
| `--listen ADDR` | Indirizzo di ascolto del mittente (default: `0.0.0.0:9000`) |
| `--connect ADDR` | Indirizzo a cui connettersi come ricevitore (obbligatorio in modalità ricevitore) |
| `--streams N` | Numero di stream TCP paralleli (default: 4) |
| `--chunk-size N` | Dimensione dei chunk in byte, solo mittente (default: 65536 = 64 KiB). Pubblicizzata al ricevitore tramite HELLO; ignorata da `--receiver` |
| `--resend-timeout D` | Tempo di attesa prima di richiedere la ritrasmissione di un chunk mancante (default: 3s) |
| `--conn-timeout D` | Timeout di inattività per connessione; uno stream senza traffico per questo periodo è considerato morto e viene ucciso e ricreato (default: 30s, `0` per disabilitare) |
| `--progress` | Mostra l'interfaccia di avanzamento live su stderr (default: `true`; `--progress=false` per disattivarla) |
| `--version` | Mostra la versione ed esce |

## Interfaccia di avanzamento

- Aggiornata **una volta al secondo**.
- Mostra la **velocità di ogni connessione** e la **velocità totale**,
  insieme ai byte trasferiti e al tempo trascorso.
- Quando stderr è un terminale, l'output è disegnato come una **finestra
  centrata** di larghezza proporzionata alla dimensione del terminale. A ogni
  refresh il cursore viene riportato al vertice della finestra e le righe
  vengono riscritte nello stesso posto: il buffer di scorrimento del terminale
  non cresce, anche per trasferimenti molto lunghi.
- Quando stderr **non** è un terminale (es. output reindirizzato in un file),
  ogni secondo viene stampata una singola riga di testo semplice, sicura per
  log e pipe.
- Con `--progress=false` l'interfaccia è del tutto silenziosa.

### Esempio dell'aspetto a terminale (con `--streams 4`)

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

## Affidabilità

- Ogni chunk inviato viene mantenuto in un buffer fino alla ricezione
  dell'ACK da parte del ricevitore.
- I chunk fuori ordine vengono conservati dal ricevitore finché il gap non
  viene colmato, poi scritti in sequenza su stdout.
- Se un chunk risulta mancante oltre `--resend-timeout`, il ricevitore invia
  `RESEND`; il mittente ritrasmette il chunk sullo stream che ha portato la
  richiesta.
- Se una connessione cade o resta inattiva oltre `--conn-timeout`, viene
  chiusa e ristabilita su entrambi i lati; il mittente ripete `HELLO` sulla
  nuova connessione.

## Cifratura dello stream con OpenSSL

`multisend` tratta input/output come byte opachi, quindi il modo più semplice
per mantenere confidenziale un trasferimento è convogliare i dati attraverso
`openssl enc` — prima che entrino nel mittente e di nuovo dopo che escono dal
ricevitore — usando una passphrase condivisa fissa:

```sh
# Macchina A (mittente)
zfs send pool/dataset \
  | openssl enc -aes-256-cbc -pbkdf2 -iter 100000 -k 'la tua passphrase condivisa' \
  | multisend --sender --listen :9000 --streams 4

# Macchina B (ricevitore)
multisend --receiver --connect macchinaA:9000 --streams 4 \
  | openssl enc -d -aes-256-cbc -pbkdf2 -iter 100000 -k 'la tua passphrase condivisa' \
  | zfs recv pool/dataset
```

Note:

- Usa la **stessa passphrase, algoritmo e valore di `-iter`** su entrambi i
  lati. `-pbkdf2` (OpenSSL 1.1.1+) deriva la chiave con un KDF salato invece
  del legacy `EVP_BytesToKey`, e richiede lo stesso flag in decrittazione.
- Il salt viene generato di nuovo a ogni esecuzione e incorporato
  nell'intestazione dell'output: input identico produce comunque cifratura
  diversa, e la decrittazione non richiede coordinamento aggiuntivo.
- La passphrase non viene mai trasmessa da `multisend`: la cifratura avviene
  sul flusso di byte *prima* del multiplexing, quindi sulle connessioni TCP
  viaggia solo il ciphertext. La numerazione interna dei chunk non rivela il
  plaintext.
- Esempio di verifica locale:

  ```sh
  dd if=/dev/urandom bs=1M count=100 \
    | openssl enc -aes-256-cbc -pbkdf2 -iter 100000 -k 'secret' \
    | multisend --sender --listen :9000 \
    | multisend --receiver --connect 127.0.0.1:9000 \
    | openssl enc -d -aes-256-cbc -pbkdf2 -iter 100000 -k 'secret' \
    | md5sum
  ```

- Note di sicurezza: con una passphrase condivisa fissa, la sicurezza dipende
  dalla robustezza della passphrase e dal costo del KDF — aumenta `-iter`
  (default 10000, es. `100000` o più a seconda della macchina) di
  conseguenza. Una passphrase passata con `-k` è visibile nella lista dei
  processi (`ps`); dove conta, preferisci `-pass env:PASS` (da una variabile
  d'ambiente) o `-pass file:<percorso>`, così la passphrase non fa parte della
  riga di comando. Sul filo non c'è plaintext, ma questo non autentica le
  parti: protegge solo la confidenzialità.

## Nota sui test automatici

Il ricevitore supporta l'hook di test `MULTISEND_TEST_DROP_SEQS` (numeri di
sequenza separati da virgola): i chunk indicati vengono scartati una volta
(nessuno storage, nessun ACK) per esercitare il percorso di ritrasmissione.
È pensato esclusivamente per i test automatizzati.

```sh
MULTISEND_TEST_DROP_SEQS=200 multisend --receiver --connect 127.0.0.1:9000
```

## Documentazione in altre lingue

- [English](README.md)