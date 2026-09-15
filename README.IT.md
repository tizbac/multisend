# multisend

Multiplexer parallelo di stream TCP per dati su pipe.

`multisend` legge dati da stdin, li suddivide in chunk numerati e li distribuisce
su **N stream TCP paralleli**. Il lato ricevente riassembla i chunk in ordine e
li scrive su stdout. Progettato per essere usato in pipe tra comandi come
`zfs send`/`zfs recv`. Il sistema è **bidirezionale**: ogni nodo esegue sia la
metà di trasmissione (stdin→rete) sia quella di ricezione (rete→stdout). Il nodo
resta attivo finché una delle due direzioni ha ancora dati; esce solo quando
entrambe le direzioni sono completate (contratto "stay-up").

## Caratteristiche

- Flusso dati bidirezionale: ogni nodo esegue `outFlow` (stdin→rete) e
  `inFlow` (rete→stdout) sulle stesse connessioni
- Distribuzione dei chunk sugli N stream TCP in modalità `round-robin`
  (default) o `least-unacked` (il prossimo chunk va allo stream con meno chunk
  non ancora confermati); gli stream down sono sempre saltati
- Numerazione di sequenza dei chunk a 64 bit con framing STREAMID per il
  routing per-stream
- Buffer in memoria con eviction basata su ACK
- Richiesta automatica di ritrasmissione dei chunk mancanti
- Ristabilimento automatico delle connessioni in caso di caduta; gli stream
  marcati down vengono saltati e ripristinati dopo il timeout di inattività
- **Reporting live, aggiornato una volta al secondo**: velocità di ogni
  connessione più velocità totale, mostrate in una **finestra bianca centrata**
  (testo nero) che **cresce ma non si restringe mai**; a ogni aggiornamento il
  cursore torna all'inizio della finestra e il contenuto viene riscritto al suo
  posto, senza riempire il buffer di scorrimento del terminale
- **Interfaccia di avanzamento opzionale**: la si può attivare o disattivare
  con il flag `--progress`
- **Modalità single-port** (default true): tutti gli N stream condividono una
  sola porta; il routing avviene tramite frame `STREAMID`. Con
  `--single-port=false` ogni stream usa porta_base + indice

## Formato del protocollo

Ogni messaggio in transito ha la forma:

```
[1 byte tipo][8 byte seq (big-endian)][4 byte datalen (big-endian)][dati...]
```

Tipi di messaggio: `DATA(1)`, `ACK(2)`, `RESEND(3)`, `DONE(4)`, `HELLO(5)`,
`STREAMID(6)`.

Il messaggio `STREAMID` porta l'indice dello stream (nel campo seq) ed è il
primo frame inviato da un nodo al momento di una nuova connessione; il listener
lo usa per instradare il socket allo slot dello stream corretto (così funziona
il multiplexing single-port).

Il messaggio `HELLO`, inviato da un nodo all'apertura di ogni connessione
(anche quella ripristinata dopo una caduta), pubblicizza la dimensione dei
chunk scelta dal lato mittente; il ricevente la utilizza per calcolare il
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
multisend --listen ADDR [--streams N] [--chunk-size N] \
          [--resend-timeout D] [--conn-timeout D] [--single-port bool] \
          [--queue-mode M] [--conn-lifetime D] [--progress]
multisend --connect ADDR [--streams N] [--chunk-size N] \
          [--resend-timeout D] [--conn-timeout D] [--single-port bool] \
          [--queue-mode M] [--conn-lifetime D] [--progress]
```

Deve essere indicata esattamente una tra `--listen` e `--connect`.
`--single-port` di default è `true` (tutti gli N stream condividono una porta,
identificati dai frame `STREAMID`); con `--single-port=false` ogni stream usa
porta_base + indice.

Caso d'uso reale (due macchine):

```sh
# Macchina A
zfs send pool/dataset | multisend --listen :9000 --streams 4

# Macchina B
multisend --connect macchinaA:9000 --streams 4 | zfs recv pool/dataset
```

Test locale (due pipeline separate; NON concatenare `dd | A | B | md5sum`
perché il loop di feedback bidirezionale andrebbe in attesa):

```sh
# Pipeline A
dd if=/dev/urandom bs=1M count=100 \
  | multisend --listen :9000 --streams 1 >/dev/null 2>/tmp/ms_A.log &

# Pipeline B
multisend --connect 127.0.0.1:9000 --streams 1 </dev/null >/tmp/ms_B.out 2>/tmp/ms_B.log
# verifica: md5sum
```

## Opzioni

| Flag | Descrizione |
| --- | --- |
| `--listen ADDR` | Indirizzo di ascolto (ruolo server); esattamente una tra `--listen`/`--connect` |
| `--connect ADDR` | Indirizzo a cui connettersi (ruolo client) |
| `--single-port` | Usa una sola porta per tutti gli N stream (default: `true`); con `false` ogni stream usa porta_base + indice |
| `--streams N` | Numero di stream TCP paralleli (default: 4) |
| `--chunk-size N` | Dimensione dei chunk in byte (default: 65536 = 64 KiB). Pubblicizzata al peer via HELLO |
| `--resend-timeout D` | Tempo di attesa prima di richiedere la ritrasmissione di un chunk mancante (default: 3s) |
| `--conn-timeout D` | Timeout di inattività per connessione; uno stream senza traffico per questo periodo è considerato morto e viene ristabilito (default: 30s, `0` per disabilitare) |
| `--conn-lifetime D` | Durata massima della connessione lato client; allo scadere di questo valore tutti gli stream vengono chiusi e ricostruiti anche se ancora attivi (default: `0` = mantenerli per sempre) |
| `--queue-mode M` | Modalità di coda dei chunk: `round-robin` (default) o `least-unacked` (il prossimo chunk va allo stream con meno chunk non ancora confermati) |
| `--progress` | Mostra l'interfaccia di avanzamento live su stderr (default: `true`; `--progress=false` per disattivarla) |
| `--version` | Mostra la versione ed esce |

## Interfaccia di avanzamento

- Aggiornata **una volta al secondo**.
- Mostra la **velocità di ogni connessione** e la **velocità totale**, insieme
  ai byte trasferiti e al tempo trascorso.
- Quando stderr è un terminale, l'output è disegnato come una **finestra bianca
  centrata** (testo nero) che **cresce ma non si restringe mai**: la larghezza
  è quella del contenuto più largo mai mostrato, più i bordi. A ogni refresh il
  cursore viene riportato al vertice e le righe riscritte nello stesso posto:
  il buffer di scorrimento non cresce, anche per trasferimenti molto lunghi.
- Ogni riga connessione mostra uno **stato** (`OK` verde grasso = traffico
  recente entro `--conn-timeout/5`, `STL` giallo grasso = inattivo più a lungo,
  `DISCONN` rosso grasso = down/in ripristino) e una **barra di peso**: la
  quota di chunk inviati dallo stream sul totale di tutti gli stream, con la
  percentuale.
- Le barre `pend` (magenta), `buff` (verde) e `miss` (rosso) sono nei propri
  colori; le velocità di invio per stream sono rosse, di ricezione verdi.
- Quando stderr **non** è un terminale (es. reindirizzato in un file), ogni
  secondo viene stampata una singola riga di testo semplice (le velocità di
  invio per stream includono il conteggio chunk `xN`), sicura per log e pipe.
- Con `--progress=false` l'interfaccia è del tutto silenziosa.

### Esempio dell'aspetto a terminale (con `--streams 2`, modalità least-unacked)

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

## Affidabilità

- Ogni chunk inviato viene mantenuto in un buffer fino alla ricezione dell'ACK.
- I chunk fuori ordine vengono conservati dal ricevente finché il gap non viene
  colmato, poi scritti in sequenza su stdout.
- Se un chunk risulta mancante oltre `--resend-timeout`, il ricevente invia
  `RESEND`; il mittente ritrasmette il chunk sullo stream che ha portato la
  richiesta.
- Se una connessione cade o resta inattiva oltre `--conn-timeout`, viene
  chiusa e ristabilita su entrambi i lati; il nodo ripete `HELLO` sulla nuova
  connessione.
- Quando `--conn-lifetime` è impostato (solo lato client), ogni stream viene
  chiuso con forza e ricostruito all'intervallo stabilito anche se tutte le
  connessioni sono sane, per eseguire una rotazione periodica delle
  connessioni.

## Cifratura dello stream con OpenSSL

`multisend` tratta input/output come byte opachi, quindi il modo più semplice
per mantenere confidenziale un trasferimento è convogliare i dati attraverso
`openssl enc` — prima che entrino in `multisend` e di nuovo dopo che ne
escono — usando una passphrase condivisa fissa:

```sh
# Macchina A (in ascolto)
zfs send pool/dataset \
  | openssl enc -aes-256-cbc -pbkdf2 -iter 100000 -k 'la tua passphrase condivisa' \
  | multisend --listen :9000 --streams 4

# Macchina B (client)
multisend --connect macchinaA:9000 --streams 4 \
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
- Verifica locale (due pipeline separate; concatenare listen e connect nella
  stessa pipe creerebbe un loop di feedback bidirezionale e andrebbe in
  attesa):

  ```sh
  dd if=/dev/urandom bs=1M count=100 > /tmp/plain.bin
  openssl enc -aes-256-cbc -pbkdf2 -iter 100000 -k 'secret' -in /tmp/plain.bin -out /tmp/cipher.bin
  (multisend --listen :9000 < /tmp/cipher.bin > /dev/null) &
  multisend --connect 127.0.0.1:9000 </dev/null > /tmp/cipher.out
  openssl enc -d -aes-256-cbc -pbkdf2 -iter 100000 -k 'secret' -in /tmp/cipher.out -out /tmp/plain.out
  cmp /tmp/plain.bin /tmp/plain.out
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

Il lato ricevente supporta l'hook di test `MULTISEND_TEST_DROP_SEQS` (numeri
di sequenza separati da virgola): i chunk indicati vengono scartati una volta
(nessuno storage, nessun ACK) per esercitare il percorso di ritrasmissione. È
pensato esclusivamente per i test automatizzati.

```sh
MULTISEND_TEST_DROP_SEQS=200 multisend --connect 127.0.0.1:9000
```

## Documentazione in altre lingue

- [English](README.md)
