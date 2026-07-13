# nusadb-go — Go `database/sql` driver for NusaDB

A pure-Go, dependency-free driver that speaks the
[Nusa Wire Protocol](../../docs/wire-protocol.md) (`PROTOCOL_VERSION 1.1`) directly
over TCP. No cgo, no third-party modules (SCRAM-SHA-256 uses the Go 1.24 stdlib
`crypto/pbkdf2`). `sql.ColumnType.DatabaseTypeName()` reports each column's NusaDB
type name (protocol 1.1).

## Install

```bash
go get github.com/nusadb/go
```

Requires Go 1.24+.

## Usage

```go
import (
	"database/sql"
	_ "github.com/nusadb/go" // registers the "nusadb" driver
)

db, err := sql.Open("nusadb", "nusadb://nusa-root@127.0.0.1:5678/nusadb")
if err != nil {
	log.Fatal(err)
}
defer db.Close()

_, err = db.Exec("CREATE TABLE t (id INT NOT NULL, name TEXT)")
_, err = db.Exec("INSERT INTO t VALUES ($1, $2)", 1, "alice")

var name string
err = db.QueryRow("SELECT name FROM t WHERE id = $1", 1).Scan(&name)
```

### Value types

Each cell decodes to a typed `database/sql/driver.Value` by the column's protocol 1.1 type
tag: `BOOL` → `bool`, `INT` → `int64`, `FLOAT` → `float64`, `DATE`/`TIMESTAMP`/`TIMESTAMPTZ` →
`time.Time`, `BYTEA` → `[]byte`. `NUMERIC`, `UUID`, `TIME`, `INTERVAL`, `JSON`, `ARRAY`, and
`TEXT` have no distinct `driver.Value` and come back as `string`. `Scan` still converts to your
destination type either way; the typing matters when scanning into `interface{}`. A value that
does not parse as its tag falls back to the raw string, so an unexpected wire form never fails
the scan.

### DSN

```
nusadb://user:password@host:port/database
```

Defaults: host `127.0.0.1`, port `5678`, user `nusadb`, database `nusadb`.

### Parameters

Placeholders are positional **`$1`, `$2`, …**. Pass the values to `Exec`/`Query`;
`nil` is SQL `NULL`. Values are sent in the wire text format. `time.Time` is
formatted as `2006-01-02 15:04:05.999999`.

### Authentication

When the server runs with `--auth-user USER:PASSWORD`, put the password in the DSN
userinfo. The driver performs SCRAM-SHA-256 and verifies the server's signature
(mutual auth, constant-time).

### Batch (bulk insert/update)

`database/sql` has no batch interface, so the package provides `nusadb.ExecMany(db, sql, argsList)`:
it runs `sql` once per argument set, reusing a single prepared statement, and returns the per-set
`RowsAffected` counts. The wire protocol has no batch pipeline, so this is N round-trips, not one.

```go
counts, err := nusadb.ExecMany(db, "INSERT INTO t VALUES ($1, $2)", [][]any{
	{1, "a"}, {2, "b"}, {3, "c"},
})
```

### Bulk load / export (`COPY`)

For high-throughput load/export, `CopyIn` / `CopyTo` drive the `COPY` sub-protocol — one round-trip
for the whole dataset. `database/sql` has no COPY interface, so they take a checked-out `*sql.Conn`
(`db.Conn(ctx)`). Move bytes in the server's text format (tab-delimited fields, `\N` for SQL `NULL`,
one row per line); you write the `COPY` statement with any `WITH (...)` options.

```go
sc, _ := db.Conn(ctx)
defer sc.Close()

loaded, err := nusadb.CopyIn(sc, "COPY t (id, name) FROM STDIN",
	strings.NewReader("1\talice\n2\t\\N\n"))

var buf bytes.Buffer
exported, err := nusadb.CopyTo(sc, "COPY t TO STDOUT", &buf)
```

A `COPY` the server refuses (bad SQL, an RLS-protected table) returns an error; the connection stays usable.

### Context cancellation

`QueryContext`/`ExecContext` honour context cancellation: if the context is
cancelled while a statement runs, the driver opens a side connection and sends a
`CancelRequest`, so the server aborts the statement (`docs/wire-protocol.md` §13).

## Transactions

Statements autocommit unless wrapped in an explicit transaction. `db.Begin()` (and
`db.BeginTx`) send `BEGIN`, and the returned `*sql.Tx` commits or rolls back with
`COMMIT`/`ROLLBACK`. Isolation-level and read-only options passed to `BeginTx` are not
yet honoured (the server uses its default). `LastInsertId` is unsupported; use
`RETURNING` or a sequence.

```go
tx, _ := db.Begin()
tx.Exec("INSERT INTO t VALUES (1)")
tx.Commit() // or tx.Rollback()
```

Savepoints work through the same `*sql.Tx` — `database/sql` has no dedicated savepoint API, so
issue them as statements:

```go
tx, _ := db.Begin()
tx.Exec("INSERT INTO t VALUES (1)")
tx.Exec("SAVEPOINT sp1")
tx.Exec("INSERT INTO t VALUES (2)")
tx.Exec("ROLLBACK TO SAVEPOINT sp1") // undoes (2), keeps (1); the transaction continues
tx.Commit()
```

## Notifications (LISTEN/NOTIFY)

`database/sql` has no notification API, so reach it through `sql.Conn.Raw` and the `Notifier`
interface. `Listen(channel)` subscribes; a `Notify(channel, payload)` from any connection on the same
database is delivered asynchronously. `Poll(timeout)` waits for the next one (0 blocks), or
`Notifications()` drains those buffered during other queries:

```go
conn, _ := db.Conn(ctx)
conn.Raw(func(dc any) error {
    nc := dc.(nusadb.Notifier)
    _ = nc.Listen("orders")
    note, _ := nc.Poll(5 * time.Second) // -> *Notification{PID, Channel, Payload}, or nil on timeout
    fmt.Println(note.Channel, note.Payload)
    return nc.Unlisten("orders")
})
```

## TLS

The server uses **implicit TLS 1.3** (start it with `--tls-cert`/`--tls-key`). Enable it from the
DSN with the `tls` parameter:

```
nusadb://user@host:5678/nusadb?tls=true          # verify against the system roots + host name
nusadb://user@host:5678/nusadb?tls=skip-verify   # encrypt without verification (development only)
nusadb://user@host:5678/nusadb?tls=mycfg         # use a config registered with RegisterTLSConfig
```

For a custom CA, a client certificate (mTLS), or a pinned server name, register a `*tls.Config`
before opening the connection and reference it by name:

```go
nusadb.RegisterTLSConfig("mycfg", &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}})
db, _ := sql.Open("nusadb", "nusadb://user@host:5678/nusadb?tls=mycfg")
```

`tls` is absent (plaintext) by default.

## Tests

```bash
cargo build -p nusadb-server          # the tests boot this binary
cd drivers/go && go test ./...
```

The tests boot a real `nusadb-server` on an ephemeral port (honouring
`CARGO_TARGET_DIR`) and exercise queries, parameters, prepared statements, errors,
transaction commit/rollback, and SCRAM auth.

## License

Apache-2.0.
