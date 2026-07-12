// Package nusadb is a database/sql driver for NusaDB. It speaks the Nusa Wire
// Protocol (docs/wire-protocol.md, PROTOCOL_VERSION 1.1) directly over TCP with
// no cgo and no third-party dependencies.
//
// Register name: "nusadb". DSN (URL form):
//
//	nusadb://user:password@host:port/database
//
// Example:
//
//	db, err := sql.Open("nusadb", "nusadb://nusa-root@127.0.0.1:5678/nusadb")
//	rows, err := db.Query("SELECT id, name FROM t WHERE id = $1", 1)
//
// Placeholders are positional $1, $2, … (the server's native marker).
//
// Transactions: statements autocommit unless wrapped in an explicit transaction.
// db.Begin / db.BeginTx send BEGIN, and the returned Tx commits or rolls back with
// COMMIT/ROLLBACK over the same connection. Isolation-level and read-only options
// passed to BeginTx are not yet honoured; the server uses its default.
package nusadb

import (
	"bufio"
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func init() {
	sql.Register("nusadb", &Driver{})
}

// Preparer is implemented by *sql.DB, *sql.Conn, and *sql.Tx — anything that can prepare a
// statement, so ExecMany can run inside or outside an explicit transaction.
type Preparer interface {
	Prepare(query string) (*sql.Stmt, error)
}

// ExecMany runs query once per argument set, reusing a single prepared statement — the bulk
// insert/update path. It returns the per-set RowsAffected counts. The Nusa wire protocol has no
// batch pipeline, so this performs N round-trips, not one; the first failing set returns the counts
// gathered so far together with its error. database/sql has no batch interface, so this is the
// idiomatic NusaDB helper for bulk DML.
func ExecMany(p Preparer, query string, argsList [][]any) ([]int64, error) {
	stmt, err := p.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stmt.Close() }()
	counts := make([]int64, 0, len(argsList))
	for _, args := range argsList {
		res, err := stmt.Exec(args...)
		if err != nil {
			return counts, err
		}
		n, _ := res.RowsAffected()
		counts = append(counts, n)
	}
	return counts, nil
}

// CopyIn bulk-loads via COPY ... FROM STDIN over a checked-out *sql.Conn (get one with
// db.Conn(ctx)). src yields the server's text format (tab-delimited fields, \N for SQL NULL, one row
// per line); you write the COPY statement with any WITH (...) options. Returns the rows loaded.
// database/sql has no COPY interface, so this reaches the driver connection via (*sql.Conn).Raw.
func CopyIn(sc *sql.Conn, query string, src io.Reader) (int64, error) {
	var n int64
	err := sc.Raw(func(dc any) error {
		cc, ok := dc.(*conn)
		if !ok {
			return errors.New("nusadb: not a nusadb connection")
		}
		var e error
		n, e = cc.copyIn(query, src)
		return e
	})
	return n, err
}

// CopyTo bulk-exports via COPY ... TO STDOUT over a checked-out *sql.Conn, writing the server's
// text-format rows to dst. Returns the rows exported.
func CopyTo(sc *sql.Conn, query string, dst io.Writer) (int64, error) {
	var n int64
	err := sc.Raw(func(dc any) error {
		cc, ok := dc.(*conn)
		if !ok {
			return errors.New("nusadb: not a nusadb connection")
		}
		var e error
		n, e = cc.copyOut(query, dst)
		return e
	})
	return n, err
}

// Error is a structured server error: a 5-character SQLSTATE plus a message
// (docs/wire-protocol.md §14).
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("nusadb: %s: %s", e.Code, e.Message) }

// Config is a parsed DSN.
type Config struct {
	Host     string
	Port     int
	User     string
	Database string
	Password string
	// TLSConfig, when non-nil, wraps the connection in TLS (implicit TLS — the server expects the
	// handshake before any frame). Set it via the DSN `tls` parameter (see ParseDSN).
	TLSConfig *tls.Config
}

var (
	tlsConfigsMu sync.RWMutex
	tlsConfigs   = map[string]*tls.Config{}
)

// RegisterTLSConfig registers a *tls.Config under name so a DSN can select it with `tls=<name>` —
// the way to supply a custom CA, a client certificate (mTLS), or a server name. Passing a nil cfg
// removes the entry. The names "true", "false", and "skip-verify" are reserved.
func RegisterTLSConfig(name string, cfg *tls.Config) error {
	switch name {
	case "", "true", "false", "skip-verify":
		return fmt.Errorf("nusadb: reserved TLS config name %q", name)
	}
	tlsConfigsMu.Lock()
	defer tlsConfigsMu.Unlock()
	if cfg == nil {
		delete(tlsConfigs, name)
	} else {
		tlsConfigs[name] = cfg
	}
	return nil
}

func lookupTLSConfig(name string) (*tls.Config, bool) {
	tlsConfigsMu.RLock()
	defer tlsConfigsMu.RUnlock()
	cfg, ok := tlsConfigs[name]
	return cfg, ok
}

// ParseDSN parses a "nusadb://user:password@host:port/database" URL.
func ParseDSN(dsn string) (*Config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("nusadb: invalid DSN: %w", err)
	}
	if u.Scheme != "nusadb" {
		return nil, fmt.Errorf("nusadb: DSN scheme must be nusadb://, got %q", u.Scheme)
	}
	cfg := &Config{Host: "127.0.0.1", Port: 5678, User: "nusa-root", Password: "nusa-root", Database: "nusadb"}
	if h := u.Hostname(); h != "" {
		cfg.Host = h
	}
	if p := u.Port(); p != "" {
		port, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("nusadb: invalid port %q", p)
		}
		cfg.Port = port
	}
	if u.User != nil {
		if name := u.User.Username(); name != "" {
			cfg.User = name
		}
		if pw, ok := u.User.Password(); ok {
			cfg.Password = pw
		}
	}
	if db := u.Path; len(db) > 1 {
		cfg.Database = db[1:]
	}
	// `tls` selects how the connection is secured: "true" verifies the server against the system
	// roots and the host name; "skip-verify" encrypts without verification (development only); any
	// other value names a config registered with RegisterTLSConfig; "false"/absent stays plaintext.
	switch tlsParam := u.Query().Get("tls"); tlsParam {
	case "", "false":
		// plaintext
	case "true":
		cfg.TLSConfig = &tls.Config{ServerName: cfg.Host, MinVersion: tls.VersionTLS13}
	case "skip-verify":
		cfg.TLSConfig = &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13} //nolint:gosec // explicit opt-in
	default:
		registered, ok := lookupTLSConfig(tlsParam)
		if !ok {
			return nil, fmt.Errorf("nusadb: unknown tls config %q (register it with RegisterTLSConfig)", tlsParam)
		}
		clone := registered.Clone()
		if clone.ServerName == "" {
			clone.ServerName = cfg.Host
		}
		cfg.TLSConfig = clone
	}
	return cfg, nil
}

// Driver implements driver.Driver and driver.DriverContext.
type Driver struct{}

// Open opens a new connection from a DSN.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector returns a connector for the DSN (driver.DriverContext).
func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &connector{cfg: cfg, driver: d}, nil
}

type connector struct {
	cfg    *Config
	driver *Driver
}

func (c *connector) Driver() driver.Driver { return c.driver }

// Connect opens and authenticates a connection.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	var d net.Dialer
	address := net.JoinHostPort(c.cfg.Host, strconv.Itoa(c.cfg.Port))
	netConn, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	// Disable Nagle's algorithm: the wire protocol is request/response, so coalescing
	// small frames trades ~40ms of delayed-ACK latency per round-trip for nothing.
	if tcp, ok := netConn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	// Implicit TLS: a server started with --tls-cert/--tls-key expects the TLS handshake before any
	// protocol frame, so wrap the socket and complete the handshake up front (NoDelay is already set
	// on the underlying TCP socket above).
	if c.cfg.TLSConfig != nil {
		tlsConn := tls.Client(netConn, c.cfg.TLSConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = netConn.Close()
			return nil, fmt.Errorf("nusadb: TLS handshake failed: %w", err)
		}
		netConn = tlsConn
	}
	co := &conn{cfg: c.cfg, netConn: netConn, r: bufio.NewReader(netConn)}
	if err := co.handshake(); err != nil {
		_ = netConn.Close()
		return nil, err
	}
	return co, nil
}

// conn is one authenticated connection.
type conn struct {
	cfg      *Config
	netConn  net.Conn
	r        *bufio.Reader
	pid      uint32
	secret   uint32
	hasKey   bool
	nextName uint64
	badConn  bool
	// Async LISTEN/NOTIFY messages received while reading other responses; drained by Notifications.
	notifications []Notification
}

// Notifier is the asynchronous LISTEN/NOTIFY API of a NusaDB connection. Reach it through the
// standard library with sql.Conn.Raw:
//
//	sqlConn.Raw(func(dc any) error {
//	    nc := dc.(nusadb.Notifier)
//	    _ = nc.Listen("orders")
//	    n, _ := nc.Poll(5 * time.Second)
//	    ...
//	    return nil
//	})
type Notifier interface {
	// Listen subscribes to notifications on a channel (LISTEN channel).
	Listen(channel string) error
	// Unlisten stops listening on a channel (UNLISTEN channel); "" unlistens all (UNLISTEN *).
	Unlisten(channel string) error
	// Notify sends a notification with an optional payload (NOTIFY channel[, 'payload']).
	Notify(channel, payload string) error
	// Notifications returns and clears notifications already received (no socket read).
	Notifications() []Notification
	// Poll waits up to timeout for the next notification (nil on timeout; 0 blocks).
	Poll(timeout time.Duration) (*Notification, error)
}

// Notification is an asynchronous LISTEN/NOTIFY message delivered by the server.
type Notification struct {
	// PID is the backend pid of the connection that issued the NOTIFY.
	PID uint32
	// Channel is the channel the notification was sent on.
	Channel string
	// Payload is the payload (empty string when NOTIFY carried none).
	Payload string
}

func (c *conn) send(frame []byte) error {
	if _, err := c.netConn.Write(frame); err != nil {
		c.badConn = true
		return err
	}
	return nil
}

func (c *conn) read() (message, error) {
	msg, err := readMessage(c.r)
	if err != nil {
		c.badConn = true
	}
	return msg, err
}

func (c *conn) handshake() error {
	if err := c.send(encStartup(c.cfg.User, c.cfg.Database)); err != nil {
		return err
	}
	for {
		msg, err := c.read()
		if err != nil {
			return err
		}
		switch msg.typ {
		case bAuth:
			rd := msg.reader()
			sub, err := rd.u32()
			if err != nil {
				return err
			}
			if sub == authSASL {
				if err := c.scram(rd); err != nil {
					return err
				}
			}
		case bBackendKey:
			rd := msg.reader()
			pid, err := rd.u32()
			if err != nil {
				return err
			}
			secret, err := rd.u32()
			if err != nil {
				return err
			}
			c.pid, c.secret, c.hasKey = pid, secret, true
		case bReady:
			return nil
		case bError:
			return readError(msg)
		}
	}
}

func (c *conn) scram(offer *reader) error {
	count, err := offer.u16()
	if err != nil {
		return err
	}
	supported := false
	for i := uint16(0); i < count; i++ {
		m, err := offer.str()
		if err != nil {
			return err
		}
		if m == scramMechanism {
			supported = true
		}
	}
	if !supported {
		return errors.New("nusadb: server offered no supported SASL mechanism")
	}
	if c.cfg.Password == "" {
		return errors.New("nusadb: server requires a password but none was given")
	}

	bare, first, err := scramClientFirst(c.cfg.User)
	if err != nil {
		return err
	}
	if err := c.send(encSaslInitial(scramMechanism, []byte(first))); err != nil {
		return err
	}

	msg, err := c.read()
	if err != nil {
		return err
	}
	if msg.typ == bError {
		return readError(msg)
	}
	rd := msg.reader()
	if msg.typ != bAuth {
		return errors.New("nusadb: expected a SASL continue message")
	}
	if sub, err := rd.u32(); err != nil || sub != authSASLContinue {
		return errors.New("nusadb: expected a SASL continue message")
	}
	serverFirst := string(rd.rest())

	final, expected, err := scramClientFinal(c.cfg.Password, bare, serverFirst)
	if err != nil {
		return err
	}
	if err := c.send(encSaslResponse(final)); err != nil {
		return err
	}

	msg, err = c.read()
	if err != nil {
		return err
	}
	if msg.typ == bError {
		return readError(msg)
	}
	rd = msg.reader()
	if msg.typ != bAuth {
		return errors.New("nusadb: expected a SASL final message")
	}
	if sub, err := rd.u32(); err != nil || sub != authSASLFinal {
		return errors.New("nusadb: expected a SASL final message")
	}
	if !verifyServerFinal(string(rd.rest()), expected) {
		return errors.New("nusadb: server signature did not verify")
	}
	return nil
}

// resultSet is a collected statement result.
type resultSet struct {
	columns []string
	// columnTypes holds each column's canonical type name (protocol 1.1, R42-B.03), parallel to
	// columns; an entry is "" when the server answered with the untyped (1.0) row description.
	columnTypes []string
	rows        [][][]byte
	tag         string
}

func (c *conn) readUntilReady() (*resultSet, error) {
	rs := &resultSet{}
	var serverErr error
	for {
		msg, err := c.read()
		if err != nil {
			return nil, err
		}
		switch msg.typ {
		case bRowDescription:
			rd := msg.reader()
			n, err := rd.u16()
			if err != nil {
				return nil, err
			}
			rs.columns = make([]string, n)
			rs.columnTypes = make([]string, n)
			for i := range rs.columns {
				if rs.columns[i], err = rd.str(); err != nil {
					return nil, err
				}
			}
		case bRowDescriptionTyped:
			// Protocol 1.1: each column is a name plus a 1-byte type tag (§9.2).
			rd := msg.reader()
			n, err := rd.u16()
			if err != nil {
				return nil, err
			}
			rs.columns = make([]string, n)
			rs.columnTypes = make([]string, n)
			for i := range rs.columns {
				if rs.columns[i], err = rd.str(); err != nil {
					return nil, err
				}
				tag, err := rd.u8()
				if err != nil {
					return nil, err
				}
				rs.columnTypes[i] = typeName(tag)
			}
		case bDataRow:
			fields, err := msg.reader().decodeFields()
			if err != nil {
				return nil, err
			}
			rs.rows = append(rs.rows, fields)
		case bCommandComplete:
			if rs.tag, err = msg.reader().str(); err != nil {
				return nil, err
			}
		case bNotification:
			// A pending LISTEN/NOTIFY message can lead the next query's response; buffer it.
			note, nerr := decodeNotification(msg)
			if nerr != nil {
				return nil, nerr
			}
			c.notifications = append(c.notifications, note)
		case bError:
			serverErr = readError(msg)
		case bReady:
			if serverErr != nil {
				return nil, serverErr
			}
			return rs, nil
		}
	}
}

// quoteIdentifier quotes an identifier (a LISTEN/NOTIFY channel) so an arbitrary name is emitted
// safely; the server folds a quoted identifier to its literal value.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// quoteLiteral quotes a string as a SQL literal ('…', doubling '), for a NOTIFY payload.
func quoteLiteral(text string) string {
	return "'" + strings.ReplaceAll(text, "'", "''") + "'"
}

// decodeNotification decodes a NotificationResponse body: [pid:u32][channel:str][payload:str].
func decodeNotification(msg message) (Notification, error) {
	rd := msg.reader()
	pid, err := rd.u32()
	if err != nil {
		return Notification{}, err
	}
	channel, err := rd.str()
	if err != nil {
		return Notification{}, err
	}
	payload, err := rd.str()
	if err != nil {
		return Notification{}, err
	}
	return Notification{PID: pid, Channel: channel, Payload: payload}, nil
}

// Listen subscribes to asynchronous notifications on channel (LISTEN channel). Collect them with
// Notifications or Poll.
func (c *conn) Listen(channel string) error {
	_, err := c.simpleQuery("LISTEN " + quoteIdentifier(channel))
	return err
}

// Unlisten stops listening on channel (UNLISTEN channel); an empty channel unlistens all (UNLISTEN *).
func (c *conn) Unlisten(channel string) error {
	target := "*"
	if channel != "" {
		target = quoteIdentifier(channel)
	}
	_, err := c.simpleQuery("UNLISTEN " + target)
	return err
}

// Notify sends a notification on channel with an optional payload (NOTIFY channel[, 'payload']).
func (c *conn) Notify(channel, payload string) error {
	sql := "NOTIFY " + quoteIdentifier(channel)
	if payload != "" {
		sql += ", " + quoteLiteral(payload)
	}
	_, err := c.simpleQuery(sql)
	return err
}

// Notifications returns and clears the notifications already received (buffered while reading other
// responses). It does not touch the socket — use Poll to wait for new ones.
func (c *conn) Notifications() []Notification {
	pending := c.notifications
	c.notifications = nil
	return pending
}

// Poll waits up to timeout for the next notification (a buffered one is returned immediately; nil on
// timeout). A timeout of 0 blocks until one arrives. Only meaningful after Listen.
func (c *conn) Poll(timeout time.Duration) (*Notification, error) {
	if len(c.notifications) > 0 {
		note := c.notifications[0]
		c.notifications = c.notifications[1:]
		return &note, nil
	}
	if timeout > 0 {
		if err := c.netConn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return nil, err
		}
		defer func() { _ = c.netConn.SetReadDeadline(time.Time{}) }()
	}
	msg, err := c.read()
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return nil, nil
		}
		c.badConn = true
		return nil, err
	}
	switch msg.typ {
	case bNotification:
		note, derr := decodeNotification(msg)
		if derr != nil {
			return nil, derr
		}
		return &note, nil
	case bError:
		return nil, readError(msg)
	default:
		return nil, fmt.Errorf("nusadb: unexpected message %q while polling for notifications", msg.typ)
	}
}

// copyIn drives COPY ... FROM STDIN (§12.1): stream src as CopyData chunks, finish with CopyDone,
// and return the loaded row count. A read error on src sends CopyFail and leaves the conn ready.
func (c *conn) copyIn(query string, src io.Reader) (int64, error) {
	if err := c.send(encQuery(query)); err != nil {
		return 0, err
	}
	if err := c.awaitCopyStart(bCopyIn); err != nil {
		return 0, err
	}
	buf := make([]byte, 64*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if serr := c.send(encCopyData(buf[:n])); serr != nil {
				return 0, serr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = c.send(encCopyFail("nusadb: client read error during COPY"))
			c.drainToReady()
			return 0, fmt.Errorf("nusadb: COPY source read failed: %w", err)
		}
	}
	if err := c.send(encCopyDone()); err != nil {
		return 0, err
	}
	return c.finishCopy()
}

// copyOut drives COPY ... TO STDOUT (§12.2): write each CopyData chunk to dst, return the row count.
func (c *conn) copyOut(query string, dst io.Writer) (int64, error) {
	if err := c.send(encQuery(query)); err != nil {
		return 0, err
	}
	if err := c.awaitCopyStart(bCopyOut); err != nil {
		return 0, err
	}
	for {
		msg, err := c.read()
		if err != nil {
			return 0, err
		}
		switch msg.typ {
		case bCopyData:
			if _, werr := dst.Write(msg.payload); werr != nil {
				c.drainToReady()
				return 0, fmt.Errorf("nusadb: COPY sink write failed: %w", werr)
			}
		case bCopyDone:
			return c.finishCopy()
		case bError:
			serverErr := readError(msg)
			c.drainToReady()
			return 0, serverErr
		}
	}
}

// awaitCopyStart reads until the expected CopyInResponse/CopyOutResponse; a refused COPY arrives as
// Error then ReadyForQuery and is surfaced, the conn left ready.
func (c *conn) awaitCopyStart(expected byte) error {
	var serverErr error
	for {
		msg, err := c.read()
		if err != nil {
			return err
		}
		switch msg.typ {
		case expected:
			return nil
		case bError:
			serverErr = readError(msg)
		case bReady:
			if serverErr != nil {
				return serverErr
			}
			return errors.New("nusadb: COPY did not start")
		}
	}
}

// finishCopy drains to ReadyForQuery, returning the row count from the COPY <n> command tag.
func (c *conn) finishCopy() (int64, error) {
	var tag string
	var serverErr error
	for {
		msg, err := c.read()
		if err != nil {
			return 0, err
		}
		switch msg.typ {
		case bCommandComplete:
			if tag, err = msg.reader().str(); err != nil {
				return 0, err
			}
		case bError:
			serverErr = readError(msg)
		case bReady:
			if serverErr != nil {
				return 0, serverErr
			}
			return copyCount(tag), nil
		}
	}
}

// drainToReady reads and discards frames until ReadyForQuery (best-effort resync).
func (c *conn) drainToReady() {
	for {
		msg, err := c.read()
		if err != nil || msg.typ == bReady {
			return
		}
	}
}

// copyCount parses the row count from a "COPY <n>" command tag (0 if absent/unparseable).
func copyCount(tag string) int64 {
	for i := len(tag) - 1; i >= 0; i-- {
		if tag[i] == ' ' {
			n, err := strconv.ParseInt(tag[i+1:], 10, 64)
			if err != nil {
				return 0
			}
			return n
		}
	}
	return 0
}

func (c *conn) freshName(prefix string) string {
	id := atomic.AddUint64(&c.nextName, 1)
	return fmt.Sprintf("nusa_%s_%d", prefix, id)
}

func (c *conn) simpleQuery(query string) (*resultSet, error) {
	if err := c.send(encQuery(query)); err != nil {
		return nil, err
	}
	return c.readUntilReady()
}

func (c *conn) extendedQuery(query string, args []driver.Value) (*resultSet, error) {
	values, present := encodeArgs(args)
	if err := c.sendAll(
		encParse("", query),
		encBind("", "", values, present),
		encDescribePortal(""),
		encExecute("", 0),
		encSync(),
	); err != nil {
		return nil, err
	}
	return c.readUntilReady()
}

func (c *conn) sendAll(frames ...[]byte) error {
	for _, f := range frames {
		if err := c.send(f); err != nil {
			return err
		}
	}
	return nil
}

// --- database/sql/driver.Conn ---

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	name := c.freshName("stmt")
	if err := c.sendAll(encParse(name, query), encSync()); err != nil {
		return nil, err
	}
	if _, err := c.readUntilReady(); err != nil { // surfaces a parse error
		return nil, err
	}
	return &stmt{conn: c, name: name}, nil
}

func (c *conn) Close() error {
	_ = c.send(encTerminate())
	return c.netConn.Close()
}

// Begin starts a transaction (sends BEGIN); the returned Tx commits/rolls it back.
func (c *conn) Begin() (driver.Tx, error) {
	if _, err := c.simpleQuery("BEGIN"); err != nil {
		return nil, err
	}
	return &nusaTx{conn: c}, nil
}

func (c *conn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	// Isolation level / read-only options are not yet honoured; the server uses its default.
	return c.Begin()
}

// nusaTx is a database/sql transaction over one connection.
type nusaTx struct {
	conn *conn
}

func (t *nusaTx) Commit() error {
	_, err := t.conn.simpleQuery("COMMIT")
	return err
}

func (t *nusaTx) Rollback() error {
	_, err := t.conn.simpleQuery("ROLLBACK")
	return err
}

// Ping verifies the connection with a trivial query.
func (c *conn) Ping(_ context.Context) error {
	_, err := c.simpleQuery("SELECT 1")
	return err
}

// IsValid lets database/sql discard a connection that hit a transport error.
func (c *conn) IsValid() bool { return !c.badConn }

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	stop := c.watchCancel(ctx)
	defer stop()
	rs, err := c.runQuery(query, namedToValues(args))
	if err != nil {
		return nil, err
	}
	return &rows{rs: rs}, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stop := c.watchCancel(ctx)
	defer stop()
	rs, err := c.runQuery(query, namedToValues(args))
	if err != nil {
		return nil, err
	}
	return result{affected: parseAffected(rs.tag)}, nil
}

// watchCancel arranges for the server-side statement to be cancelled if ctx is
// cancelled before the query returns (docs/wire-protocol.md §13). The returned
// function stops the watcher and must be deferred.
func (c *conn) watchCancel(ctx context.Context) func() {
	if ctx.Done() == nil || !c.hasKey {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			c.sendCancel()
		case <-done:
		}
	}()
	return func() { close(done) }
}

// sendCancel opens a fresh connection and sends a CancelRequest for this
// connection's in-flight statement. Best effort: errors are ignored.
func (c *conn) sendCancel() {
	addr := net.JoinHostPort(c.cfg.Host, strconv.Itoa(c.cfg.Port))
	side, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return
	}
	defer side.Close()
	_, _ = side.Write(encCancel(c.pid, c.secret))
}

func (c *conn) runQuery(query string, args []driver.Value) (*resultSet, error) {
	if len(args) == 0 {
		return c.simpleQuery(query)
	}
	return c.extendedQuery(query, args)
}

// --- database/sql/driver.Stmt ---

type stmt struct {
	conn *conn
	name string
}

func (s *stmt) Close() error  { return nil } // freed when the connection closes
func (s *stmt) NumInput() int { return -1 }  // variable; database/sql skips arg-count checks

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	rs, err := s.run(args)
	if err != nil {
		return nil, err
	}
	return result{affected: parseAffected(rs.tag)}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	rs, err := s.run(args)
	if err != nil {
		return nil, err
	}
	return &rows{rs: rs}, nil
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	stop := s.conn.watchCancel(ctx)
	defer stop()
	return s.Exec(namedToValues(args))
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	stop := s.conn.watchCancel(ctx)
	defer stop()
	return s.Query(namedToValues(args))
}

func (s *stmt) run(args []driver.Value) (*resultSet, error) {
	values, present := encodeArgs(args)
	portal := s.conn.freshName("portal")
	if err := s.conn.sendAll(
		encBind(portal, s.name, values, present),
		encDescribePortal(portal),
		encExecute(portal, 0),
		encSync(),
	); err != nil {
		return nil, err
	}
	return s.conn.readUntilReady()
}

// --- database/sql/driver.Rows ---

type rows struct {
	rs  *resultSet
	pos int
}

func (r *rows) Columns() []string { return r.rs.columns }
func (r *rows) Close() error      { return nil }

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName: the NusaDB type name
// of column index (protocol 1.1, R42-B.03), or "" if the server did not report it.
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	if index >= 0 && index < len(r.rs.columnTypes) {
		return r.rs.columnTypes[index]
	}
	return ""
}

func (r *rows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rs.rows) {
		return io.EOF
	}
	row := r.rs.rows[r.pos]
	r.pos++
	for i := range dest {
		if i >= len(row) || row[i] == nil {
			dest[i] = nil
			continue
		}
		typeName := ""
		if i < len(r.rs.columnTypes) {
			typeName = r.rs.columnTypes[i]
		}
		dest[i] = decodeCell(row[i], typeName)
	}
	return nil
}

// timeLayouts are tried in order for DATE / TIMESTAMP columns; the `.999999999` makes the fractional
// seconds optional, and `-07` / `-07:00` cover the server's bare and colon-separated offsets.
var timeLayouts = []string{
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999-07",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02",
}

// decodeCell turns a text-format field into a typed database/sql driver.Value by its protocol 1.1
// type tag: BOOL->bool, INT->int64, FLOAT->float64, DATE/TIMESTAMP->time.Time, BYTES->[]byte. Other
// types (NUMERIC, UUID, TIME, INTERVAL, JSON, ARRAY, TEXT) have no distinct driver.Value and stay a
// string. A value that does not parse as its tag falls back to the raw string, so an unexpected wire
// form never fails the scan.
func decodeCell(field []byte, typeName string) driver.Value {
	switch typeName {
	case "BYTES":
		// A BYTEA column arrives as \x<hex>; decode it to the raw bytes.
		return decodeByteaHex(field)
	case "BOOL":
		s := string(field)
		return s == "true" || s == "t" || s == "1"
	case "INT":
		if n, err := strconv.ParseInt(string(field), 10, 64); err == nil {
			return n
		}
		return string(field)
	case "FLOAT":
		if f, err := strconv.ParseFloat(string(field), 64); err == nil {
			return f
		}
		return string(field)
	case "DATE", "TIMESTAMP", "TIMESTAMPTZ":
		for _, layout := range timeLayouts {
			if t, err := time.Parse(layout, string(field)); err == nil {
				return t
			}
		}
		return string(field)
	default:
		return string(field)
	}
}

// result implements driver.Result.
type result struct {
	affected int64
}

// LastInsertId reports 0: the server has no auto-generated keys (use an explicit primary key or a
// sequence). Returning 0 (rather than an error) lets ORMs that probe it keep an explicit key.
func (r result) LastInsertId() (int64, error) {
	return 0, nil
}

func (r result) RowsAffected() (int64, error) { return r.affected, nil }

// --- helpers ---

func readError(msg message) error {
	rd := msg.reader()
	code, err := rd.str()
	if err != nil {
		return err
	}
	message, err := rd.str()
	if err != nil {
		return err
	}
	return &Error{Code: code, Message: message}
}

func namedToValues(named []driver.NamedValue) []driver.Value {
	values := make([]driver.Value, len(named))
	for i, n := range named {
		values[i] = n.Value
	}
	return values
}

// encodeArgs renders driver values as wire text fields (present[i] == false is NULL).
func encodeArgs(args []driver.Value) (values [][]byte, present []bool) {
	values = make([][]byte, len(args))
	present = make([]bool, len(args))
	for i, a := range args {
		if a == nil {
			present[i] = false
			continue
		}
		present[i] = true
		switch v := a.(type) {
		case []byte:
			// Bind raw bytes as the BYTEA text form \x<hex>, which the server coerces into BYTEA.
			values[i] = byteaHexLiteral(v)
		case string:
			values[i] = []byte(v)
		case bool:
			if v {
				values[i] = []byte("true")
			} else {
				values[i] = []byte("false")
			}
		case int64:
			values[i] = []byte(strconv.FormatInt(v, 10))
		case float64:
			values[i] = []byte(strconv.FormatFloat(v, 'g', -1, 64))
		case time.Time:
			values[i] = []byte(v.Format("2006-01-02 15:04:05.999999"))
		default:
			values[i] = []byte(fmt.Sprintf("%v", v))
		}
	}
	return values, present
}

// byteaHexLiteral renders bytes as the BYTEA text form \x<hex>.
func byteaHexLiteral(b []byte) []byte {
	out := make([]byte, 2+hex.EncodedLen(len(b)))
	out[0] = '\\'
	out[1] = 'x'
	hex.Encode(out[2:], b)
	return out
}

// decodeByteaHex decodes a BYTEA \x<hex> text field to raw bytes (falls back to the raw field on a
// malformed value).
func decodeByteaHex(field []byte) []byte {
	h := field
	if len(h) >= 2 && h[0] == '\\' && h[1] == 'x' {
		h = h[2:]
	}
	out := make([]byte, hex.DecodedLen(len(h)))
	if _, err := hex.Decode(out, h); err != nil {
		return append([]byte(nil), field...)
	}
	return out
}

func parseAffected(tag string) int64 {
	if tag == "" {
		return 0
	}
	last := tag
	for i := len(tag) - 1; i >= 0; i-- {
		if tag[i] == ' ' {
			last = tag[i+1:]
			break
		}
	}
	n, err := strconv.ParseInt(last, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// Ensure interface conformance at compile time.
var (
	_ driver.Driver             = (*Driver)(nil)
	_ driver.DriverContext      = (*Driver)(nil)
	_ driver.Connector          = (*connector)(nil)
	_ driver.Conn               = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.Validator          = (*conn)(nil)
	_ driver.Tx                 = (*nusaTx)(nil)
	_ driver.Stmt               = (*stmt)(nil)
	_ driver.StmtQueryContext   = (*stmt)(nil)
	_ driver.StmtExecContext    = (*stmt)(nil)
	_ driver.Rows               = (*rows)(nil)
	_ driver.Result             = (result{})
)
