package nusadb_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	nusadb "github.com/nusadb/nusadb-go"
)

// testServer is a child nusadb-server process on an ephemeral port.
type testServer struct {
	port int
	cmd  *exec.Cmd
	dir  string
}

func repoRoot(t *testing.T) string {
	// driver_test.go lives in drivers/go; the repo root is three levels up.
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func serverBinary(t *testing.T) string {
	var bases []string
	if env := os.Getenv("CARGO_TARGET_DIR"); env != "" {
		bases = append(bases, env)
	}
	bases = append(bases, filepath.Join(repoRoot(t), "target"))
	names := []string{"nusadb-server"}
	if runtime.GOOS == "windows" {
		names = []string{"nusadb-server.exe"}
	}
	for _, base := range bases {
		for _, profile := range []string{"debug", "release"} {
			for _, name := range names {
				p := filepath.Join(base, profile, name)
				if _, err := os.Stat(p); err == nil {
					return p
				}
			}
		}
	}
	t.Skip("nusadb-server binary not found; run `cargo build -p nusadb-server` first")
	return ""
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func startServer(t *testing.T, extra ...string) *testServer {
	bin := serverBinary(t)
	port := freePort(t)
	dir := t.TempDir()
	args := append([]string{
		"--listen", fmt.Sprintf("127.0.0.1:%d", port),
		"--data-dir", dir,
	}, extra...)
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), "RUST_LOG=error")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	ts := &testServer{port: port, cmd: cmd, dir: dir}
	ts.waitReady(t)
	return ts
}

func (ts *testServer) waitReady(t *testing.T) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", ts.port), 500*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("server on port %d did not become ready", ts.port)
}

func (ts *testServer) stop() {
	_ = ts.cmd.Process.Kill()
	_, _ = ts.cmd.Process.Wait()
}

func (ts *testServer) dsn(user, password string) string {
	if password != "" {
		return fmt.Sprintf("nusadb://%s:%s@127.0.0.1:%d/nusadb", user, password, ts.port)
	}
	return fmt.Sprintf("nusadb://%s@127.0.0.1:%d/nusadb", user, ts.port)
}

func TestParseDSNTLS(t *testing.T) {
	// Plaintext by default.
	cfg, err := nusadb.ParseDSN("nusadb://u@127.0.0.1:5678/nusadb")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TLSConfig != nil {
		t.Fatal("expected no TLS by default")
	}
	// tls=true verifies against the host name.
	cfg, err = nusadb.ParseDSN("nusadb://u@db.example.com:5678/nusadb?tls=true")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TLSConfig == nil || cfg.TLSConfig.ServerName != "db.example.com" || cfg.TLSConfig.InsecureSkipVerify {
		t.Fatalf("tls=true: got %+v", cfg.TLSConfig)
	}
	// tls=skip-verify disables verification.
	cfg, err = nusadb.ParseDSN("nusadb://u@127.0.0.1:5678/nusadb?tls=skip-verify")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TLSConfig == nil || !cfg.TLSConfig.InsecureSkipVerify {
		t.Fatal("tls=skip-verify should set InsecureSkipVerify")
	}
	// A registered config is selected by name (and inherits the host as ServerName when unset).
	if err := nusadb.RegisterTLSConfig("pinned", &tls.Config{MinVersion: tls.VersionTLS13}); err != nil {
		t.Fatal(err)
	}
	cfg, err = nusadb.ParseDSN("nusadb://u@host.example:5678/nusadb?tls=pinned")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TLSConfig == nil || cfg.TLSConfig.ServerName != "host.example" {
		t.Fatalf("tls=pinned: got %+v", cfg.TLSConfig)
	}
	// An unknown name is rejected.
	if _, err := nusadb.ParseDSN("nusadb://u@127.0.0.1:5678/nusadb?tls=nope"); err == nil {
		t.Fatal("unknown tls config should error")
	}
	// Reserved names cannot be registered.
	if err := nusadb.RegisterTLSConfig("true", &tls.Config{}); err == nil {
		t.Fatal("reserved name should be rejected")
	}
}

func TestSimpleQueryRoundTrip(t *testing.T) {
	ts := startServer(t)
	defer ts.stop()
	db, err := sql.Open("nusadb", ts.dsn("u", ""))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE go_simple (id INT NOT NULL, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	res, err := db.Exec("INSERT INTO go_simple VALUES (5, 'alice')")
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("RowsAffected = %d, want 1", n)
	}

	var id int
	var name string
	if err := db.QueryRow("SELECT id, name FROM go_simple").Scan(&id, &name); err != nil {
		t.Fatal(err)
	}
	if id != 5 || name != "alice" {
		t.Fatalf("got (%d, %q), want (5, alice)", id, name)
	}
}

func TestTypedDecoding(t *testing.T) {
	ts := startServer(t)
	defer ts.stop()
	db, err := sql.Open("nusadb", ts.dsn("u", ""))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE go_decode (b BOOL, i INT, f FLOAT, ts TIMESTAMP, n NUMERIC(10,2))"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO go_decode VALUES (true, 42, 1.5, TIMESTAMP '2024-01-15 10:30:00', 12.34)"); err != nil {
		t.Fatal(err)
	}

	// Scanning into interface{} passes the driver's typed value through unchanged, so this asserts
	// the dynamic Go type the driver produced for each wire tag.
	var b, i, f, ts2, n interface{}
	if err := db.QueryRow("SELECT b, i, f, ts, n FROM go_decode").Scan(&b, &i, &f, &ts2, &n); err != nil {
		t.Fatal(err)
	}
	if v, ok := b.(bool); !ok || !v {
		t.Fatalf("BOOL = %T(%v), want bool true", b, b)
	}
	if v, ok := i.(int64); !ok || v != 42 {
		t.Fatalf("INT = %T(%v), want int64 42", i, i)
	}
	if v, ok := f.(float64); !ok || v != 1.5 {
		t.Fatalf("FLOAT = %T(%v), want float64 1.5", f, f)
	}
	if _, ok := ts2.(time.Time); !ok {
		t.Fatalf("TIMESTAMP = %T, want time.Time", ts2)
	}
	// NUMERIC has no lossless native driver.Value, so it stays a string.
	if v, ok := n.(string); !ok || v != "12.34" {
		t.Fatalf("NUMERIC = %T(%v), want string 12.34", n, n)
	}
}

func TestByteaRoundTrip(t *testing.T) {
	ts := startServer(t)
	defer ts.stop()
	db, err := sql.Open("nusadb", ts.dsn("u", ""))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE bt (id INT NOT NULL, data BYTEA, PRIMARY KEY (id))"); err != nil {
		t.Fatal(err)
	}
	payload := []byte{0xde, 0xad, 0xbe, 0xef, 0x00, 0x7f}
	if _, err := db.Exec("INSERT INTO bt VALUES ($1, $2)", 1, payload); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO bt VALUES ($1, $2)", 2, []byte{}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO bt VALUES ($1, $2)", 3, nil); err != nil {
		t.Fatal(err)
	}

	var got []byte
	if err := db.QueryRow("SELECT data FROM bt WHERE id = 1").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("bytea round-trip = %x, want %x", got, payload)
	}
	var empty []byte
	if err := db.QueryRow("SELECT data FROM bt WHERE id = 2").Scan(&empty); err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("empty bytea = %x, want empty", empty)
	}
	var isNull []byte
	if err := db.QueryRow("SELECT data FROM bt WHERE id = 3").Scan(&isNull); err != nil {
		t.Fatal(err)
	}
	if isNull != nil {
		t.Fatalf("null bytea = %x, want nil", isNull)
	}
}

func TestCopyInAndOut(t *testing.T) {
	ts := startServer(t)
	defer ts.stop()
	db, err := sql.Open("nusadb", ts.dsn("u", ""))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE copy_t (id INT NOT NULL, name TEXT, PRIMARY KEY (id))"); err != nil {
		t.Fatal(err)
	}

	sc, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()

	// COPY FROM STDIN: tab-delimited rows, \N for NULL.
	loaded, err := nusadb.CopyIn(sc, "COPY copy_t FROM STDIN",
		bytes.NewReader([]byte("1\talice\n2\t\\N\n3\tcarol\n")))
	if err != nil {
		t.Fatal(err)
	}
	if loaded != 3 {
		t.Fatalf("CopyIn loaded %d rows, want 3", loaded)
	}

	// The NULL round-tripped.
	var name sql.NullString
	if err := db.QueryRow("SELECT name FROM copy_t WHERE id = 2").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name.Valid {
		t.Fatalf("row 2 name = %q, want NULL", name.String)
	}

	// COPY TO STDOUT: bytes come back in the same text format.
	var buf bytes.Buffer
	exported, err := nusadb.CopyTo(sc, "COPY copy_t TO STDOUT", &buf)
	if err != nil {
		t.Fatal(err)
	}
	if exported != 3 {
		t.Fatalf("CopyTo exported %d rows, want 3", exported)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	want := []string{"1\talice", "2\t\\N", "3\tcarol"}
	if len(lines) != len(want) {
		t.Fatalf("export = %q", buf.String())
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Fatalf("export line %d = %q, want %q", i, lines[i], want[i])
		}
	}

	// A refused COPY errors; the connection stays usable.
	if _, err := nusadb.CopyIn(sc, "COPY no_such_table FROM STDIN", bytes.NewReader(nil)); err == nil {
		t.Fatal("expected an error copying into a missing table")
	}
	var n int
	if err := db.QueryRow("SELECT count(*) FROM copy_t").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("count = %d, want 3", n)
	}
}

// TestColumnTypes verifies the protocol 1.1 typed metadata (R42-B.03): the driver exposes each
// column's NusaDB type name via database/sql's ColumnType.DatabaseTypeName.
func TestColumnTypes(t *testing.T) {
	ts := startServer(t)
	defer ts.stop()
	db, err := sql.Open("nusadb", ts.dsn("u", ""))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE go_types (id INT NOT NULL, name TEXT, active BOOL)"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("SELECT id, name, active FROM go_types")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cts, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	got := []string{cts[0].DatabaseTypeName(), cts[1].DatabaseTypeName(), cts[2].DatabaseTypeName()}
	want := []string{"INT", "TEXT", "BOOL"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("column %d type = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestParameterizedQuery(t *testing.T) {
	ts := startServer(t)
	defer ts.stop()
	db, _ := sql.Open("nusadb", ts.dsn("u", ""))
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE go_params (id INT NOT NULL, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO go_params VALUES ($1, $2)", 1, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO go_params VALUES ($1, $2)", 2, nil); err != nil {
		t.Fatal(err)
	}

	var id int
	var name sql.NullString
	if err := db.QueryRow("SELECT id, name FROM go_params WHERE id = $1", 2).Scan(&id, &name); err != nil {
		t.Fatal(err)
	}
	if id != 2 || name.Valid {
		t.Fatalf("got (%d, valid=%v), want (2, valid=false)", id, name.Valid)
	}
}

func TestPreparedStatement(t *testing.T) {
	ts := startServer(t)
	defer ts.stop()
	db, _ := sql.Open("nusadb", ts.dsn("u", ""))
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE go_prep (id INT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	stmt, err := db.Prepare("INSERT INTO go_prep VALUES ($1)")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if _, err := stmt.Exec(i); err != nil {
			t.Fatal(err)
		}
	}
	stmt.Close()

	var count int
	if err := db.QueryRow("SELECT id FROM go_prep WHERE id = $1", 2).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("got %d, want 2", count)
	}
}

func TestExecMany(t *testing.T) {
	ts := startServer(t)
	defer ts.stop()
	db, _ := sql.Open("nusadb", ts.dsn("u", ""))
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE go_batch (id INT NOT NULL, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	counts, err := nusadb.ExecMany(db, "INSERT INTO go_batch VALUES ($1, $2)", [][]any{
		{1, "a"},
		{2, "b"},
		{3, "c"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(counts) != 3 || counts[0] != 1 || counts[1] != 1 || counts[2] != 1 {
		t.Fatalf("got counts %v, want [1 1 1]", counts)
	}

	var n int
	if err := db.QueryRow("SELECT count(*) FROM go_batch").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("got %d rows, want 3", n)
	}

	// An empty batch is a no-op.
	empty, err := nusadb.ExecMany(db, "INSERT INTO go_batch VALUES ($1, $2)", nil)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty batch: counts=%v err=%v", empty, err)
	}
}

func TestServerErrorSurfaces(t *testing.T) {
	ts := startServer(t)
	defer ts.stop()
	db, _ := sql.Open("nusadb", ts.dsn("u", ""))
	defer db.Close()

	_, err := db.Query("SELECT * FROM ghost")
	if err == nil {
		t.Fatal("expected an error for a missing table")
	}
	var serverErr *nusadb.Error
	if !errors.As(err, &serverErr) {
		t.Fatalf("expected *nusadb.Error, got %T: %v", err, err)
	}
	if len(serverErr.Code) != 5 {
		t.Fatalf("SQLSTATE %q is not 5 chars", serverErr.Code)
	}

	// The pool stays usable.
	if err := db.Ping(); err != nil {
		t.Fatalf("ping after error: %v", err)
	}
}

func TestTransactionsCommitAndRollback(t *testing.T) {
	ts := startServer(t)
	defer ts.stop()
	db, _ := sql.Open("nusadb", ts.dsn("u", ""))
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE go_tx (id INT NOT NULL)"); err != nil {
		t.Fatal(err)
	}

	// Rolled-back insert leaves no row.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO go_tx VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow("SELECT id FROM go_tx").Scan(&count); err != sql.ErrNoRows {
		t.Fatalf("expected no rows after rollback, got err=%v", err)
	}

	// Committed insert persists.
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO go_tx VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT id FROM go_tx").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("got %d, want 2", count)
	}
}

// TestSavepoints proves savepoints work through the standard database/sql transaction. Go's
// database/sql has no dedicated savepoint API, so a savepoint is just a statement the driver
// transports verbatim: SAVEPOINT / ROLLBACK TO SAVEPOINT / RELEASE SAVEPOINT on the open *sql.Tx.
func TestSavepoints(t *testing.T) {
	ts := startServer(t)
	defer ts.stop()
	db, _ := sql.Open("nusadb", ts.dsn("u", ""))
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE go_sp (id INT NOT NULL)"); err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	exec := func(sql string) {
		if _, err := tx.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	exec("INSERT INTO go_sp VALUES (1)")
	exec("SAVEPOINT sp1")
	exec("INSERT INTO go_sp VALUES (2)")
	exec("ROLLBACK TO SAVEPOINT sp1") // undoes (2), keeps (1), transaction continues
	exec("INSERT INTO go_sp VALUES (3)")
	exec("SAVEPOINT sp2")
	exec("INSERT INTO go_sp VALUES (4)")
	exec("RELEASE SAVEPOINT sp2") // keeps (4)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := db.QueryRow("SELECT count(*) FROM go_sp").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("got %d rows, want 3 (savepoint should have kept 1,3,4)", count)
	}
}

// TestListenNotify proves LISTEN/NOTIFY works end to end via the notification API reached through
// sql.Conn.Raw: a connection that LISTENs on a channel receives its own NOTIFY (self-delivery).
func TestListenNotify(t *testing.T) {
	ts := startServer(t)
	defer ts.stop()
	db, _ := sql.Open("nusadb", ts.dsn("u", ""))
	defer db.Close()

	sqlConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sqlConn.Close()

	err = sqlConn.Raw(func(dc any) error {
		nc := dc.(nusadb.Notifier)
		if lerr := nc.Listen("go_chan"); lerr != nil {
			return lerr
		}
		if nerr := nc.Notify("go_chan", "hello"); nerr != nil {
			return nerr
		}
		note, perr := nc.Poll(5 * time.Second)
		if perr != nil {
			return perr
		}
		if note == nil {
			t.Fatal("expected a self-delivered notification, got nil")
		}
		if note.Channel != "go_chan" || note.Payload != "hello" {
			t.Fatalf("got channel=%q payload=%q, want go_chan/hello", note.Channel, note.Payload)
		}
		// After UNLISTEN a further NOTIFY is not delivered (Poll times out -> nil).
		if uerr := nc.Unlisten("go_chan"); uerr != nil {
			return uerr
		}
		if nerr := nc.Notify("go_chan", "ignored"); nerr != nil {
			return nerr
		}
		note, perr = nc.Poll(300 * time.Millisecond)
		if perr != nil {
			return perr
		}
		if note != nil {
			t.Fatalf("expected no notification after UNLISTEN, got %+v", note)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestQuerySurface proves the driver transports the full SELECT surface (ORDER BY, DISTINCT,
// LIMIT/OFFSET, GROUP/HAVING, window, CTE, subquery, set ops, every JOIN flavour, LATERAL) to a
// real server via raw db.Query text. Each case asserts the row count the server actually returns.
func TestQuerySurface(t *testing.T) {
	ts := startServer(t)
	defer ts.stop()
	db, err := sql.Open("nusadb", ts.dsn("u", ""))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fixture := []string{
		"CREATE TABLE surf_a (id INT NOT NULL, grp TEXT, v INT)",
		"INSERT INTO surf_a VALUES (1, 'a', 10)",
		"INSERT INTO surf_a VALUES (2, 'a', 30)",
		"INSERT INTO surf_a VALUES (3, 'b', 20)",
		"INSERT INTO surf_a VALUES (4, 'b', 20)",
		"INSERT INTO surf_a VALUES (5, 'a', 10)",
		"CREATE TABLE surf_b (id INT NOT NULL, a_id INT, tag TEXT)",
		"INSERT INTO surf_b VALUES (10, 1, 'p')",
		"INSERT INTO surf_b VALUES (11, 1, 'q')",
		"INSERT INTO surf_b VALUES (12, 2, 'r')",
	}
	for _, stmt := range fixture {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("fixture %q: %v", stmt, err)
		}
	}

	cases := []struct {
		name string
		sql  string
		want int
	}{
		{"A_order_by", "SELECT id FROM surf_a ORDER BY v DESC, id", 5},
		{"B_distinct", "SELECT DISTINCT v FROM surf_a ORDER BY v", 3},
		{"C_distinct_on", "SELECT DISTINCT ON (grp) grp, v FROM surf_a ORDER BY grp, v", 2},
		{"D_limit", "SELECT id FROM surf_a ORDER BY id LIMIT 2", 2},
		{"E_offset", "SELECT id FROM surf_a ORDER BY id LIMIT 2 OFFSET 3", 2},
		{"F_group_having", "SELECT grp, count(*) FROM surf_a GROUP BY grp HAVING count(*) > 1 ORDER BY grp", 2},
		{"G_window", "SELECT id, row_number() OVER (PARTITION BY grp ORDER BY v) FROM surf_a ORDER BY id", 5},
		{"H_cte", "WITH g AS (SELECT grp, count(*) c FROM surf_a GROUP BY grp) SELECT count(*) FROM g", 1},
		{"I_subquery_in", "SELECT id FROM surf_a WHERE v IN (SELECT max(v) FROM surf_a) ORDER BY id", 1},
		{"J_union", "SELECT id FROM surf_a WHERE id = 1 UNION SELECT id FROM surf_a WHERE id = 2", 2},
		{"K_inner_join", "SELECT surf_a.grp, surf_b.tag FROM surf_a JOIN surf_b ON surf_a.id = surf_b.a_id ORDER BY surf_b.id", 3},
		{"L_left_join", "SELECT surf_a.grp, surf_b.tag FROM surf_a LEFT JOIN surf_b ON surf_a.id = surf_b.a_id ORDER BY surf_a.id, surf_b.id", 6},
		{"M_right_join", "SELECT surf_a.grp, surf_b.tag FROM surf_a RIGHT JOIN surf_b ON surf_a.id = surf_b.a_id ORDER BY surf_b.id", 3},
		{"N_full_join", "SELECT surf_a.grp, surf_b.tag FROM surf_a FULL JOIN surf_b ON surf_a.id = surf_b.a_id ORDER BY surf_a.id, surf_b.id", 6},
		{"O_cross_join", "SELECT surf_a.id, surf_b.id FROM surf_a CROSS JOIN surf_b", 15},
		{"P_lateral", "SELECT surf_a.id, l.tag FROM surf_a JOIN LATERAL (SELECT tag FROM surf_b WHERE surf_b.a_id = surf_a.id LIMIT 1) l ON true ORDER BY surf_a.id", 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := db.Query(tc.sql)
			if err != nil {
				t.Fatalf("query errored: %v\n  sql: %s", err, tc.sql)
			}
			defer rows.Close()
			n := 0
			for rows.Next() {
				n++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("row iteration errored: %v\n  sql: %s", err, tc.sql)
			}
			if n != tc.want {
				t.Fatalf("row count = %d, want %d\n  sql: %s", n, tc.want, tc.sql)
			}
		})
	}
}

func TestScramAuthentication(t *testing.T) {
	ts := startServer(t, "--auth-user", "alice:secret")
	defer ts.stop()

	db, _ := sql.Open("nusadb", ts.dsn("alice", "secret"))
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("correct password should authenticate: %v", err)
	}

	bad, _ := sql.Open("nusadb", ts.dsn("alice", "wrong"))
	defer bad.Close()
	if err := bad.Ping(); err == nil {
		t.Fatal("wrong password should be rejected")
	}
}
