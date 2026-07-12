package nusadbgorm_test

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	nusadbgorm "github.com/nusadb/nusadb-go/gorm"
	"gorm.io/gorm"
)

// Product is a GORM model with an explicit primary key (the server has no sequence default).
type Product struct {
	ID    uint `gorm:"primaryKey;autoIncrement:false"`
	Name  string
	Price int
}

func serverBinary(t *testing.T) string {
	var bases []string
	if env := os.Getenv("CARGO_TARGET_DIR"); env != "" {
		bases = append(bases, env)
	}
	_, file, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	bases = append(bases, filepath.Join(repoRoot, "target"))
	name := "nusadb-server"
	if runtime.GOOS == "windows" {
		name = "nusadb-server.exe"
	}
	for _, base := range bases {
		for _, profile := range []string{"debug", "release"} {
			p := filepath.Join(base, profile, name)
			if _, err := os.Stat(p); err == nil {
				return p
			}
		}
	}
	t.Skip("nusadb-server binary not found; run `cargo build -p nusadb-server`")
	return ""
}

func startServer(t *testing.T) (int, func()) {
	bin := serverBinary(t)
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	dir := t.TempDir()
	cmd := exec.Command(bin, "--listen", fmt.Sprintf("127.0.0.1:%d", port), "--data-dir", dir)
	cmd.Env = append(os.Environ(), "RUST_LOG=error")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond); err == nil {
			c.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return port, func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() }
}

func TestGormCRUDAndTransaction(t *testing.T) {
	port, stop := startServer(t)
	defer stop()

	dsn := fmt.Sprintf("nusadb://u@127.0.0.1:%d/nusadb", port)
	db, err := gorm.Open(nusadbgorm.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm open: %v", err)
	}

	// AutoMigrate creates the table (HasTable via SHOW TABLES → CreateTable).
	if err := db.AutoMigrate(&Product{}); err != nil {
		t.Fatalf("automigrate: %v", err)
	}

	// Create.
	if err := db.Create(&Product{ID: 1, Name: "alice", Price: 10}).Error; err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := db.Create(&Product{ID: 2, Name: "bob", Price: 20}).Error; err != nil {
		t.Fatalf("create: %v", err)
	}

	// Read one.
	var p Product
	if err := db.First(&p, 1).Error; err != nil {
		t.Fatalf("first: %v", err)
	}
	if p.Name != "alice" || p.Price != 10 {
		t.Fatalf("got %+v, want alice/10", p)
	}

	// Read all ordered.
	var all []Product
	if err := db.Order("id").Find(&all).Error; err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(all) != 2 || all[0].Name != "alice" || all[1].Name != "bob" {
		t.Fatalf("got %+v, want [alice bob]", all)
	}

	// Update.
	if err := db.Model(&Product{}).Where("id = ?", 1).Update("price", 15).Error; err != nil {
		t.Fatalf("update: %v", err)
	}
	db.First(&p, 1)
	if p.Price != 15 {
		t.Fatalf("after update price = %d, want 15", p.Price)
	}

	// Delete.
	if err := db.Delete(&Product{}, 2).Error; err != nil {
		t.Fatalf("delete: %v", err)
	}
	var count int64
	db.Model(&Product{}).Count(&count)
	if count != 1 {
		t.Fatalf("count after delete = %d, want 1", count)
	}

	// Transaction that rolls back leaves no row.
	_ = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&Product{ID: 99, Name: "temp"}).Error; err != nil {
			return err
		}
		return errors.New("rollback")
	})
	var temp Product
	if err := db.First(&temp, 99).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected row 99 to be rolled back, got err=%v", err)
	}
}
