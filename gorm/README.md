# nusadb-gorm — GORM dialector for NusaDB

A [GORM](https://gorm.io) dialector built on the NusaDB `database/sql`
[driver](../). It lets you use GORM models, AutoMigrate, CRUD, and transactions
against a NusaDB server.

## Install

```bash
go get github.com/nusadb/nusadb-go/gorm
```

## Usage

```go
import (
	"gorm.io/gorm"
	nusadbgorm "github.com/nusadb/nusadb-go/gorm"
)

type Product struct {
	ID    uint `gorm:"primaryKey;autoIncrement:false"`
	Name  string
	Price int
}

db, err := gorm.Open(nusadbgorm.Open("nusadb://nusa-root@127.0.0.1:5678/nusadb"), &gorm.Config{})

db.AutoMigrate(&Product{})
db.Create(&Product{ID: 1, Name: "alice", Price: 10})

var p Product
db.First(&p, 1)

db.Model(&p).Update("price", 15)
db.Delete(&Product{}, 1)

db.Transaction(func(tx *gorm.DB) error {
	return tx.Create(&Product{ID: 2, Name: "bob"}).Error
})
```

## Notes

- **Primary keys must be explicit** (`autoIncrement:false`): the server has no
  sequence default and no `RETURNING`, so set the key yourself (or use a sequence
  via raw SQL).
- The dialector renders `LIMIT`/`OFFSET` inline (the server requires a constant
  there), opens the `database/sql` connection over the NusaDB driver, and maps GORM
  types to `INT`/`TEXT`/`BOOL`/`FLOAT`/`NUMERIC`/`TIMESTAMP`.
- The full GORM query surface works: `Where`, `Limit`/`Offset`, `Order`, `Joins`,
  `Group`/`Having`, and aggregates (`Count`, `sum`/`min`/`max`/`avg` via `Select`).
- `AutoMigrate` checks existence with `SHOW TABLES`; the `Migrator`'s `HasTable` and
  `HasColumn` use `SHOW TABLES` / `SHOW COLUMNS FROM` (NusaDB reports tables under the
  fixed schema `public`, so the base migrator's `information_schema` probes would
  otherwise never match).
- Transactions use the driver's `database/sql` transaction support (BEGIN/COMMIT/
  ROLLBACK over the wire).

## Test

```bash
cargo build -p nusadb-server
cd drivers/go/gorm && go test ./...
```

The test boots a real `nusadb-server` (honouring `CARGO_TARGET_DIR`) and exercises
AutoMigrate, `HasTable`/`HasColumn` introspection, create/read/update/delete, count,
pagination, aggregates, GROUP BY + HAVING, a JOIN, and a rolled-back transaction.

## License

Apache-2.0.
