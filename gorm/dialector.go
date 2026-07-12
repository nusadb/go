// Package nusadbgorm is a GORM dialector for NusaDB, built on the database/sql driver.
//
// Usage:
//
//	import (
//	    "gorm.io/gorm"
//	    nusadbgorm "github.com/nusadb/nusadb-go/gorm"
//	)
//
//	db, err := gorm.Open(nusadbgorm.Open("nusadb://nusa-root@127.0.0.1:5678/nusadb"), &gorm.Config{})
package nusadbgorm

import (
	"database/sql"
	"fmt"
	"strconv"

	_ "github.com/nusadb/nusadb-go" // registers the "nusadb" database/sql driver
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

// Dialector is the GORM dialector for NusaDB.
type Dialector struct {
	DSN  string
	Conn gorm.ConnPool
}

// Open returns a GORM dialector for the given NusaDB DSN.
func Open(dsn string) gorm.Dialector {
	return Dialector{DSN: dsn}
}

// New returns a GORM dialector using an existing connection pool.
func New(conn gorm.ConnPool) gorm.Dialector {
	return Dialector{Conn: conn}
}

// Name is the dialector name.
func (Dialector) Name() string { return "nusadb" }

// Initialize opens the connection pool and registers GORM's default callbacks.
func (d Dialector) Initialize(db *gorm.DB) error {
	if d.Conn != nil {
		db.ConnPool = d.Conn
	} else {
		conn, err := sql.Open("nusadb", d.DSN)
		if err != nil {
			return err
		}
		db.ConnPool = conn
	}
	// Minimal clause sets: the server has no RETURNING / ON CONFLICT / FOR UPDATE.
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		CreateClauses: []string{"INSERT", "VALUES"},
		QueryClauses:  []string{"SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT"},
		UpdateClauses: []string{"UPDATE", "SET", "WHERE"},
		DeleteClauses: []string{"DELETE", "FROM", "WHERE"},
	})
	// The server requires a constant LIMIT/OFFSET, so render them inline rather than as bind vars
	// (GORM's default `clause.Limit` parameterises them).
	db.ClauseBuilders["LIMIT"] = func(c clause.Clause, builder clause.Builder) {
		if limit, ok := c.Expression.(clause.Limit); ok {
			if limit.Limit != nil && *limit.Limit >= 0 {
				builder.WriteString("LIMIT ")
				builder.WriteString(strconv.Itoa(*limit.Limit))
			}
			if limit.Offset > 0 {
				if limit.Limit != nil {
					builder.WriteByte(' ')
				}
				builder.WriteString("OFFSET ")
				builder.WriteString(strconv.Itoa(limit.Offset))
			}
		}
	}
	return nil
}

// Migrator returns a migrator that introspects via SHOW TABLES / SHOW COLUMNS.
func (d Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{
		migrator.Migrator{
			Config: migrator.Config{
				DB:        db,
				Dialector: d,
			},
		},
	}
}

// BindVarTo writes a positional bind marker ($1, $2, …).
func (Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, _ interface{}) {
	writer.WriteByte('$')
	writer.WriteString(fmt.Sprint(len(stmt.Vars)))
}

// QuoteTo double-quotes an identifier, splitting on '.'.
func (Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('"')
	for i := 0; i < len(str); i++ {
		if str[i] == '.' {
			writer.WriteByte('"')
			writer.WriteByte('.')
			writer.WriteByte('"')
		} else {
			writer.WriteByte(str[i])
		}
	}
	writer.WriteByte('"')
}

// Explain renders a statement with its bind values inlined (for logging).
func (Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

// DataTypeOf maps a schema field to a NusaDB column type.
func (Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "BOOL"
	case schema.Int, schema.Uint:
		return "INT"
	case schema.Float:
		return "FLOAT"
	case schema.String:
		return "TEXT"
	case schema.Time:
		return "TIMESTAMP"
	case schema.Bytes:
		return "TEXT"
	default:
		return string(field.DataType)
	}
}

// DefaultValueOf reports the column default (none are generated server-side).
func (Dialector) DefaultValueOf(*schema.Field) clause.Expression {
	return clause.Expr{SQL: "NULL"}
}
