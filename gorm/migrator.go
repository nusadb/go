package nusadbgorm

import (
	"gorm.io/gorm"
	"gorm.io/gorm/migrator"
)

// Migrator adds NusaDB-specific introspection to GORM's base migrator. The base implementation
// filters `information_schema` by `table_schema = current_database()`, but NusaDB reports every
// table under the fixed schema `public` (never the database name), so those probes never match.
// We override them to use the schema-agnostic `SHOW TABLES` / `SHOW COLUMNS FROM` commands instead.
type Migrator struct {
	migrator.Migrator
}

// HasTable reports whether the table backing `value` exists, via `SHOW TABLES`.
func (m Migrator) HasTable(value interface{}) bool {
	var target string
	if err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		target = stmt.Table
		return nil
	}); err != nil {
		return false
	}

	rows, err := m.DB.Raw("SHOW TABLES").Rows()
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false
		}
		if name == target {
			return true
		}
	}
	return false
}

// HasColumn reports whether `field` (a model field name or raw column name) exists on the table
// backing `value`, via `SHOW COLUMNS FROM`. The base migrator's `information_schema` probe always
// returns false here because of the schema-name mismatch described on [Migrator].
func (m Migrator) HasColumn(value interface{}, field string) bool {
	var query, column string
	if err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		column = field
		if stmt.Schema != nil {
			if f := stmt.Schema.LookUpField(field); f != nil {
				column = f.DBName
			}
		}
		query = "SHOW COLUMNS FROM " + stmt.Quote(stmt.Table)
		return nil
	}); err != nil {
		return false
	}

	rows, err := m.DB.Raw(query).Rows()
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var name, typ, nullable string
		if err := rows.Scan(&name, &typ, &nullable); err != nil {
			return false
		}
		if name == column {
			return true
		}
	}
	return false
}
