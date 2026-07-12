package nusadbgorm_test

import (
	"fmt"
	"testing"

	nusadbgorm "github.com/nusadb/nusadb-go/gorm"
	"gorm.io/gorm"
)

// Category and Item are a parent/child pair used to exercise GORM joins.
type Category struct {
	ID    uint `gorm:"primaryKey;autoIncrement:false"`
	Label string
}

type Item struct {
	ID         uint `gorm:"primaryKey;autoIncrement:false"`
	CategoryID uint
	Name       string
}

// TestGormQueryOperations exercises the query surface GORM users rely on beyond plain CRUD:
// pagination (LIMIT/OFFSET), WHERE, aggregates (Count/sum/min/max/avg), GROUP BY + HAVING, and a
// raw JOIN. Each must round-trip against a real server.
func TestGormQueryOperations(t *testing.T) {
	port, stop := startServer(t)
	defer stop()

	dsn := fmt.Sprintf("nusadb://u@127.0.0.1:%d/nusadb", port)
	db, err := gorm.Open(nusadbgorm.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm open: %v", err)
	}
	if err := db.AutoMigrate(&Product{}); err != nil {
		t.Fatalf("automigrate product: %v", err)
	}

	// Migrator introspection: HasTable / HasColumn must reflect reality (the base migrator's
	// information_schema probe mismatches NusaDB's `public` schema and always returns false).
	mig := db.Migrator()
	if !mig.HasTable(&Product{}) {
		t.Fatal("HasTable(Product) = false, want true")
	}
	if !mig.HasColumn(&Product{}, "Price") {
		t.Fatal("HasColumn(Product, Price) = false, want true")
	}
	if mig.HasColumn(&Product{}, "nonexistent") {
		t.Fatal("HasColumn(Product, nonexistent) = true, want false")
	}

	for i, name := range []string{"a", "b", "c", "d"} {
		p := Product{ID: uint(i + 1), Name: name, Price: (i + 1) * 10}
		if err := db.Create(&p).Error; err != nil {
			t.Fatalf("seed product: %v", err)
		}
	}

	// Pagination: ordered LIMIT 2 OFFSET 1 → ids 2,3.
	var page []Product
	if err := db.Order("id").Limit(2).Offset(1).Find(&page).Error; err != nil {
		t.Fatalf("limit/offset: %v", err)
	}
	if len(page) != 2 || page[0].ID != 2 || page[1].ID != 3 {
		t.Fatalf("limit/offset got %+v, want ids [2 3]", page)
	}

	// WHERE + Count.
	var n int64
	if err := db.Model(&Product{}).Where("price >= ?", 20).Count(&n).Error; err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 3 {
		t.Fatalf("count where price>=20 = %d, want 3", n)
	}

	// Aggregates via Select+Scan (prices 10,20,30,40).
	var agg struct {
		Total int
		Min   int
		Max   int
		Avg   float64
	}
	if err := db.Model(&Product{}).
		Select("sum(price) as total, min(price) as min, max(price) as max, avg(price) as avg").
		Scan(&agg).Error; err != nil {
		t.Fatalf("aggregate scan: %v", err)
	}
	if agg.Total != 100 || agg.Min != 10 || agg.Max != 40 || agg.Avg != 25 {
		t.Fatalf("aggregates got %+v, want total=100 min=10 max=40 avg=25", agg)
	}

	// GROUP BY + HAVING: bucket prices by parity, keep buckets with >=2 rows.
	type bucket struct {
		Parity int
		Cnt    int
	}
	var buckets []bucket
	if err := db.Model(&Product{}).
		Select("price % 20 as parity, count(*) as cnt").
		Group("price % 20").
		Having("count(*) >= ?", 2).
		Order("parity").
		Scan(&buckets).Error; err != nil {
		t.Fatalf("group/having: %v", err)
	}
	if len(buckets) != 2 || buckets[0].Cnt != 2 || buckets[1].Cnt != 2 {
		t.Fatalf("group/having got %+v, want two buckets of 2", buckets)
	}

	// JOIN across two tables.
	if err := db.AutoMigrate(&Category{}, &Item{}); err != nil {
		t.Fatalf("automigrate join tables: %v", err)
	}
	for _, c := range []Category{{ID: 1, Label: "fruit"}, {ID: 2, Label: "veg"}} {
		if err := db.Create(&c).Error; err != nil {
			t.Fatalf("seed category: %v", err)
		}
	}
	for _, it := range []Item{{ID: 1, CategoryID: 1, Name: "apple"}, {ID: 2, CategoryID: 2, Name: "kale"}, {ID: 3, CategoryID: 99, Name: "orphan"}} {
		if err := db.Create(&it).Error; err != nil {
			t.Fatalf("seed item: %v", err)
		}
	}
	type joined struct {
		Name  string
		Label string
	}
	var rows []joined
	if err := db.Table("items").
		Select("items.name as name, categories.label as label").
		Joins("JOIN categories ON categories.id = items.category_id").
		Order("items.id").
		Scan(&rows).Error; err != nil {
		t.Fatalf("join: %v", err)
	}
	if len(rows) != 2 || rows[0].Name != "apple" || rows[0].Label != "fruit" || rows[1].Name != "kale" {
		t.Fatalf("join got %+v, want apple/fruit + kale/veg", rows)
	}
}
