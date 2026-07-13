module github.com/nusadb/go/gorm

go 1.24

require (
	github.com/nusadb/go v0.1.0
	gorm.io/gorm v1.30.0
)

require (
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	golang.org/x/text v0.21.0 // indirect
)

// Local development builds resolve the root driver from the working tree. Consumers ignore
// `replace` directives in a dependency's go.mod, so they get the required v0.1.0 from the
// `v0.1.0` tag instead.
replace github.com/nusadb/go => ../
