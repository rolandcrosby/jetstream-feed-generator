package db

import (
	_ "embed"
)

//go:embed migrations/schema.sql
var SchemaSQL string
