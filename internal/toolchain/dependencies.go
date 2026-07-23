// Package toolchain keeps approved runtime dependencies compiled and checksum-pinned
// before business slices begin.
package toolchain

import (
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	_ *migrate.Migrate
	_ *pgxpool.Pool
	_ prometheus.Collector
)
