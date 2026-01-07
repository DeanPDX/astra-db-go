// Package harness defines a test harness for running integration tests.
package harness

import (
	"log/slog"
	"sync"

	"github.com/DeanPDX/dotconfig"
	astradb "github.com/datastax/astra-db-go"
	"github.com/datastax/astra-db-go/options"
)

// TestEnv represents our test environment.
type TestEnv struct {
	APIEndpoint      string `env:"API_ENDPOINT"`
	ApplicationToken string `env:"APPLICATION_TOKEN"`
	TestPrefix       string `env:"TEST_PREFIX"`
}

// Environment() retrieves a test environment with config based on environment variables.
func Environment() TestEnv {
	c, err := dotconfig.FromFileName[TestEnv](".env")
	if err != nil {
		slog.Error("dotconfig.FromFileName failed", "error", err)
	}
	return c
}

// DefaultDb returns a Db handle configured with the test environment settings.
func (e *TestEnv) DefaultDb() *astradb.Db {
	client := astradb.NewClient(
		options.WithToken(e.ApplicationToken),
	)
	return client.Database(e.APIEndpoint)
}

// An integration test
type IntegrationTest struct {
	Name string
	Run  func(e *TestEnv) error
}

var (
	testsMu sync.RWMutex // Guards `tests`.
	tests   = make([]IntegrationTest, 0)
)

// Register adds a test(s) to our test runner. The approach is similar to
// how [database/sql] allows you to `Register` SQL drivers.
//
// [database/sql]: https://cs.opensource.google/go/go/+/refs/tags/go1.25.4:src/database/sql/sql.go;l=36
func Register(args ...IntegrationTest) {
	testsMu.Lock()
	defer testsMu.Unlock()
	tests = append(tests, args...)
}

// Tests returns all registered tests.
func Tests() []IntegrationTest {
	// By the time we are running the tests, nothing is adding to this map.
	// But - creating a copy and guarding with `testsMu` just to be extra safe
	// because it's not guaranteed future developers will adhere to this.
	testsMu.Lock()
	defer testsMu.Unlock()
	t := tests
	return t
}
