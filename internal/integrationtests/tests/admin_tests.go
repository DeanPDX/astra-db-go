package tests

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	astradb "github.com/datastax/astra-db-go"
	"github.com/datastax/astra-db-go/internal/integrationtests/harness"
	"github.com/datastax/astra-db-go/options"
)

func init() {
	// Register our tests
	t := []harness.IntegrationTest{
		{Name: "AdminFindAvailableRegionsNoFilter", Run: AdminFindAvailableRegionsNoFilter},
		{Name: "AdminFindAvailableRegionsFilterByOrg", Run: AdminFindAvailableRegionsFilterByOrg},
		{Name: "AdminListDatabases", Run: AdminListDatabases},
		{Name: "AdminListDatabasesPaginated", Run: AdminListDatabasesPaginated},
		{Name: "AdminCreateDropDatabase", Run: AdminCreateDropDatabase},
		{Name: "AdminKeyspaceCreateListDrop", Run: AdminKeyspaceCreateListDrop},
	}
	harness.Register(t...)
}

func AdminFindAvailableRegionsNoFilter(e *harness.TestEnv) error {
	ctx := context.Background()
	client := e.DefaultClient()
	admin, err := client.Admin()
	if err != nil {
		return fmt.Errorf("Admin() failed: %w", err)
	}

	_, err = admin.FindAvailableRegions(ctx)
	return err
}

func AdminFindAvailableRegionsFilterByOrg(e *harness.TestEnv) error {
	ctx := context.Background()
	client := e.DefaultClient()
	admin, err := client.Admin()
	if err != nil {
		return fmt.Errorf("Admin() failed: %w", err)
	}

	_, err = admin.FindAvailableRegions(ctx,
		options.FindAvailableRegions().SetFilterByOrg(true))
	return err
}

func AdminListDatabases(e *harness.TestEnv) error {
	ctx := context.Background()
	client := e.DefaultClient()
	admin, err := client.Admin()
	if err != nil {
		return fmt.Errorf("Admin() failed: %w", err)
	}

	databases, err := admin.ListDatabases(ctx, options.ListDatabases().SetProvider(options.CloudProviderAzure))
	if err != nil {
		return fmt.Errorf("ListDatabases failed: %w", err)
	}
	slog.Info("Listed databases", "count", len(databases))

	for _, db := range databases {
		slog.Info("Database", "id", db.ID, "name", db.Info.Name, "status", db.Status, "provider", db.Info.CloudProvider, "region", db.Info.Region)
	}

	return nil
}

func AdminListDatabasesPaginated(e *harness.TestEnv) error {
	ctx := context.Background()
	client := e.DefaultClient()
	admin, err := client.Admin()
	if err != nil {
		return fmt.Errorf("Admin() failed: %w", err)
	}

	// Using low page-size to try to  ensure pagination is exercised in tests.
	pageSize := 5
	var all []astradb.Database
	opts := options.ListDatabases().SetInclude(options.DatabaseIncludeAll).SetLimit(pageSize)

	for page := 1; ; page++ {
		databases, err := admin.ListDatabases(ctx, opts)
		if err != nil {
			return fmt.Errorf("ListDatabases page %d failed: %w", page, err)
		}
		slog.Info("Listed databases page", "page", page, "count", len(databases))
		all = append(all, databases...)

		if len(databases) < pageSize {
			break
		}
		// Set up cursor for next page
		opts.SetStartingAfter(databases[len(databases)-1].ID)
	}

	slog.Info("Total databases found via pagination", "count", len(all))
	for _, db := range all {
		slog.Info("Database", "id", db.ID, "name", db.Info.Name, "status", db.Status)
	}

	return nil
}

func AdminCreateDropDatabase(e *harness.TestEnv) error {
	ctx := context.Background()
	client := e.DefaultClient()
	admin, err := client.Admin()
	if err != nil {
		return fmt.Errorf("Admin() failed: %w", err)
	}

	// Create a database (non-blocking to keep test fast)
	dbInfo := astradb.DatabaseInfo{
		Name:          "go-sdk-integration-test",
		CloudProvider: "gcp",
		Region:        "us-east1",
	}

	slog.Info("Creating database", "name", dbInfo.Name, "provider", dbInfo.CloudProvider, "region", dbInfo.Region)
	dbAdmin, err := admin.CreateDatabase(ctx, dbInfo,
		options.CreateDatabase())
	if err != nil {
		return fmt.Errorf("CreateDatabase failed: %w", err)
	}
	slog.Info("Database creation initiated", "id", dbAdmin.ID())

	// Verify we can get the database info via DbAdmin.
	// Note: this is just a pass-through for AstraAdmin.GetDatabase,
	// so we don't need to also test that function directly.
	db, err := dbAdmin.Info(ctx)
	if err != nil {
		return fmt.Errorf("DbAdmin.Info failed: %w", err)
	}
	slog.Info("Database info retrieved", "id", dbAdmin.ID(), "status", db.Status, "name", db.Info.Name)

	// Drop the database via DbAdmin (non-blocking)
	slog.Info("Dropping database (non-blocking)", "id", dbAdmin.ID())
	err = dbAdmin.Drop(ctx,
		options.DropDatabase().SetBlocking(false))
	if err != nil {
		return fmt.Errorf("DbAdmin.Drop failed: %w", err)
	}
	slog.Info("Database drop initiated", "id", dbAdmin.ID())

	return nil
}

func AdminKeyspaceCreateListDrop(e *harness.TestEnv) error {
	ctx := context.Background()
	client := e.DefaultClient()
	admin, err := client.Admin()
	if err != nil {
		return fmt.Errorf("Admin() failed: %w", err)
	}

	// Find an active database to test against
	databases, err := admin.ListDatabases(ctx,
		options.ListDatabases().SetInclude(options.DatabaseIncludeActive).SetLimit(1))
	if err != nil {
		return fmt.Errorf("ListDatabases failed: %w", err)
	}
	if len(databases) == 0 {
		return fmt.Errorf("no active databases found to test keyspace operations")
	}

	db := databases[0]
	slog.Info("Using database for keyspace tests", "id", db.ID, "name", db.Info.Name, "region", db.Info.Region)

	// Get an DbAdmin for this database
	dbAdmin := admin.DbAdmin(db.ID)

	// Create a test keyspace with a unique name
	ksName := fmt.Sprintf("go_sdk_test_ks_%d", time.Now().UnixMilli())
	slog.Info("Creating keyspace", "name", ksName)
	err = dbAdmin.CreateKeyspace(ctx, ksName)
	if err != nil {
		return fmt.Errorf("CreateKeyspace failed: %w", err)
	}
	slog.Info("Keyspace created", "name", ksName)

	// List keyspaces and verify ours exists
	keyspaces, err := dbAdmin.ListKeyspaces(ctx)
	if err != nil {
		return fmt.Errorf("ListKeyspaces failed: %w", err)
	}
	slog.Info("Listed keyspaces", "count", len(keyspaces), "keyspaces", keyspaces)

	found := false
	for _, ks := range keyspaces {
		if ks == ksName {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("keyspace %q not found in ListKeyspaces result", ksName)
	}

	// Drop the keyspace
	slog.Info("Dropping keyspace", "name", ksName)
	err = dbAdmin.DropKeyspace(ctx, ksName)
	if err != nil {
		return fmt.Errorf("DropKeyspace failed: %w", err)
	}
	slog.Info("Keyspace dropped", "name", ksName)

	return nil
}
