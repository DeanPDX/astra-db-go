package tests

import (
	"context"
	"fmt"
	"log/slog"

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
	}
	harness.Register(t...)
}

func AdminFindAvailableRegionsNoFilter(e *harness.TestEnv) error {
	ctx := context.Background()
	client := e.DefaultClient()
	admin := client.Admin()

	_, err := admin.FindAvailableRegions(ctx)
	return err
}

func AdminFindAvailableRegionsFilterByOrg(e *harness.TestEnv) error {
	ctx := context.Background()
	client := e.DefaultClient()
	admin := client.Admin()

	_, err := admin.FindAvailableRegions(ctx,
		options.FindAvailableRegions().SetFilterByOrg(true))
	return err
}

func AdminListDatabases(e *harness.TestEnv) error {
	ctx := context.Background()
	client := e.DefaultClient()
	admin := client.Admin()

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
	admin := client.Admin()

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
	admin := client.Admin()

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
	// Note: this is just a pass-through for Admin.GetDatabase,
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
