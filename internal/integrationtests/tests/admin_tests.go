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
