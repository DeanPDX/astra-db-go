package tests

import (
	"context"

	"github.com/datastax/astra-db-go/internal/integrationtests/harness"
	"github.com/datastax/astra-db-go/options"
)

func init() {
	// Register our tests
	t := []harness.IntegrationTest{
		{Name: "AdminFindAvailableRegionsNoFilter", Run: AdminFindAvailableRegionsNoFilter},
		{Name: "AdminFindAvailableRegionsAll", Run: AdminFindAvailableRegionsAll},
		{Name: "AdminFindAvailableRegionsVector", Run: AdminFindAvailableRegionsVector},
	}
	harness.Register(t...)
}

func AdminFindAvailableRegionsAll(e *harness.TestEnv) error {
	ctx := context.Background()
	client := e.DefaultClient()
	admin := client.Admin()

	_, err := admin.FindAvailableRegions(ctx,
		options.FindAvailableRegions().SetFilterByOrg(true).SetRegionType("all"))
	return err
}

func AdminFindAvailableRegionsNoFilter(e *harness.TestEnv) error {
	ctx := context.Background()
	client := e.DefaultClient()
	admin := client.Admin()

	_, err := admin.FindAvailableRegions(ctx)
	return err
}

func AdminFindAvailableRegionsVector(e *harness.TestEnv) error {
	ctx := context.Background()
	client := e.DefaultClient()
	admin := client.Admin()

	_, err := admin.FindAvailableRegions(ctx,
		options.FindAvailableRegions().SetRegionType("vector").SetFilterByOrg(true))
	return err
}
