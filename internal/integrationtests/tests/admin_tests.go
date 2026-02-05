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
		{Name: "AdminFindAvailableRegionsFilterByOrg", Run: AdminFindAvailableRegionsFilterByOrg},
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
