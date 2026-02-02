package tests

import (
	"context"

	"github.com/datastax/astra-db-go/internal/integrationtests/harness"
	"github.com/datastax/astra-db-go/options"
)

func init() {
	// Register our tests
	t := []harness.IntegrationTest{
		{Name: "AdminFindAvailableRegions", Run: AdminFindAvailableRegions}}
	harness.Register(t...)
}

func AdminFindAvailableRegions(e *harness.TestEnv) error {
	ctx := context.Background()
	client := e.DefaultClient()
	admin := client.Admin()

	_, err := admin.FindAvailableRegions(ctx,
		options.FindAvailableRegions().SetFilterByOrg(options.FilterByOrgEnabled))
	return err
}
