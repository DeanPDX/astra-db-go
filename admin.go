// Copyright DataStax, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package astradb

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/datastax/astra-db-go/options"
)

// DefaultAdminAPIVersion is the default version of the Astra DevOps API.
const DefaultAdminAPIVersion = "v2"

// AstraAdmin provides access to Astra DevOps API operations.
// Obtain an AstraAdmin instance from DataAPIClient.Admin().
// Only valid for Astra environments.
type AstraAdmin struct {
	client      *DataAPIClient
	options     *options.APIOptions
	apiVersion  string
	environment options.Environment
}

func (a *AstraAdmin) createCommand(method string, path string, payload any) *adminCommand {
	return &adminCommand{
		admin:       a,
		method:      method,
		path:        path,
		payload:     payload,
		queryParams: url.Values{},
	}
}

// Region represents an available serverless region from the DevOps API.
type Region struct {
	// Classification indicates the region's classification level (e.g., "standard").
	Classification string `json:"classification"`
	// CloudProvider is the cloud provider (e.g., "aws", "gcp", "azure").
	CloudProvider string `json:"cloudProvider"`
	// DisplayName is the human-readable name of the region.
	DisplayName string `json:"displayName"`
	// Enabled indicates whether the region is currently available.
	Enabled bool `json:"enabled"`
	// Name is the region identifier used in API calls.
	Name string `json:"name"`
	// RegionType indicates the type of region (e.g., "serverless", "vector").
	RegionType string `json:"region_type"`
	// ReservedForQualifiedUsers indicates if region is restricted.
	ReservedForQualifiedUsers bool `json:"reservedForQualifiedUsers"`
	// Zone is the geographic zone (e.g., "na", "eu", "apac").
	Zone string `json:"zone"`
}

// DatabaseStatus represents the status of an Astra database.
type DatabaseStatus string

const (
	// DatabaseStatusActive indicates the database is ready for use.
	DatabaseStatusActive DatabaseStatus = "ACTIVE"
	// DatabaseStatusPending indicates the database creation is pending.
	DatabaseStatusPending DatabaseStatus = "PENDING"
	// DatabaseStatusInitializing indicates the database is being initialized.
	DatabaseStatusInitializing DatabaseStatus = "INITIALIZING"
	// DatabaseStatusTerminating indicates the database is being terminated.
	DatabaseStatusTerminating DatabaseStatus = "TERMINATING"
	// DatabaseStatusTerminated indicates the database has been terminated.
	DatabaseStatusTerminated DatabaseStatus = "TERMINATED"
	// DatabaseStatusMaintenance indicates the database is under maintenance.
	DatabaseStatusMaintenance DatabaseStatus = "MAINTENANCE"
)

// Database represents full database information from the DevOps API.
type Database struct {
	// ID is the unique database identifier.
	ID string `json:"id"`
	// OrgID is the organization identifier.
	OrgID string `json:"orgId"`
	// OwnerID is the owner's identifier.
	OwnerID string `json:"ownerId"`
	// Info contains database configuration details.
	Info DatabaseDetails `json:"info"`
	// CreationTime is when the database was created.
	CreationTime string `json:"creationTime"`
	// TerminationTime is when the database was terminated (if applicable).
	TerminationTime string `json:"terminationTime,omitempty"`
	// Status is the current database status.
	Status DatabaseStatus `json:"status"`
}

// DatabaseDetails contains the nested info object from the database response.
type DatabaseDetails struct {
	// Name is the database name.
	Name string `json:"name"`
	// Keyspace is the default keyspace.
	Keyspace string `json:"keyspace"`
	// CloudProvider is the cloud provider.
	CloudProvider string `json:"cloudProvider"`
	// Region is the deployment region.
	Region string `json:"region"`
	// AdditionalKeyspaces lists extra keyspaces.
	AdditionalKeyspaces []string `json:"additionalKeyspaces"`
	// DbType is the database type (e.g., "vector").
	DbType string `json:"dbType"`
}

// resolveOptions merges AstraAdmin options with client options.
func (a *AstraAdmin) resolveOptions() *options.APIOptions {
	var clientOpts *options.APIOptions
	if a.client != nil {
		clientOpts = a.client.Options()
	}
	return options.Merge(clientOpts, a.options)
}

// FindAvailableRegions retrieves available serverless regions from the DevOps API.
//
// Example - get all regions:
//
//	admin, err := client.Admin()
//	regions, err := admin.FindAvailableRegions(ctx)
//
// Example - filter by organization access:
//
//	regions, err := admin.FindAvailableRegions(ctx,
//	    options.FindAvailableRegions().SetFilterByOrg(true))
func (a *AstraAdmin) FindAvailableRegions(ctx context.Context, opts ...options.Builder[options.FindAvailableRegionsOptions]) ([]Region, error) {
	// Merge options
	merged, err := options.MergeOptions(opts...)
	if err != nil {
		return nil, err
	}

	// Build command with query parameters.
	// Hard-coding to region-type=vector because classic isn't relevant to this client.
	cmd := a.createCommand(http.MethodGet, "/regions/serverless", nil).
		withQueryParam("region-type", "vector")
	if merged != nil {
		if merged.FilterByOrg != nil && *merged.FilterByOrg {
			cmd.withQueryParam("filter-by-org", "enabled")
		}
	}

	// Execute request
	resp, err := cmd.execute(ctx)
	if err != nil {
		return nil, err
	}

	// Parse response - the API returns a JSON array of regions
	var regions []Region
	if err := json.Unmarshal(resp.Body, &regions); err != nil {
		return nil, fmt.Errorf("failed to parse regions response: %w", err)
	}

	return regions, nil
}

// ListDatabases retrieves databases accessible to the caller.
//
// By default, only non-terminated databases are returned (up to 25).
// Use SetLimit (up to 100) and SetStartingAfter to control pagination.
//
// Example - list databases:
//
//	admin, err := client.Admin()
//	databases, err := admin.ListDatabases(ctx)
//
// Example - list only active GCP databases:
//
//	databases, err := admin.ListDatabases(ctx,
//	    options.ListDatabases().
//	        SetInclude(options.DatabaseIncludeActive).
//	        SetProvider(options.CloudProviderGCP))
//
// Example - paginate through results and retrieve all databases:
//
//	func listAll(ctx context.Context, admin *astradb.AstraAdmin) ([]astradb.Database, error) {
//		var all []astradb.Database
//		pageSize := 100
//		opts := options.ListDatabases().SetInclude(options.DatabaseIncludeAll).SetLimit(pageSize)
//		for {
//			databases, err := admin.ListDatabases(ctx, opts)
//			if err != nil {
//				return nil, fmt.Errorf("admin.ListDatabases failed: %w", err)
//			}
//			all = append(all, databases...)
//			if len(databases) < pageSize {
//				break
//			}
//			// Set up cursor for next page
//			opts.SetStartingAfter(databases[len(databases)-1].ID)
//		}
//		return all, nil
//	}
func (a *AstraAdmin) ListDatabases(ctx context.Context, opts ...options.Builder[options.ListDatabasesOptions]) ([]Database, error) {
	merged, err := options.MergeOptions(opts...)
	if err != nil {
		return nil, err
	}

	cmd := a.createCommand(http.MethodGet, "/databases", nil)
	if merged != nil {
		if merged.Include != nil {
			cmd.withQueryParam("include", string(*merged.Include))
		}
		if merged.Provider != nil {
			cmd.withQueryParam("provider", string(*merged.Provider))
		}
		if merged.Limit != nil {
			cmd.withQueryParam("limit", fmt.Sprintf("%d", *merged.Limit))
		}
		if merged.StartingAfter != nil {
			cmd.withQueryParam("starting_after", *merged.StartingAfter)
		}
	}

	resp, err := cmd.execute(ctx)
	if err != nil {
		return nil, err
	}

	var databases []Database
	if err := json.Unmarshal(resp.Body, &databases); err != nil {
		return nil, fmt.Errorf("failed to parse databases response: %w", err)
	}

	return databases, nil
}

// GetDatabase retrieves information about a specific database.
//
// Example:
//
//	admin, err := client.Admin()
//	db, err := admin.GetDatabase(ctx, "database-id")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println("Status:", db.Status)
func (a *AstraAdmin) GetDatabase(ctx context.Context, databaseID string) (*Database, error) {
	cmd := a.createCommand(http.MethodGet, "/databases/"+databaseID, nil)
	resp, err := cmd.execute(ctx)
	if err != nil {
		return nil, err
	}

	var db Database
	if err := json.Unmarshal(resp.Body, &db); err != nil {
		return nil, fmt.Errorf("failed to parse database response: %w", err)
	}

	return &db, nil
}

// extractDevOpsError handles error responses from the DevOps API.
func (a *AstraAdmin) extractDevOpsError(statusCode int, body []byte) error {
	// Try to parse as a structured error
	var devOpsErr struct {
		Message string   `json:"message"`
		Errors  []string `json:"errors"`
	}
	if err := json.Unmarshal(body, &devOpsErr); err == nil && devOpsErr.Message != "" {
		return fmt.Errorf("DevOps API error (status %d): %s", statusCode, devOpsErr.Message)
	}

	// Fallback to raw body
	return fmt.Errorf("DevOps API error (status %d): %s", statusCode, string(body))
}

// DatabaseInfo contains the required parameters for creating a database.
type DatabaseInfo struct {
	// Name is the database name. Must start and end with a letter or number.
	// Can contain letters, numbers, and special characters: & + - _ ( ) < > . , @
	// Cannot exceed 50 characters.
	Name string
	// CloudProvider is the cloud provider (e.g., "aws", "gcp", "azure").
	CloudProvider string
	// Region is the cloud provider region for the database location.
	Region string
}

// createDatabaseRequest is the request payload for the create database API.
type createDatabaseRequest struct {
	Name          string `json:"name"`
	CloudProvider string `json:"cloudProvider"`
	Region        string `json:"region"`
	Keyspace      string `json:"keyspace,omitempty"`
	DbType        string `json:"dbType"`
	Tier          string `json:"tier"`
	CapacityUnits int    `json:"capacityUnits"`
}

// DbAdmin returns a DbAdmin handle for the given database ID.
//
// No API calls are made; this simply creates a handle for performing
// admin operations on the specified database.
//
// Example:
//
//	dbAdmin := admin.DbAdmin("database-id")
//	keyspaces, err := dbAdmin.ListKeyspaces(ctx)
func (a *AstraAdmin) DbAdmin(databaseID string) *DbAdmin {
	return &DbAdmin{
		id:    databaseID,
		admin: a,
	}
}

// CreateDatabase creates a new serverless vector database and returns an
// [DbAdmin] for performing admin operations on it.
//
// The DevOps API endpoint is: POST https://api.astra.datastax.com/v2/databases
//
// By default, this method blocks until the database reaches ACTIVE status (typically
// about 2 minutes). Use SetBlocking(false) to return immediately after the creation
// request is accepted.
//
// Example - create a database (blocking by default):
//
//	admin, err := client.Admin()
//	dbAdmin, err := admin.CreateDatabase(ctx, astradb.DatabaseInfo{
//	    Name:          "my-database",
//	    CloudProvider: "gcp",
//	    Region:        "us-east1",
//	})
//
// Example - create without waiting:
//
//	dbAdmin, err := admin.CreateDatabase(ctx, astradb.DatabaseInfo{
//	    Name:          "my-database",
//	    CloudProvider: "gcp",
//	    Region:        "us-east1",
//	}, options.CreateDatabase().SetBlocking(false))
//
// Example - create with custom keyspace and poll interval:
//
//	dbAdmin, err := admin.CreateDatabase(ctx, astradb.DatabaseInfo{
//	    Name:          "my-database",
//	    CloudProvider: "aws",
//	    Region:        "us-east-1",
//	}, options.CreateDatabase().
//	    SetKeyspace("my_keyspace").
//	    SetPollInterval(5 * time.Second))
func (a *AstraAdmin) CreateDatabase(ctx context.Context, info DatabaseInfo, opts ...options.Builder[options.CreateDatabaseOptions]) (*DbAdmin, error) {
	// Merge options
	merged, err := options.MergeOptions(opts...)
	if err != nil {
		return nil, err
	}

	// Build request payload
	payload := createDatabaseRequest{
		Name:          info.Name,
		CloudProvider: info.CloudProvider,
		Region:        info.Region,
		DbType:        "vector",
		Tier:          "serverless",
		CapacityUnits: 1,
	}
	if merged != nil && merged.Keyspace != nil {
		payload.Keyspace = *merged.Keyspace
	}

	// Execute request
	cmd := a.createCommand(http.MethodPost, "/databases", payload)
	httpResp, err := cmd.execute(ctx)
	if err != nil {
		return nil, err
	}

	// Database ID is in the location header.
	dbID := httpResp.Headers.Get("Location")
	if dbID == "" {
		return nil, fmt.Errorf("missing Location header in response")
	}

	dbAdmin := a.DbAdmin(dbID)

	// Determine blocking behavior (default: true)
	blocking := true
	if merged != nil && merged.Blocking != nil {
		blocking = *merged.Blocking
	}

	if !blocking {
		return dbAdmin, nil
	}

	// Poll until database is ACTIVE
	pollInterval := options.DefaultDatabasePollInterval
	if merged != nil && merged.PollInterval != nil {
		pollInterval = *merged.PollInterval
	}

	slog.Debug("Waiting for database to become ACTIVE", "id", dbID, "pollInterval", pollInterval)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return dbAdmin, ctx.Err()
		case <-ticker.C:
			db, err := a.GetDatabase(ctx, dbID)
			if err != nil {
				return dbAdmin, fmt.Errorf("failed to get database status: %w", err)
			}
			slog.Debug("Database status", "id", dbID, "status", db.Status)
			if db.Status == DatabaseStatusActive {
				return dbAdmin, nil
			}
			if db.Status == DatabaseStatusTerminated || db.Status == DatabaseStatusTerminating {
				return dbAdmin, fmt.Errorf("database entered unexpected status: %s", db.Status)
			}
		}
	}
}

// DropDatabase terminates a database, permanently deleting all of its data.
//
// The DevOps API endpoint is: POST https://api.astra.datastax.com/v2/databases/{id}/terminate
//
// By default, this method blocks until the database is fully terminated (typically
// about 6-7 minutes). Use SetBlocking(false) to return immediately after the termination
// request is accepted.
//
// WARNING: This action cannot be undone. All data, including automatic backups, will be
// permanently deleted.
//
// Example - drop database (blocking by default):
//
//	admin, err := client.Admin()
//	err = admin.DropDatabase(ctx, "database-id")
//
// Example - drop without waiting:
//
//	err := admin.DropDatabase(ctx, "database-id",
//	    options.DropDatabase().SetBlocking(false))
func (a *AstraAdmin) DropDatabase(ctx context.Context, databaseID string, opts ...options.Builder[options.DropDatabaseOptions]) error {
	// Merge options
	merged, err := options.MergeOptions(opts...)
	if err != nil {
		return err
	}

	cmd := a.createCommand(http.MethodPost, "/databases/"+databaseID+"/terminate", nil)
	_, err = cmd.execute(ctx)
	if err != nil {
		return err
	}

	// Determine blocking behavior (default: true)
	blocking := true
	if merged != nil && merged.Blocking != nil {
		blocking = *merged.Blocking
	}

	if !blocking {
		return nil
	}

	// Poll until database is terminated or gone
	pollInterval := options.DefaultDatabasePollInterval
	if merged != nil && merged.PollInterval != nil {
		pollInterval = *merged.PollInterval
	}

	slog.Debug("Waiting for database to be terminated", "id", databaseID, "pollInterval", pollInterval)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			db, err := a.GetDatabase(ctx, databaseID)
			if err != nil {
				// If we get a 404 or similar, the database is gone
				slog.Debug("Database no longer accessible (likely terminated)", "id", databaseID, "error", err)
				return nil
			}
			slog.Debug("Database status", "id", databaseID, "status", db.Status)
			if db.Status == DatabaseStatusTerminated {
				return nil
			}
		}
	}
}
