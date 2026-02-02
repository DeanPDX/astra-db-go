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
	"io"
	"net/http"
	"net/url"

	"github.com/datastax/astra-db-go/options"
)

const (
	// DevOpsAPIBaseURL is the base URL for the Astra DevOps API.
	DevOpsAPIBaseURL = "https://api.astra.datastax.com"
)

// Admin provides access to Astra DevOps API operations.
// Obtain an Admin instance from DataAPIClient.Admin().
type Admin struct {
	client  *DataAPIClient
	options *options.APIOptions
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

// resolveOptions merges Admin options with client options.
func (a *Admin) resolveOptions() *options.APIOptions {
	var clientOpts *options.APIOptions
	if a.client != nil {
		clientOpts = a.client.Options()
	}
	return options.Merge(clientOpts, a.options)
}

// FindAvailableRegions retrieves available serverless regions from the DevOps API.
//
// The DevOps API endpoint is: GET https://api.astra.datastax.com/v2/regions/serverless
//
// Example - get all regions:
//
//	admin := client.Admin()
//	regions, err := admin.FindAvailableRegions(ctx)
//
// Example - get only vector regions:
//
//	regions, err := admin.FindAvailableRegions(ctx,
//	    options.FindAvailableRegions().SetRegionType(options.RegionTypeVector))
//
// Example - filter by organization access:
//
//	regions, err := admin.FindAvailableRegions(ctx,
//	    options.FindAvailableRegions().SetFilterByOrg(options.FilterByOrgEnabled))
func (a *Admin) FindAvailableRegions(ctx context.Context, opts ...options.Builder[options.FindAvailableRegionsOptions]) ([]Region, error) {
	// Merge options
	merged, err := options.MergeOptions(opts...)
	if err != nil {
		return nil, err
	}

	// Build URL with query parameters
	reqURL, err := url.Parse(DevOpsAPIBaseURL + "/v2/regions/serverless")
	if err != nil {
		return nil, err
	}

	q := reqURL.Query()
	if merged != nil {
		if merged.RegionType != nil && *merged.RegionType != "" {
			q.Set("region-type", *merged.RegionType)
		}
		if merged.FilterByOrg != nil && *merged.FilterByOrg != "" {
			q.Set("filter-by-org", *merged.FilterByOrg)
		}
	}
	reqURL.RawQuery = q.Encode()

	// Create request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return nil, err
	}

	// Set authentication - DevOps API uses Bearer token
	resolvedOpts := a.resolveOptions()
	token := resolvedOpts.GetToken()
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Accept", "application/json")

	// Add any custom headers
	for key, value := range resolvedOpts.Headers {
		req.Header.Set(key, value)
	}

	// Execute request
	httpClient := resolvedOpts.GetHTTPClient()
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Handle error responses
	if resp.StatusCode >= 400 {
		return nil, a.extractDevOpsError(resp.StatusCode, body)
	}

	// Parse response - the API returns a JSON array of regions
	var regions []Region
	if err := json.Unmarshal(body, &regions); err != nil {
		return nil, fmt.Errorf("failed to parse regions response: %w", err)
	}

	return regions, nil
}

// extractDevOpsError handles error responses from the DevOps API.
func (a *Admin) extractDevOpsError(statusCode int, body []byte) error {
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
