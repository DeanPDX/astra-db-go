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
	"encoding/json"
	"testing"

	"github.com/datastax/astra-db-go/options"
)

const sampleRegionsResponse = `[
	{
		"classification": "general-purpose",
		"cloudProvider": "aws",
		"displayName": "US East (N. Virginia)",
		"enabled": true,
		"name": "us-east-1",
		"region_type": "vector",
		"reservedForQualifiedUsers": false,
		"zone": "na"
	},
	{
		"classification": "general-purpose",
		"cloudProvider": "gcp",
		"displayName": "US Central (Iowa)",
		"enabled": true,
		"name": "us-central1",
		"region_type": "serverless",
		"reservedForQualifiedUsers": false,
		"zone": "na"
	}
]`

func TestRegionUnmarshal(t *testing.T) {
	var regions []Region
	if err := json.Unmarshal([]byte(sampleRegionsResponse), &regions); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if len(regions) != 2 {
		t.Fatalf("expected 2 regions, got %d", len(regions))
	}

	// Verify first region
	region := regions[0]
	if region.Name != "us-east-1" {
		t.Errorf("expected name 'us-east-1', got %s", region.Name)
	}
	if region.CloudProvider != "aws" {
		t.Errorf("expected cloudProvider 'aws', got %s", region.CloudProvider)
	}
	if region.DisplayName != "US East (N. Virginia)" {
		t.Errorf("expected displayName 'US East (N. Virginia)', got %s", region.DisplayName)
	}
	if !region.Enabled {
		t.Error("expected enabled to be true")
	}
	if region.RegionType != "vector" {
		t.Errorf("expected region_type 'vector', got %s", region.RegionType)
	}
	if region.Classification != "general-purpose" {
		t.Errorf("expected classification 'general-purpose', got %s", region.Classification)
	}
	if region.Zone != "na" {
		t.Errorf("expected zone 'na', got %s", region.Zone)
	}
	if region.ReservedForQualifiedUsers {
		t.Error("expected reservedForQualifiedUsers to be false")
	}

	// Verify second region
	region2 := regions[1]
	if region2.Name != "us-central1" {
		t.Errorf("expected name 'us-central1', got %s", region2.Name)
	}
	if region2.CloudProvider != "gcp" {
		t.Errorf("expected cloudProvider 'gcp', got %s", region2.CloudProvider)
	}
}

func TestFindAvailableRegionsOptionsBuilder(t *testing.T) {
	t.Run("filter by org", func(t *testing.T) {
		opts := options.FindAvailableRegions().SetFilterByOrg(true)
		merged, err := options.MergeOptions(opts)
		if err != nil {
			t.Fatalf("MergeOptions: %v", err)
		}
		if merged.FilterByOrg == nil || *merged.FilterByOrg != true {
			t.Error("expected FilterByOrg to be true")
		}
	})

	t.Run("combined options", func(t *testing.T) {
		opts := options.FindAvailableRegions().
			SetFilterByOrg(true)
		merged, err := options.MergeOptions(opts)
		if err != nil {
			t.Fatalf("MergeOptions: %v", err)
		}
		if merged.FilterByOrg == nil || *merged.FilterByOrg != true {
			t.Error("expected FilterByOrg to be true")
		}
	})
}

func TestAdminResolveOptions(t *testing.T) {
	// Verify that Admin inherits options from client
	client := NewClient(options.WithToken("client-token"))
	admin := client.Admin()

	opts := admin.resolveOptions()
	if opts.GetToken() != "client-token" {
		t.Errorf("expected token 'client-token', got %s", opts.GetToken())
	}
}

func TestAdminOptionOverride(t *testing.T) {
	// Verify that Admin-level options override client options
	client := NewClient(options.WithToken("client-token"))
	admin := client.Admin(options.WithToken("admin-token"))

	opts := admin.resolveOptions()
	if opts.GetToken() != "admin-token" {
		t.Errorf("expected token 'admin-token', got %s", opts.GetToken())
	}
}

func TestFindAvailableRegionsOptionsStruct(t *testing.T) {
	// Test that the raw struct can be used directly (implements Builder)
	opts := &options.FindAvailableRegionsOptions{
		FilterByOrg: boolPtr(true),
	}

	merged, err := options.MergeOptions(opts)
	if err != nil {
		t.Fatalf("MergeOptions: %v", err)
	}
	if merged.FilterByOrg == nil || *merged.FilterByOrg != true {
		t.Error("expected FilterByOrg to be true")
	}
}

/*

curl -sS -L -X GET "https://api.astra.datastax.com/v2/regions/serverless?region-type=REGION_TYPE&filter-by-org=FILTER_BY_ORG" \
--header "Authorization: Bearer APPLICATION_TOKEN" \
--header "Content-Type: application/json"
*/

func TestSTuff(t *testing.T) {
	// Verify that Admin inherits options from client
	client := NewClient(options.WithToken("client-token"))
	admin := client.Admin()
	cmd := admin.createCommand("GET", "/regions/serverless", nil)
	url, err := cmd.url()
	if err != nil {
		t.Fatalf("cmd.url() producted unexpected error: %v", err)
	}
	expectedURL := "https://api.astra.datastax.com/v2/regions/serverless"
	if url != expectedURL {
		t.Errorf("expected: %s\ngot: %s", expectedURL, url)
	}
}

/*
curl -sS -L -X GET "https://api.astra.datastax.com/v2/databases?include=STRING&provider=STRING&starting_after=STRING&limit=INTEGER" \
--header "Authorization: Bearer APPLICATION_TOKEN" \
--header "Content-Type: application/json"
*/
