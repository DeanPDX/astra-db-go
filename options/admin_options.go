// Copyright DataStax, Inc.

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

package options

// RegionType constants for FindAvailableRegions query parameter.
// Use these with SetRegionType() to filter regions by type.
const (
	// RegionTypeAll returns both vector and serverless regions.
	RegionTypeAll = "all"
	// RegionTypeVector returns only vector-capable regions.
	RegionTypeVector = "vector"
)

// FilterByOrg constants for FindAvailableRegions query parameter.
// Use these with SetFilterByOrg() to filter by organization access.
const (
	// FilterByOrgEnabled returns only regions accessible to the organization.
	FilterByOrgEnabled = "enabled"
	// FilterByOrgDisabled returns all regions regardless of organization access.
	FilterByOrgDisabled = "disabled"
)

// FindAvailableRegionsOptions represents options for the FindAvailableRegions operation.
type FindAvailableRegionsOptions struct {
	// RegionType filters regions by type.
	// Valid values: RegionTypeAll, RegionTypeVector, or empty string (serverless only).
	RegionType *string

	// FilterByOrg filters by organization access.
	// Valid values: FilterByOrgEnabled, FilterByOrgDisabled, or empty string.
	FilterByOrg *string
}

// Validate implements the Validator interface for FindAvailableRegionsOptions.
func (o FindAvailableRegionsOptions) Validate() error {
	// No required fields, always valid
	return nil
}

// List implements Builder[FindAvailableRegionsOptions] allowing the raw struct to be
// passed directly to methods that accept ...Builder[FindAvailableRegionsOptions].
func (o *FindAvailableRegionsOptions) List() []func(*FindAvailableRegionsOptions) {
	return []func(*FindAvailableRegionsOptions){
		func(target *FindAvailableRegionsOptions) {
			copyNonNilFields(o, target)
		},
	}
}

// FindAvailableRegionsOptionsBuilder is a builder for FindAvailableRegionsOptions that implements
// Builder[FindAvailableRegionsOptions] following the MongoDB Go driver pattern.
type FindAvailableRegionsOptionsBuilder struct {
	Opts []func(*FindAvailableRegionsOptions)
}

// FindAvailableRegions creates a new FindAvailableRegionsOptionsBuilder.
func FindAvailableRegions() *FindAvailableRegionsOptionsBuilder {
	return &FindAvailableRegionsOptionsBuilder{}
}

// List implements Builder[FindAvailableRegionsOptions].
func (b *FindAvailableRegionsOptionsBuilder) List() []func(*FindAvailableRegionsOptions) {
	return b.Opts
}

// SetRegionType sets the region-type query parameter.
// Valid values: RegionTypeAll, RegionTypeVector, or empty string.
func (b *FindAvailableRegionsOptionsBuilder) SetRegionType(v string) *FindAvailableRegionsOptionsBuilder {
	b.Opts = append(b.Opts, func(o *FindAvailableRegionsOptions) {
		o.RegionType = &v
	})
	return b
}

// SetFilterByOrg sets the filter-by-org query parameter.
// Valid values: FilterByOrgEnabled, FilterByOrgDisabled, or empty string.
func (b *FindAvailableRegionsOptionsBuilder) SetFilterByOrg(v string) *FindAvailableRegionsOptionsBuilder {
	b.Opts = append(b.Opts, func(o *FindAvailableRegionsOptions) {
		o.FilterByOrg = &v
	})
	return b
}
