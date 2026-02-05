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

// FindAvailableRegionsOptions represents options for the FindAvailableRegions operation.
type FindAvailableRegionsOptions struct {
	// FilterByOrg filters by organization access. Whether to only return regions that
	// can be used by the caller’s organization.
	FilterByOrg *bool
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

// SetFilterByOrg sets the filter-by-org query parameter.
// Valid values: FilterByOrgEnabled, FilterByOrgDisabled, or empty string.
func (b *FindAvailableRegionsOptionsBuilder) SetFilterByOrg(v bool) *FindAvailableRegionsOptionsBuilder {
	b.Opts = append(b.Opts, func(o *FindAvailableRegionsOptions) {
		o.FilterByOrg = &v
	})
	return b
}
