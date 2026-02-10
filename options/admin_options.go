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

import "time"

// DefaultDatabasePollInterval is the default interval for polling database status.
const DefaultDatabasePollInterval = 10 * time.Second

// DefaultKeyspacePollInterval is the default interval for polling keyspace operations.
const DefaultKeyspacePollInterval = 1 * time.Second

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

// CreateDatabaseOptions represents options for the CreateDatabase operation.
type CreateDatabaseOptions struct {
	// Keyspace is the initial keyspace name. Defaults to "default_keyspace" if not specified.
	Keyspace *string
	// Blocking controls whether to wait for the database to become ACTIVE.
	// Defaults to true.
	Blocking *bool
	// PollInterval is how often to check the database status when blocking.
	// Defaults to DefaultDatabasePollInterval (10 seconds).
	PollInterval *time.Duration
}

// Validate implements the Validator interface for CreateDatabaseOptions.
func (o CreateDatabaseOptions) Validate() error {
	return nil
}

// List implements Builder[CreateDatabaseOptions] allowing the raw struct to be
// passed directly to methods that accept ...Builder[CreateDatabaseOptions].
func (o *CreateDatabaseOptions) List() []func(*CreateDatabaseOptions) {
	return []func(*CreateDatabaseOptions){
		func(target *CreateDatabaseOptions) {
			copyNonNilFields(o, target)
		},
	}
}

// CreateDatabaseOptionsBuilder is a builder for CreateDatabaseOptions.
type CreateDatabaseOptionsBuilder struct {
	Opts []func(*CreateDatabaseOptions)
}

// CreateDatabase creates a new CreateDatabaseOptionsBuilder.
func CreateDatabase() *CreateDatabaseOptionsBuilder {
	return &CreateDatabaseOptionsBuilder{}
}

// List implements Builder[CreateDatabaseOptions].
func (b *CreateDatabaseOptionsBuilder) List() []func(*CreateDatabaseOptions) {
	return b.Opts
}

// SetKeyspace sets the initial keyspace name for the database.
func (b *CreateDatabaseOptionsBuilder) SetKeyspace(v string) *CreateDatabaseOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateDatabaseOptions) {
		o.Keyspace = &v
	})
	return b
}

// SetBlocking controls whether to wait for the database to become ACTIVE.
// Defaults to true if not specified.
func (b *CreateDatabaseOptionsBuilder) SetBlocking(v bool) *CreateDatabaseOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateDatabaseOptions) {
		o.Blocking = &v
	})
	return b
}

// SetPollInterval sets how often to check the database status when blocking.
func (b *CreateDatabaseOptionsBuilder) SetPollInterval(v time.Duration) *CreateDatabaseOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateDatabaseOptions) {
		o.PollInterval = &v
	})
	return b
}

// DropDatabaseOptions represents options for the DropDatabase operation.
type DropDatabaseOptions struct {
	// Blocking controls whether to wait for the database to be fully terminated.
	// Defaults to true.
	Blocking *bool
	// PollInterval is how often to check the database status when blocking.
	// Defaults to DefaultDatabasePollInterval (10 seconds).
	PollInterval *time.Duration
}

// Validate implements the Validator interface for DropDatabaseOptions.
func (o DropDatabaseOptions) Validate() error {
	return nil
}

// List implements Builder[DropDatabaseOptions] allowing the raw struct to be
// passed directly to methods that accept ...Builder[DropDatabaseOptions].
func (o *DropDatabaseOptions) List() []func(*DropDatabaseOptions) {
	return []func(*DropDatabaseOptions){
		func(target *DropDatabaseOptions) {
			copyNonNilFields(o, target)
		},
	}
}

// DropDatabaseOptionsBuilder is a builder for DropDatabaseOptions.
type DropDatabaseOptionsBuilder struct {
	Opts []func(*DropDatabaseOptions)
}

// DropDatabase creates a new DropDatabaseOptionsBuilder.
func DropDatabase() *DropDatabaseOptionsBuilder {
	return &DropDatabaseOptionsBuilder{}
}

// List implements Builder[DropDatabaseOptions].
func (b *DropDatabaseOptionsBuilder) List() []func(*DropDatabaseOptions) {
	return b.Opts
}

// SetBlocking controls whether to wait for the database to be fully terminated.
// Defaults to true if not specified.
func (b *DropDatabaseOptionsBuilder) SetBlocking(v bool) *DropDatabaseOptionsBuilder {
	b.Opts = append(b.Opts, func(o *DropDatabaseOptions) {
		o.Blocking = &v
	})
	return b
}

// SetPollInterval sets how often to check the database status when blocking.
func (b *DropDatabaseOptionsBuilder) SetPollInterval(v time.Duration) *DropDatabaseOptionsBuilder {
	b.Opts = append(b.Opts, func(o *DropDatabaseOptions) {
		o.PollInterval = &v
	})
	return b
}

// CreateKeyspaceOptions represents options for the CreateKeyspace operation.
type CreateKeyspaceOptions struct {
	// Blocking controls whether to wait for the keyspace to become visible.
	// Defaults to true.
	Blocking *bool
	// PollInterval is how often to check whether the keyspace exists when blocking.
	// Defaults to DefaultKeyspacePollInterval (1 second).
	PollInterval *time.Duration
}

// Validate implements the Validator interface for CreateKeyspaceOptions.
func (o CreateKeyspaceOptions) Validate() error {
	return nil
}

// List implements Builder[CreateKeyspaceOptions] allowing the raw struct to be
// passed directly to methods that accept ...Builder[CreateKeyspaceOptions].
func (o *CreateKeyspaceOptions) List() []func(*CreateKeyspaceOptions) {
	return []func(*CreateKeyspaceOptions){
		func(target *CreateKeyspaceOptions) {
			copyNonNilFields(o, target)
		},
	}
}

// CreateKeyspaceOptionsBuilder is a builder for CreateKeyspaceOptions.
type CreateKeyspaceOptionsBuilder struct {
	Opts []func(*CreateKeyspaceOptions)
}

// CreateKeyspace creates a new CreateKeyspaceOptionsBuilder.
func CreateKeyspace() *CreateKeyspaceOptionsBuilder {
	return &CreateKeyspaceOptionsBuilder{}
}

// List implements Builder[CreateKeyspaceOptions].
func (b *CreateKeyspaceOptionsBuilder) List() []func(*CreateKeyspaceOptions) {
	return b.Opts
}

// SetBlocking controls whether to wait for the keyspace to become visible.
// Defaults to true if not specified.
func (b *CreateKeyspaceOptionsBuilder) SetBlocking(v bool) *CreateKeyspaceOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateKeyspaceOptions) {
		o.Blocking = &v
	})
	return b
}

// SetPollInterval sets how often to check whether the keyspace exists when blocking.
func (b *CreateKeyspaceOptionsBuilder) SetPollInterval(v time.Duration) *CreateKeyspaceOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateKeyspaceOptions) {
		o.PollInterval = &v
	})
	return b
}
