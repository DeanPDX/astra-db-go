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

import "github.com/datastax/astra-db-go/options"

// DataAPIClient is a client for interacting with an Astra DB database.
// Construct a new client using [NewClient].
//
// Options set on the client are inherited by all databases, collections,
// tables, and commands created from it, unless overridden at a lower level.
type DataAPIClient struct {
	options *options.APIOptions
}

// NewClient returns a new DataAPIClient with the given options.
//
// Example:
//
//	client := astradb.NewClient(
//	    options.WithToken("AstraCS:..."),
//	)
func NewClient(opts ...options.APIOption) *DataAPIClient {
	return &DataAPIClient{
		options: options.NewAPIOptions(opts...),
	}
}

// Options returns the client's options (or an empty options if nil).
func (c *DataAPIClient) Options() *options.APIOptions {
	if c.options == nil {
		return &options.APIOptions{}
	}
	return c.options
}

// Database returns a handle for the given database endpoint.
//
// Options set here override those set on the client.
//
// Example:
//
//	db := client.Database("https://...",
//	    options.WithKeyspace("my_keyspace"),
//	)
func (c *DataAPIClient) Database(endpoint string, opts ...options.APIOption) *Db {
	return &Db{
		endpoint: endpoint,
		client:   c,
		options:  options.NewAPIOptions(opts...),
	}
}
