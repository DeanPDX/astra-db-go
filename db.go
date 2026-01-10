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

// Package astradb implements the astra database client.
package astradb

import (
	"context"

	"github.com/datastax/astra-db-go/options"
)

// Db represents a connection to a specific Astra DB database.
//
// Options set on the database are inherited by all collections, tables,
// and commands created from it, unless overridden at a lower level.
type Db struct {
	endpoint string
	client   *DataAPIClient
	options  *options.APIOptions
}

func (d *Db) newCmd(name string, payload any) command {
	return newCmd(d, name, payload)
}

// Endpoint returns the database API endpoint.
func (d *Db) Endpoint() string {
	return d.endpoint
}

// Options returns the database's options (or empty options if nil).
func (d *Db) Options() *options.APIOptions {
	if d.options == nil {
		return &options.APIOptions{}
	}
	return d.options
}

// Client returns the parent DataAPIClient.
func (d *Db) Client() *DataAPIClient {
	return d.client
}

// Collection returns a handle for the named collection.
//
// Options set here override those set on the database.
//
// Example:
//
//	coll := db.Collection("my_collection",
//	    options.WithTimeout(60 * time.Second),
//	)
func (d *Db) Collection(name string, opts ...options.APIOption) *Collection {
	return &Collection{
		db:      d,
		name:    name,
		options: options.NewAPIOptions(opts...),
	}
}

// CreateCollection creates a collection in the database.
// Note: Warnings are accessible via the WarningHandler option callback only.
func (d *Db) CreateCollection(ctx context.Context, name string, collOpts *options.CollectionOptions) (*Collection, error) {
	payload := struct {
		Name    string                     `json:"name"`
		Options *options.CollectionOptions `json:"options,omitempty"`
	}{
		Name:    name,
		Options: collOpts,
	}
	cmd := d.newCmd("createCollection", payload)
	_, _, err := cmd.Execute(ctx)
	if err != nil {
		return nil, err
	}
	return &Collection{
		db:   d,
		name: name,
	}, nil
}

// DropCollection drops a collection from the database.
// Note: Warnings are accessible via the WarningHandler option callback only.
func (d *Db) DropCollection(ctx context.Context, name string) error {
	payload := struct {
		Name string `json:"name"`
	}{
		Name: name,
	}
	cmd := newCmd(d, "deleteCollection", payload)
	_, _, err := cmd.Execute(ctx)
	return err
}
