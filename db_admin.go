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
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/datastax/astra-db-go/options"
)

// DbAdmin provides admin operations for a specific Astra database.
// Obtain a DbAdmin from [Admin.CreateDatabase] or [Admin.DbAdmin].
//
// Example:
//
//	admin := client.Admin()
//	dbAdmin, err := admin.CreateDatabase(ctx, astradb.DatabaseInfo{
//	    Name:          "my-database",
//	    CloudProvider: "gcp",
//	    Region:        "us-east1",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	keyspaces, err := dbAdmin.ListKeyspaces(ctx)
type DbAdmin struct {
	id    string
	admin *Admin
}

// ID returns the database ID.
func (d *DbAdmin) ID() string {
	return d.id
}

// Info retrieves full database information from the DevOps API.
//
// Example:
//
//	info, err := dbAdmin.Info(ctx)
//	fmt.Println("Status:", info.Status)
func (d *DbAdmin) Info(ctx context.Context) (*Database, error) {
	return d.admin.GetDatabase(ctx, d.id)
}

// Drop terminates the database, permanently deleting all of its data.
//
// By default, this method blocks until the database is fully terminated.
// Use SetBlocking(false) to return immediately after the termination
// request is accepted.
//
// WARNING: This action cannot be undone.
//
// Example:
//
//	err := dbAdmin.Drop(ctx)
func (d *DbAdmin) Drop(ctx context.Context, opts ...options.Builder[options.DropDatabaseOptions]) error {
	return d.admin.DropDatabase(ctx, d.id, opts...)
}

// ListKeyspaces returns the keyspace names for this database, with the
// default keyspace first.
//
// Example:
//
//	keyspaces, err := dbAdmin.ListKeyspaces(ctx)
func (d *DbAdmin) ListKeyspaces(ctx context.Context) ([]string, error) {
	db, err := d.admin.GetDatabase(ctx, d.id)
	if err != nil {
		return nil, err
	}
	keyspaces := []string{db.Info.Keyspace}
	keyspaces = append(keyspaces, db.Info.AdditionalKeyspaces...)
	return keyspaces, nil
}

// CreateKeyspace creates a new keyspace in this database.
//
// By default, this method blocks until the keyspace is visible. Use
// SetBlocking(false) to return immediately after the request is accepted.
//
// Example:
//
//	err := dbAdmin.CreateKeyspace(ctx, "my_keyspace")
func (d *DbAdmin) CreateKeyspace(ctx context.Context, keyspace string, opts ...options.Builder[options.CreateKeyspaceOptions]) error {
	merged, err := options.MergeOptions(opts...)
	if err != nil {
		return err
	}

	cmd := d.admin.createCommand(http.MethodPost, "/databases/"+d.id+"/keyspaces/"+keyspace, nil)
	_, err = cmd.execute(ctx)
	if err != nil {
		return err
	}

	blocking := true
	if merged != nil && merged.Blocking != nil {
		blocking = *merged.Blocking
	}

	if !blocking {
		return nil
	}

	pollInterval := options.DefaultKeyspacePollInterval
	if merged != nil && merged.PollInterval != nil {
		pollInterval = *merged.PollInterval
	}

	slog.Debug("Waiting for keyspace to be created", "keyspace", keyspace, "pollInterval", pollInterval)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			keyspaces, err := d.ListKeyspaces(ctx)
			if err != nil {
				return fmt.Errorf("failed to list keyspaces while polling: %w", err)
			}
			for _, ks := range keyspaces {
				if ks == keyspace {
					return nil
				}
			}
			slog.Debug("Keyspace not yet visible", "keyspace", keyspace)
		}
	}
}

// DropKeyspace drops a keyspace from this database.
//
// Example:
//
//	err := dbAdmin.DropKeyspace(ctx, "my_keyspace")
func (d *DbAdmin) DropKeyspace(ctx context.Context, keyspace string) error {
	cmd := d.admin.createCommand(http.MethodDelete, "/databases/"+d.id+"/keyspaces/"+keyspace, nil)
	_, err := cmd.execute(ctx)
	return err
}
