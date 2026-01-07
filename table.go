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

	"github.com/datastax/astra-db-go/cursor"
	"github.com/datastax/astra-db-go/filter"
	"github.com/datastax/astra-db-go/options"
	"github.com/datastax/astra-db-go/results"
	"github.com/datastax/astra-db-go/table"
)

// Table represents a table in the Astra DB.
//
// Options set on the table are inherited by all commands
// executed on it, unless overridden at the command level.
type Table struct {
	db      *Db
	name    string
	options *options.APIOptions
}

// Name returns the table name.
func (t *Table) Name() string {
	return t.name
}

// Options returns the table's options (or empty options if nil).
func (t *Table) Options() *options.APIOptions {
	if t.options == nil {
		return &options.APIOptions{}
	}
	return t.options
}

// Database returns the parent database.
func (t *Table) Database() *Db {
	return t.db
}

// newCmd creates a command for this table
func (t *Table) newCmd(name string, payload any, opts ...options.APIOption) command {
	return newCmdWithOptions(t.db, t.name, name, payload, t.options, opts...)
}

// createTablePayload is the payload for the createTable command
type createTablePayload struct {
	Name       string           `json:"name"`
	Definition table.Definition `json:"definition"`
	Options    *createTableOpts `json:"options,omitempty"`
}

// createTableOpts represents the options sub-object in createTable payload
type createTableOpts struct {
	IfNotExists bool `json:"ifNotExists,omitempty"`
}

// createTableResponse represents the response from createTable
type createTableResponse struct {
	Status struct {
		OK int `json:"ok"`
	} `json:"status"`
}

// Table returns a Table object for the specified table name.
// This does not create the table or verify its existence.
//
// Options set here override those set on the database.
//
// Example:
//
//	tbl := db.Table("my_table",
//	    options.WithTimeout(60 * time.Second),
//	)
func (d *Db) Table(name string, opts ...options.APIOption) *Table {
	return &Table{
		db:      d,
		name:    name,
		options: options.NewAPIOptions(opts...),
	}
}

// CreateTable creates a new table in the database with the specified definition.
//
// The definition includes column names, data types, and the primary key configuration.
// After creating a table, you should index columns that you want to sort or filter
// to optimize queries.
//
// Example usage:
//
//	definition := table.Definition{
//		Columns: map[string]table.Column{
//			"title":           table.Text(),
//			"number_of_pages": table.Int(),
//			"rating":          table.Float(),
//			"is_checked_out":  table.Boolean(),
//		},
//		PrimaryKey: table.PrimaryKey{
//			PartitionBy: []string{"title"},
//		},
//	}
//	tbl, err := db.CreateTable(ctx, "my_table", definition)
func (d *Db) CreateTable(ctx context.Context, name string, definition table.Definition, opts ...options.TableOption) (*Table, error) {
	// Apply options
	tableOpts := options.NewCreateTableOptions(opts...)

	payload := createTablePayload{
		Name:       name,
		Definition: definition,
	}

	// Add options if ifNotExists is set
	if tableOpts.IfNotExists {
		payload.Options = &createTableOpts{
			IfNotExists: tableOpts.IfNotExists,
		}
	}

	cmd := d.newCmd("createTable", payload)

	// Override keyspace if specified in options
	if tableOpts.Keyspace != "" {
		cmd.keyspace = tableOpts.Keyspace
	}

	// Execute the command
	// Response is in format: {"status":{"ok":1}}
	_, err := cmd.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return &Table{
		db:   d,
		name: name,
	}, nil
}

// dropTablePayload is the payload for the dropTable command
type dropTablePayload struct {
	Name string `json:"name"`
}

// DropTable drops (deletes) a table from the database.
//
// Example usage:
//
//	err := db.DropTable(ctx, "my_table")
func (d *Db) DropTable(ctx context.Context, name string) error {
	cmd := d.newCmd("dropTable", dropTablePayload{Name: name})
	_, err := cmd.Execute(ctx)
	return err
}

// tableFindPayload is the payload for the find command on tables
type tableFindPayload struct {
	Filter     any             `json:"filter,omitempty"`
	Sort       map[string]any  `json:"sort,omitempty"`
	Projection map[string]bool `json:"projection,omitempty"`
	Options    *tableFindOpts  `json:"options,omitempty"`
}

// tableFindOpts represents the options sub-object in find payload
type tableFindOpts struct {
	Limit             *int    `json:"limit,omitempty"`
	Skip              *int    `json:"skip,omitempty"`
	IncludeSimilarity *bool   `json:"includeSimilarity,omitempty"`
	PageState         *string `json:"pageState,omitempty"`
}

// tableFindResponse is the response from the find command on tables
type tableFindResponse struct {
	Data struct {
		Documents     []json.RawMessage `json:"documents"`
		NextPageState *string           `json:"nextPageState"`
	} `json:"data"`
}

// Find returns a cursor for iterating over rows matching the filter criteria.
//
// The cursor automatically handles pagination, fetching new pages as needed.
//
// The filter parameter defines criteria for selecting rows. Pass an empty filter.F{}
// or nil to find all rows (not recommended for large tables).
//
// Use options to specify sorting, projection, limits, and other behaviors.
//
// Example using Next/Decode pattern:
//
//	cursor := tbl.Find(ctx, filter.Eq("is_checked_out", false))
//	defer cursor.Close(ctx)
//
//	for cursor.Next(ctx) {
//	    var row MyRow
//	    if err := cursor.Decode(&row); err != nil {
//	        return err
//	    }
//	    // Process row
//	}
//	if err := cursor.Err(); err != nil {
//	    return err
//	}
//
// Example getting all results at once:
//
//	cursor := tbl.Find(ctx, filter.F{})
//	var rows []MyRow
//	if err := cursor.All(ctx, &rows); err != nil {
//	    return err
//	}
//
// Example with vector search:
//
//	cursor := tbl.Find(ctx, filter.F{},
//	    options.WithSort(map[string]any{"vector_column": []float32{0.1, 0.2, 0.3}}),
//	    options.WithIncludeSimilarity(true),
//	)
func (t *Table) Find(ctx context.Context, f any, opts ...options.TableFindOption) *cursor.Cursor {
	// Validate filter type
	switch f.(type) {
	case filter.F, filter.Filter, map[string]any, nil:
		// Allowed filter types
	default:
		return cursor.NewWithError(fmt.Errorf("invalid filter type: %T", f))
	}

	// Build the find options once (they don't change between pages)
	findOpts := options.NewTableFindOptions(opts...)

	// Create a page fetcher that captures the table, filter, and options
	fetcher := func(fetchCtx context.Context, pageState *string) ([]json.RawMessage, *string, error) {
		payload := tableFindPayload{
			Filter:     f,
			Sort:       findOpts.Sort,
			Projection: findOpts.Projection,
		}

		// Build options - use provided pageState for pagination
		payloadOpts := &tableFindOpts{}
		hasOpts := false

		if findOpts.Limit != nil {
			payloadOpts.Limit = findOpts.Limit
			hasOpts = true
		}
		if findOpts.Skip != nil {
			payloadOpts.Skip = findOpts.Skip
			hasOpts = true
		}
		if findOpts.IncludeSimilarity != nil {
			payloadOpts.IncludeSimilarity = findOpts.IncludeSimilarity
			hasOpts = true
		}
		if pageState != nil {
			payloadOpts.PageState = pageState
			hasOpts = true
		} else if findOpts.InitialPageState != nil {
			// Only use InitialPageState for the first request
			payloadOpts.PageState = findOpts.InitialPageState
			hasOpts = true
		}

		if hasOpts {
			payload.Options = payloadOpts
		}

		cmd := t.newCmd("find", payload)
		b, err := cmd.Execute(fetchCtx)
		if err != nil {
			return nil, nil, err
		}

		var resp tableFindResponse
		if err := json.Unmarshal(b, &resp); err != nil {
			return nil, nil, err
		}

		return resp.Data.Documents, resp.Data.NextPageState, nil
	}

	return cursor.New(fetcher)
}

// FindOne finds a single row in a table matching the filter criteria.
//
// Example usage:
//
//	result := table.FindOne(ctx, filter.Eq("id", "some-uuid"))
//	var row MyRow
//	err := result.Decode(&row)
func (t *Table) FindOne(ctx context.Context, f any, opts ...options.TableFindOption) *results.SingleResult {
	// Validate filter type
	switch f.(type) {
	case filter.F, filter.Filter, map[string]any, nil:
		// Allowed filter types
	default:
		return results.NewSingleResult(nil, fmt.Errorf("invalid filter type: %T", f))
	}

	// Build the find options
	findOpts := options.NewTableFindOptions(opts...)

	// Build the payload
	payload := tableFindPayload{
		Filter:     f,
		Sort:       findOpts.Sort,
		Projection: findOpts.Projection,
	}

	// Add options if any are set (limit is not applicable for findOne)
	if findOpts.IncludeSimilarity != nil {
		payload.Options = &tableFindOpts{
			IncludeSimilarity: findOpts.IncludeSimilarity,
		}
	}

	cmd := t.newCmd("findOne", payload)
	b, err := cmd.Execute(ctx)
	return results.NewSingleResult(b, err)
}

// tableInsertOnePayload is the payload for insertOne on tables
type tableInsertOnePayload struct {
	Document any `json:"document"`
}

// tableInsertManyPayload is the payload for insertMany on tables
type tableInsertManyPayload struct {
	Documents any `json:"documents"`
}

// TableInsertResponse represents the response from insert operations on tables.
// The InsertedIds contains the primary key values of inserted rows.
type TableInsertResponse struct {
	Status struct {
		// InsertedIds contains the primary key values of inserted rows.
		// For single-column primary keys, this will be an array of scalar values.
		// For composite/compound primary keys, this will be an array of objects
		// with the primary key column names as keys.
		InsertedIds []any `json:"insertedIds"`
		// PrimaryKeySchema describes the structure of the primary key.
		// Contains information about partition keys and clustering keys.
		PrimaryKeySchema *PrimaryKeySchema `json:"primaryKeySchema,omitempty"`
	} `json:"status"`
}

// PrimaryKeySchema describes the primary key structure returned in insert responses.
// It maps column names to their type information.
type PrimaryKeySchema map[string]ColumnTypeInfo

// ColumnTypeInfo describes the type of a column in the primary key schema
type ColumnTypeInfo struct {
	Type string `json:"type"`
}

// InsertOne inserts a single row into the table.
//
// The row parameter should be a struct or map representing the row data.
// The primary key columns must be included in the row data.
//
// Returns the inserted primary key value(s) in the response.
//
// Example usage:
//
//	type Book struct {
//		Title         string  `json:"title"`
//		Author        string  `json:"author"`
//		NumberOfPages int     `json:"number_of_pages"`
//		Rating        float32 `json:"rating"`
//	}
//
//	book := Book{
//		Title:         "The Great Gatsby",
//		Author:        "F. Scott Fitzgerald",
//		NumberOfPages: 180,
//		Rating:        4.5,
//	}
//	resp, err := table.InsertOne(ctx, book)
func (t *Table) InsertOne(ctx context.Context, row any, opts ...options.APIOption) (TableInsertResponse, error) {
	var resp TableInsertResponse
	cmd := t.newCmd("insertOne", tableInsertOnePayload{
		Document: row,
	}, opts...)
	b, err := cmd.Execute(ctx)
	if err != nil {
		return resp, err
	}
	err = json.Unmarshal(b, &resp)
	return resp, err
}

// InsertMany inserts multiple rows into the table.
//
// The rows parameter must be a non-empty slice of structs or maps representing the row data.
// The primary key columns must be included in each row.
//
// Returns the inserted primary key values in the response.
//
// Example usage:
//
//	books := []Book{
//		{Title: "Book 1", Author: "Author 1", NumberOfPages: 100, Rating: 4.0},
//		{Title: "Book 2", Author: "Author 2", NumberOfPages: 200, Rating: 4.5},
//	}
//	resp, err := table.InsertMany(ctx, books)
func (t *Table) InsertMany(ctx context.Context, rows any, opts ...options.APIOption) (TableInsertResponse, error) {
	var resp TableInsertResponse

	// Ensure we have a slice with rows
	err := ensureNonEmptySlice(rows)
	if err != nil {
		return resp, fmt.Errorf("rows: %w", err)
	}

	cmd := t.newCmd("insertMany", tableInsertManyPayload{
		Documents: rows,
	}, opts...)
	b, err := cmd.Execute(ctx)
	if err != nil {
		return resp, err
	}
	err = json.Unmarshal(b, &resp)
	return resp, err
}
