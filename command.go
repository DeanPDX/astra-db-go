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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/datastax/astra-db-go/options"
)

// command represents a command to be executed against the astra DB.
type command struct {
	db              *Db
	name            string
	payload         any
	keyspace        string
	apiVersion      string
	resourceName    string
	resourceOptions *options.APIOptions // Options from the collection/table level
	commandOptions  *options.APIOptions // Options for this specific command
}

// newCmd creates a new command from the given DB
func newCmd(d *Db, name string, payload any) command {
	return command{
		db:      d,
		name:    name,
		payload: payload,
	}
}

// newCmdResource creates a new command from the given DB with a resource
func newCmdResource(d *Db, resource, name string, payload any) command {
	return command{
		db:           d,
		name:         name,
		resourceName: resource,
		payload:      payload,
	}
}

// newCmdWithOptions creates a new command with resource and command-level options
func newCmdWithOptions(d *Db, resource, name string, payload any, resourceOpts *options.APIOptions, cmdOpts ...options.APIOption) command {
	var cmdOptions *options.APIOptions
	if len(cmdOpts) > 0 {
		cmdOptions = options.NewAPIOptions(cmdOpts...)
	}

	return command{
		db:              d,
		name:            name,
		resourceName:    resource,
		payload:         payload,
		resourceOptions: resourceOpts,
		commandOptions:  cmdOptions,
	}
}

// resolveOptions merges all option layers and returns the final resolved options.
// Merge order: Defaults -> Client -> Database -> Resource (Collection/Table) -> Command
func (c *command) resolveOptions() *options.APIOptions {
	var clientOpts, dbOpts *options.APIOptions

	if c.db != nil {
		dbOpts = c.db.options
		if c.db.client != nil {
			clientOpts = c.db.client.options
		}
	}

	return options.Merge(
		clientOpts,        // Client level
		dbOpts,            // Database level
		c.resourceOptions, // Collection/Table level
		c.commandOptions,  // Command level
	)
}

// Keyspace returns the keyspace to use for this command.
// If explicitly set on the command, that value is used.
// Otherwise, it falls back to the resolved options.
func (c *command) Keyspace() string {
	if len(c.keyspace) > 0 {
		return c.keyspace
	}
	return c.resolveOptions().GetKeyspace()
}

// ApiVersion returns the API version to use for this command.
// If explicitly set on the command, that value is used.
// Otherwise, it falls back to the resolved options.
func (c *command) ApiVersion() string {
	if len(c.apiVersion) > 0 {
		return c.apiVersion
	}
	return c.resolveOptions().GetAPIVersion()
}

func (c *command) url() (string, error) {
	if c.db == nil {
		return "", errors.New("nil Db")
	}
	if len(c.db.Endpoint()) == 0 {
		return "", errors.New("empty API endpoint")
	}
	return url.JoinPath(c.db.Endpoint(), "/api/json", c.ApiVersion(), c.Keyspace(), c.resourceName)
}

// This is similar to the [.NET client]. If we have a command name we want to
// marshal into json such as:
//
//	{"createCollection":{"name":"COLLECTION_NAME","options":{}}}
//
// But if we don't have a command name, we just marshal the payload directly.
//
// [.NET client]: https://github.com/datastax/astra-db-csharp/blob/699ac093494b1a5adbb65c65be57af5b48eb8cc2/src/DataStax.AstraDB.DataApi/Core/Commands/Command.cs#L92
func (c command) MarshalJSON() ([]byte, error) {
	if len(c.name) > 0 {
		data := make(map[string]any)
		data[c.name] = c.payload
		return json.Marshal(data)
	}
	return json.Marshal(c.payload)
}

// Execute a command against the astra DB web API.
func (c *command) Execute(ctx context.Context) ([]byte, error) {
	var body []byte
	if c.db == nil {
		return body, ErrCmdNilDb
	}

	// Resolve all options for this command
	opts := c.resolveOptions()

	b, err := json.Marshal(c)
	if err != nil {
		return body, err
	}
	cmdURL, err := c.url()
	if err != nil {
		return body, err
	}
	slog.Debug("Running cmd.Execute", "req.url", cmdURL, "req.body", string(b))

	req, err := http.NewRequestWithContext(ctx, "POST", cmdURL, bytes.NewReader(b))
	if err != nil {
		return body, err
	}

	// Set authentication token from resolved options
	token := opts.GetToken()
	if token != "" {
		req.Header.Set("Token", token)
	}
	req.Header.Set("Content-Type", "application/json")

	// Add any custom headers from resolved options
	for key, value := range opts.Headers {
		req.Header.Set(key, value)
	}

	// Use HTTP client from resolved options
	httpClient := opts.GetHTTPClient()
	resp, err := httpClient.Do(req)
	if err != nil {
		return body, err
	}
	defer resp.Body.Close()

	body, err = io.ReadAll(resp.Body)
	slog.Debug("cmd.Execute response", "resp.StatusCode", resp.StatusCode, "resp.Status", resp.Status, "resp.body", string(body))
	if err != nil {
		return body, err
	}
	return c.ExtractErrors(resp.StatusCode, body)
}

// apiErrs is just to capture errors
type apiErrs struct {
	Errors DataAPIErrors `json:"errors"`
}

// ExtractErrors will extract known errors from body. For example, it will
// turn this response into an error:
//
//	{"message":"Your database is resuming from hibernation and will be available in the next few minutes."}
func (c *command) ExtractErrors(statusCode int, body []byte) ([]byte, error) {
	if statusCode >= 400 {
		// We have a transport/server-level error so let's try to extract the message.
		var transportErr DataAPIError
		json.Unmarshal(body, &transportErr)
		if len(transportErr.Message) > 0 {
			return body, errors.New(transportErr.Message)
		}
		// We can't find a message; just return the body
		return body, errors.New(string(body))
	}
	var errs apiErrs
	// Ignoring errors here because we don't want to surface them to the client.
	// We will catch any errors here with unit tests that expect errors and don't get them.
	json.Unmarshal(body, &errs)
	if len(errs.Errors) > 0 {
		return body, &errs.Errors
	}
	// No errors.
	return body, nil
}
