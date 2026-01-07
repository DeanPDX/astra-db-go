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

package options

// DBEnvironment represents the Astra DB environment.
// This is used for admin operations (creating/deleting databases).
type DBEnvironment int

// URL returns the API URL for the environment.
func (e DBEnvironment) URL() string {
	switch e {
	case EnvironmentProduction:
		return "https://api.astra.datastax.com/v2"
	case EnvironmentDev:
		return "https://api.dev.cloud.datastax.com/v2"
	case EnvironmentTest:
		return "https://api.test.cloud.datastax.com/v2"
	default:
		return ""
	}
}

const (
	// EnvironmentProduction is the production Astra DB environment.
	EnvironmentProduction DBEnvironment = iota
	// EnvironmentDev is the development Astra DB environment.
	EnvironmentDev
	// EnvironmentTest is the test Astra DB environment.
	EnvironmentTest
)

// AdminTimeouts contains timeout settings for admin operations.
type AdminTimeouts struct {
	Collection *int64
	Table      *int64
	Database   *int64
	Keyspace   *int64
}
