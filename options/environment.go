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

import "fmt"

// Environment represents the deployment environment for the database.
type Environment string

const (
	// EnvironmentProd is the Astra production environment.
	EnvironmentProd Environment = "prod"
	// EnvironmentDev is the Astra development environment.
	EnvironmentDev Environment = "dev"
	// EnvironmentTest is the Astra test environment.
	EnvironmentTest Environment = "test"
	// EnvironmentHCD is the Hyper-Converged Database environment.
	EnvironmentHCD Environment = "hcd"
	// EnvironmentDSE is the DataStax Enterprise environment.
	EnvironmentDSE Environment = "dse"
	// EnvironmentCassandra is the open-source Cassandra environment.
	EnvironmentCassandra Environment = "cassandra"
	// EnvironmentOther is any other non-Astra environment.
	EnvironmentOther Environment = "other"
)

// IsAstra returns true if this is an Astra environment (prod, dev, or test).
func (e Environment) IsAstra() bool {
	return e == EnvironmentProd || e == EnvironmentDev || e == EnvironmentTest
}

// DevOpsURL returns the Astra DevOps API base URL for this environment.
// Only meaningful for Astra environments.
func (e Environment) DevOpsURL() string {
	switch e {
	case EnvironmentDev:
		return "https://api.dev.cloud.datastax.com"
	case EnvironmentTest:
		return "https://api.test.cloud.datastax.com"
	default:
		return "https://api.astra.datastax.com"
	}
}

// AstraDBEndpoint returns the Data API endpoint for an Astra database.
// Only meaningful for Astra environments.
func (e Environment) AstraDBEndpoint(id, region string) string {
	switch e {
	case EnvironmentDev:
		return fmt.Sprintf("https://%s-%s.apps.dev.astra.datastax.com", id, region)
	case EnvironmentTest:
		return fmt.Sprintf("https://%s-%s.apps.test.astra.datastax.com", id, region)
	default:
		return fmt.Sprintf("https://%s-%s.apps.astra.datastax.com", id, region)
	}
}
