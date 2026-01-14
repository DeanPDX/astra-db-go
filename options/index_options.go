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

// Metric constants for vector index similarity measurement
const (
	MetricCosine     = "cosine"
	MetricDotProduct = "dot_product"
	MetricEuclidean  = "euclidean"
)

// CreateIndexOptions represents options for creating a regular index.
type CreateIndexOptions struct {
	// IfNotExists if true, the command will silently succeed even if an index
	// with the given name already exists. This only checks index names, not definitions.
	IfNotExists bool

	// Ascii if true, converts non-ASCII characters to US-ASCII before indexing.
	// Only applicable to text columns.
	Ascii *bool

	// Normalize if true, applies Unicode character normalization before indexing.
	// Only applicable to text columns.
	Normalize *bool

	// CaseSensitive if true (default), enforces case-sensitive matching.
	// Only applicable to text columns.
	CaseSensitive *bool
}

// CreateIndex creates a new CreateIndexOptions builder.
func CreateIndex() *CreateIndexOptions {
	return &CreateIndexOptions{}
}

// SetIfNotExists sets the ifNotExists option for index creation.
// When true, the command will silently succeed even if an index with the given name already exists.
func (o *CreateIndexOptions) SetIfNotExists(v bool) *CreateIndexOptions {
	o.IfNotExists = v
	return o
}

// SetAscii sets the ascii option for text index creation.
// When true, converts non-ASCII characters to US-ASCII before indexing.
func (o *CreateIndexOptions) SetAscii(v bool) *CreateIndexOptions {
	o.Ascii = &v
	return o
}

// SetNormalize sets the normalize option for text index creation.
// When true, applies Unicode character normalization before indexing.
func (o *CreateIndexOptions) SetNormalize(v bool) *CreateIndexOptions {
	o.Normalize = &v
	return o
}

// SetCaseSensitive sets the caseSensitive option for text index creation.
// When true (default), enforces case-sensitive matching.
func (o *CreateIndexOptions) SetCaseSensitive(v bool) *CreateIndexOptions {
	o.CaseSensitive = &v
	return o
}

// CreateVectorIndexOptions represents options for creating a vector index.
type CreateVectorIndexOptions struct {
	// IfNotExists if true, the command will silently succeed even if an index
	// with the given name already exists. This only checks index names, not definitions.
	IfNotExists bool

	// Metric is the similarity measurement for vector search.
	// Valid values: "cosine" (default), "dot_product", "euclidean"
	Metric string

	// SourceModel is the embedding generation model, enabling optimizations.
	// Valid values: "ada002", "bert", "cohere-v3", "gecko", "nv-qa-4",
	// "openai-v3-large", "openai-v3-small", "other" (default)
	SourceModel string
}

// CreateVectorIndex creates a new CreateVectorIndexOptions builder.
func CreateVectorIndex() *CreateVectorIndexOptions {
	return &CreateVectorIndexOptions{}
}

// SetIfNotExists sets the ifNotExists option for vector index creation.
// When true, the command will silently succeed even if an index with the given name already exists.
func (o *CreateVectorIndexOptions) SetIfNotExists(v bool) *CreateVectorIndexOptions {
	o.IfNotExists = v
	return o
}

// SetMetric sets the similarity metric for vector search.
// Valid values: MetricCosine, MetricDotProduct, MetricEuclidean
func (o *CreateVectorIndexOptions) SetMetric(metric string) *CreateVectorIndexOptions {
	o.Metric = metric
	return o
}

// SetSourceModel sets the source model for vector index optimization.
// This enables provider-specific optimizations for the embedding model used.
func (o *CreateVectorIndexOptions) SetSourceModel(model string) *CreateVectorIndexOptions {
	o.SourceModel = model
	return o
}

// ListIndexesOptions represents options for listing indexes.
type ListIndexesOptions struct {
	// Explain if true, returns full index metadata including definitions.
	// If false (default), only returns index names.
	Explain bool
}

// ListIndexes creates a new ListIndexesOptions builder.
func ListIndexes() *ListIndexesOptions {
	return &ListIndexesOptions{}
}

// SetExplain sets the explain option for listing indexes.
// When true, returns full index metadata. When false, returns only index names.
func (o *ListIndexesOptions) SetExplain(v bool) *ListIndexesOptions {
	o.Explain = v
	return o
}
