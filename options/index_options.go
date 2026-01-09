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

// CreateIndexOptions represents options for creating a regular index
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

// IndexOption is a functional option for configuring CreateIndexOptions
type IndexOption func(*CreateIndexOptions)

// WithIndexIfNotExists sets the ifNotExists option for index creation
func WithIndexIfNotExists(v bool) IndexOption {
	return func(opts *CreateIndexOptions) {
		opts.IfNotExists = v
	}
}

// WithAscii sets the ascii option for text index creation.
// When true, converts non-ASCII characters to US-ASCII before indexing.
func WithAscii(v bool) IndexOption {
	return func(opts *CreateIndexOptions) {
		opts.Ascii = &v
	}
}

// WithNormalize sets the normalize option for text index creation.
// When true, applies Unicode character normalization before indexing.
func WithNormalize(v bool) IndexOption {
	return func(opts *CreateIndexOptions) {
		opts.Normalize = &v
	}
}

// WithCaseSensitive sets the caseSensitive option for text index creation.
// When true (default), enforces case-sensitive matching.
func WithCaseSensitive(v bool) IndexOption {
	return func(opts *CreateIndexOptions) {
		opts.CaseSensitive = &v
	}
}

// NewCreateIndexOptions creates a CreateIndexOptions with the provided options applied
func NewCreateIndexOptions(opts ...IndexOption) *CreateIndexOptions {
	options := &CreateIndexOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

// CreateVectorIndexOptions represents options for creating a vector index
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

// VectorIndexOption is a functional option for configuring CreateVectorIndexOptions
type VectorIndexOption func(*CreateVectorIndexOptions)

// WithVectorIndexIfNotExists sets the ifNotExists option for vector index creation
func WithVectorIndexIfNotExists(v bool) VectorIndexOption {
	return func(opts *CreateVectorIndexOptions) {
		opts.IfNotExists = v
	}
}

// WithMetric sets the similarity metric for vector search.
// Valid values: MetricCosine, MetricDotProduct, MetricEuclidean
func WithMetric(metric string) VectorIndexOption {
	return func(opts *CreateVectorIndexOptions) {
		opts.Metric = metric
	}
}

// WithSourceModel sets the source model for vector index optimization.
// This enables provider-specific optimizations for the embedding model used.
func WithSourceModel(model string) VectorIndexOption {
	return func(opts *CreateVectorIndexOptions) {
		opts.SourceModel = model
	}
}

// NewCreateVectorIndexOptions creates a CreateVectorIndexOptions with the provided options applied
func NewCreateVectorIndexOptions(opts ...VectorIndexOption) *CreateVectorIndexOptions {
	options := &CreateVectorIndexOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return options
}
