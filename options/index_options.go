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

// VectorMetric represents the similarity measurement for vector search.
type VectorMetric string

// Metric constants for vector index similarity measurement
const (
	MetricCosine     VectorMetric = "cosine"
	MetricDotProduct VectorMetric = "dot_product"
	MetricEuclidean  VectorMetric = "euclidean"
)

// CreateIndexOptions represents options for creating a regular index.
type CreateIndexOptions struct {
	// IfNotExists if true, the command will silently succeed even if an index
	// with the given name already exists. This only checks index names, not definitions.
	IfNotExists *bool

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

// List implements Lister[CreateIndexOptions] allowing the raw struct to be
// passed directly to methods that accept ...Lister[CreateIndexOptions].
func (o *CreateIndexOptions) List() []func(*CreateIndexOptions) {
	return []func(*CreateIndexOptions){
		func(target *CreateIndexOptions) {
			copyNonNilFields(o, target)
		},
	}
}

// Validate implements Validator for CreateIndexOptions.
func (o CreateIndexOptions) Validate() error {
	return nil
}

// CreateIndexOptionsBuilder is a builder for CreateIndexOptions that implements
// Lister[CreateIndexOptions] following the MongoDB Go driver pattern.
type CreateIndexOptionsBuilder struct {
	Opts []func(*CreateIndexOptions)
}

// CreateIndex creates a new CreateIndexOptionsBuilder.
func CreateIndex() *CreateIndexOptionsBuilder {
	return &CreateIndexOptionsBuilder{}
}

// List implements Lister[CreateIndexOptions].
func (b *CreateIndexOptionsBuilder) List() []func(*CreateIndexOptions) {
	return b.Opts
}

// SetIfNotExists sets the ifNotExists option for index creation.
// When true, the command will silently succeed even if an index with the given name already exists.
func (b *CreateIndexOptionsBuilder) SetIfNotExists(v bool) *CreateIndexOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateIndexOptions) {
		o.IfNotExists = &v
	})
	return b
}

// SetAscii sets the ascii option for text index creation.
// When true, converts non-ASCII characters to US-ASCII before indexing.
func (b *CreateIndexOptionsBuilder) SetAscii(v bool) *CreateIndexOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateIndexOptions) {
		o.Ascii = &v
	})
	return b
}

// SetNormalize sets the normalize option for text index creation.
// When true, applies Unicode character normalization before indexing.
func (b *CreateIndexOptionsBuilder) SetNormalize(v bool) *CreateIndexOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateIndexOptions) {
		o.Normalize = &v
	})
	return b
}

// SetCaseSensitive sets the caseSensitive option for text index creation.
// When true (default), enforces case-sensitive matching.
func (b *CreateIndexOptionsBuilder) SetCaseSensitive(v bool) *CreateIndexOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateIndexOptions) {
		o.CaseSensitive = &v
	})
	return b
}

// CreateVectorIndexOptions represents options for creating a vector index.
type CreateVectorIndexOptions struct {
	// IfNotExists if true, the command will silently succeed even if an index
	// with the given name already exists. This only checks index names, not definitions.
	IfNotExists *bool

	// Metric is the similarity measurement for vector search.
	// Valid values: "cosine" (default), "dot_product", "euclidean"
	Metric *VectorMetric

	// SourceModel is the embedding generation model, enabling optimizations.
	// Valid values: "ada002", "bert", "cohere-v3", "gecko", "nv-qa-4",
	// "openai-v3-large", "openai-v3-small", "other" (default)
	SourceModel *string
}

// List implements Lister[CreateVectorIndexOptions] allowing the raw struct to be
// passed directly to methods that accept ...Lister[CreateVectorIndexOptions].
func (o *CreateVectorIndexOptions) List() []func(*CreateVectorIndexOptions) {
	return []func(*CreateVectorIndexOptions){
		func(target *CreateVectorIndexOptions) {
			copyNonNilFields(o, target)
		},
	}
}

// Validate implements Validator for CreateVectorIndexOptions.
func (o CreateVectorIndexOptions) Validate() error {
	return nil
}

// CreateVectorIndexOptionsBuilder is a builder for CreateVectorIndexOptions that implements
// Lister[CreateVectorIndexOptions] following the MongoDB Go driver pattern.
type CreateVectorIndexOptionsBuilder struct {
	Opts []func(*CreateVectorIndexOptions)
}

// CreateVectorIndex creates a new CreateVectorIndexOptionsBuilder.
func CreateVectorIndex() *CreateVectorIndexOptionsBuilder {
	return &CreateVectorIndexOptionsBuilder{}
}

// List implements Lister[CreateVectorIndexOptions].
func (b *CreateVectorIndexOptionsBuilder) List() []func(*CreateVectorIndexOptions) {
	return b.Opts
}

// SetIfNotExists sets the ifNotExists option for vector index creation.
// When true, the command will silently succeed even if an index with the given name already exists.
func (b *CreateVectorIndexOptionsBuilder) SetIfNotExists(v bool) *CreateVectorIndexOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateVectorIndexOptions) {
		o.IfNotExists = &v
	})
	return b
}

// SetMetric sets the similarity metric for vector search.
//
// Can be one of: [MetricCosine] (default), [MetricDotProduct], [MetricEuclidean].
func (b *CreateVectorIndexOptionsBuilder) SetMetric(v VectorMetric) *CreateVectorIndexOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateVectorIndexOptions) {
		o.Metric = &v
	})
	return b
}

// SetSourceModel sets the source model for vector index optimization.
// This enables provider-specific optimizations for the embedding model used.
//
// Can be one of:
// ada002, bert, cohere-v3, gecko, nv-qa-4, openai-v3-large, openai-v3-small, other (default)
func (b *CreateVectorIndexOptionsBuilder) SetSourceModel(model string) *CreateVectorIndexOptionsBuilder {
	// NOTE: following the other libraries' patterns, we are using a enum-like option for Metric, but
	// this is a string. For reference:
	// https://docs.datastax.com/en/astra-db-serverless/api-reference/table-index-methods/create-vector-index.html#parameters
	b.Opts = append(b.Opts, func(o *CreateVectorIndexOptions) {
		o.SourceModel = &model
	})
	return b
}

// ListIndexesOptions represents options for listing indexes.
type ListIndexesOptions struct {
	// Explain if true, returns full index metadata including definitions.
	// If false (default), only returns index names.
	Explain *bool
}

// List implements Lister[ListIndexesOptions] allowing the raw struct to be
// passed directly to methods that accept ...Lister[ListIndexesOptions].
func (o *ListIndexesOptions) List() []func(*ListIndexesOptions) {
	return []func(*ListIndexesOptions){
		func(target *ListIndexesOptions) {
			copyNonNilFields(o, target)
		},
	}
}

// Validate implements Validator for ListIndexesOptions.
func (o ListIndexesOptions) Validate() error {
	return nil
}

// ListIndexesOptionsBuilder is a builder for ListIndexesOptions that implements
// Lister[ListIndexesOptions] following the MongoDB Go driver pattern.
type ListIndexesOptionsBuilder struct {
	Opts []func(*ListIndexesOptions)
}

// ListIndexes creates a new ListIndexesOptionsBuilder.
func ListIndexes() *ListIndexesOptionsBuilder {
	return &ListIndexesOptionsBuilder{}
}

// List implements Lister[ListIndexesOptions].
func (b *ListIndexesOptionsBuilder) List() []func(*ListIndexesOptions) {
	return b.Opts
}

// SetExplain sets the explain option for listing indexes.
// When true, returns full index metadata. When false, returns only index names.
func (b *ListIndexesOptionsBuilder) SetExplain(v bool) *ListIndexesOptionsBuilder {
	b.Opts = append(b.Opts, func(o *ListIndexesOptions) {
		o.Explain = &v
	})
	return b
}
