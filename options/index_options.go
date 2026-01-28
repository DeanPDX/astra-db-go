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
func (o *CreateIndexOptions) List() []func(*CreateIndexOptions) error {
	return []func(*CreateIndexOptions) error{
		func(target *CreateIndexOptions) error {
			copyNonNilFields(o, target)
			return nil
		},
	}
}

// CreateIndexOptionsBuilder is a builder for CreateIndexOptions that implements
// Lister[CreateIndexOptions] following the MongoDB Go driver pattern.
type CreateIndexOptionsBuilder struct {
	Opts []func(*CreateIndexOptions) error
}

// CreateIndex creates a new CreateIndexOptionsBuilder.
func CreateIndex() *CreateIndexOptionsBuilder {
	return &CreateIndexOptionsBuilder{}
}

// List implements Lister[CreateIndexOptions].
func (b *CreateIndexOptionsBuilder) List() []func(*CreateIndexOptions) error {
	return b.Opts
}

// SetIfNotExists sets the ifNotExists option for index creation.
// When true, the command will silently succeed even if an index with the given name already exists.
func (b *CreateIndexOptionsBuilder) SetIfNotExists(v bool) *CreateIndexOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateIndexOptions) error {
		o.IfNotExists = &v
		return nil
	})
	return b
}

// SetAscii sets the ascii option for text index creation.
// When true, converts non-ASCII characters to US-ASCII before indexing.
func (b *CreateIndexOptionsBuilder) SetAscii(v bool) *CreateIndexOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateIndexOptions) error {
		o.Ascii = &v
		return nil
	})
	return b
}

// SetNormalize sets the normalize option for text index creation.
// When true, applies Unicode character normalization before indexing.
func (b *CreateIndexOptionsBuilder) SetNormalize(v bool) *CreateIndexOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateIndexOptions) error {
		o.Normalize = &v
		return nil
	})
	return b
}

// SetCaseSensitive sets the caseSensitive option for text index creation.
// When true (default), enforces case-sensitive matching.
func (b *CreateIndexOptionsBuilder) SetCaseSensitive(v bool) *CreateIndexOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateIndexOptions) error {
		o.CaseSensitive = &v
		return nil
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
	Metric *string

	// SourceModel is the embedding generation model, enabling optimizations.
	// Valid values: "ada002", "bert", "cohere-v3", "gecko", "nv-qa-4",
	// "openai-v3-large", "openai-v3-small", "other" (default)
	SourceModel *string
}

// List implements Lister[CreateVectorIndexOptions] allowing the raw struct to be
// passed directly to methods that accept ...Lister[CreateVectorIndexOptions].
func (o *CreateVectorIndexOptions) List() []func(*CreateVectorIndexOptions) error {
	return []func(*CreateVectorIndexOptions) error{
		func(target *CreateVectorIndexOptions) error {
			copyNonNilFields(o, target)
			return nil
		},
	}
}

// CreateVectorIndexOptionsBuilder is a builder for CreateVectorIndexOptions that implements
// Lister[CreateVectorIndexOptions] following the MongoDB Go driver pattern.
type CreateVectorIndexOptionsBuilder struct {
	Opts []func(*CreateVectorIndexOptions) error
}

// CreateVectorIndex creates a new CreateVectorIndexOptionsBuilder.
func CreateVectorIndex() *CreateVectorIndexOptionsBuilder {
	return &CreateVectorIndexOptionsBuilder{}
}

// List implements Lister[CreateVectorIndexOptions].
func (b *CreateVectorIndexOptionsBuilder) List() []func(*CreateVectorIndexOptions) error {
	return b.Opts
}

// SetIfNotExists sets the ifNotExists option for vector index creation.
// When true, the command will silently succeed even if an index with the given name already exists.
func (b *CreateVectorIndexOptionsBuilder) SetIfNotExists(v bool) *CreateVectorIndexOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateVectorIndexOptions) error {
		o.IfNotExists = &v
		return nil
	})
	return b
}

// SetMetric sets the similarity metric for vector search.
// Valid values: MetricCosine, MetricDotProduct, MetricEuclidean
func (b *CreateVectorIndexOptionsBuilder) SetMetric(metric string) *CreateVectorIndexOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateVectorIndexOptions) error {
		o.Metric = &metric
		return nil
	})
	return b
}

// SetSourceModel sets the source model for vector index optimization.
// This enables provider-specific optimizations for the embedding model used.
func (b *CreateVectorIndexOptionsBuilder) SetSourceModel(model string) *CreateVectorIndexOptionsBuilder {
	b.Opts = append(b.Opts, func(o *CreateVectorIndexOptions) error {
		o.SourceModel = &model
		return nil
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
func (o *ListIndexesOptions) List() []func(*ListIndexesOptions) error {
	return []func(*ListIndexesOptions) error{
		func(target *ListIndexesOptions) error {
			copyNonNilFields(o, target)
			return nil
		},
	}
}

// ListIndexesOptionsBuilder is a builder for ListIndexesOptions that implements
// Lister[ListIndexesOptions] following the MongoDB Go driver pattern.
type ListIndexesOptionsBuilder struct {
	Opts []func(*ListIndexesOptions) error
}

// ListIndexes creates a new ListIndexesOptionsBuilder.
func ListIndexes() *ListIndexesOptionsBuilder {
	return &ListIndexesOptionsBuilder{}
}

// List implements Lister[ListIndexesOptions].
func (b *ListIndexesOptionsBuilder) List() []func(*ListIndexesOptions) error {
	return b.Opts
}

// SetExplain sets the explain option for listing indexes.
// When true, returns full index metadata. When false, returns only index names.
func (b *ListIndexesOptionsBuilder) SetExplain(v bool) *ListIndexesOptionsBuilder {
	b.Opts = append(b.Opts, func(o *ListIndexesOptions) error {
		o.Explain = &v
		return nil
	})
	return b
}
