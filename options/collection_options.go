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

// CollectionOptions represents options for a collection's behavior.
type CollectionOptions struct {
	// Settings for generating ids
	DefaultId *DefaultIdOptions `json:"defaultId,omitempty"`

	// Vector specifications for the collection
	Vector *VectorOptions `json:"vector,omitempty"`

	// Overrides for document indexing
	Indexing *IndexingOptions `json:"indexing,omitempty"`

	// Lexical analysis options for the collection
	Lexical *LexicalOptions `json:"lexical,omitempty"`

	// Reranking options for the collection
	Rerank *RerankOptions `json:"rerank,omitempty"`
}

// -- Placeholder structs for the types referenced above --

type DefaultIdOptions struct {
}

// VectorOptions configures vector search for a collection.
type VectorOptions struct {
	// Dimension specifies the dimension of vectors stored in this collection.
	// Required for vector-enabled collections.
	Dimension int `json:"dimension,omitempty"`

	// Metric specifies the similarity metric used for vector search.
	// Valid values are "cosine", "euclidean", or "dot_product".
	// Default is "cosine".
	Metric string `json:"metric,omitempty"`

	// Service configures automatic vector embedding generation (vectorize).
	Service *VectorServiceOptions `json:"service,omitempty"`
}

// VectorServiceOptions configures the embedding service for vectorize.
type VectorServiceOptions struct {
	// Provider is the embedding provider name (e.g., "openai", "huggingface").
	Provider string `json:"provider,omitempty"`

	// ModelName is the name of the embedding model to use.
	ModelName string `json:"modelName,omitempty"`
}

type IndexingOptions struct {
}

type LexicalOptions struct {
}

type RerankOptions struct {
}
