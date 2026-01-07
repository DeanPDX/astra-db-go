package definitions

// CollectionDefinitions represents options for a collection's behavior.
type CollectionDefinitions struct {
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
// You will need to populate these based on their specific C# definitions.

type DefaultIdOptions struct {
	// Add fields here, e.g.:
	// Type string `json:"type"`
}

type VectorOptions struct {
	// Add fields here
}

type IndexingOptions struct {
	// Add fields here
}

type LexicalOptions struct {
	// Add fields here
}

type RerankOptions struct {
	// Add fields here
}
