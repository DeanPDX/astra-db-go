package options

import "time"

// InsertOneOptions contains both Method options (sent to DB) and Request options (client side).
type InsertOneOptions struct {
	// Method Options (sent in JSON)
	Ordered *bool `json:"ordered,omitempty"`

	// Request Options (handled by client)
	Timeout *time.Duration
}

// Constructor for the builder pattern
func InsertOne() *InsertOneOptions {
	return &InsertOneOptions{}
}

func (o *InsertOneOptions) SetOrdered(b bool) *InsertOneOptions {
	o.Ordered = &b
	return o
}

func (o *InsertOneOptions) SetTimeout(d time.Duration) *InsertOneOptions {
	o.Timeout = &d
	return o
}
