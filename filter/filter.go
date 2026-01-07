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

// Package filter defines filtering options for Astra DB queries.
package filter

import "encoding/json"

// F represents a map of filters to be applied to an Astra DB query.
// Use this in conjunction with [A] if you want to pass filters as
// they appear in JSON data.
//
// Example:
//
//	filters := filter.F{
//		"$and": filter.A{
//			filter.F{"$or": filter.A{
//				filter.F{"is_checked_out": false},
//				filter.F{"number_of_pages": filter.F{"$lt": 300}},
//			}},
//			filter.F{"$or": filter.A{
//				filter.F{"genres": filter.F{"$in": filter.A{"Fantasy", "Romance"}}},
//				filter.F{"publication_year": filter.F{"$gte": 2002}},
//			}},
//		},
//	}
//
// See [FilterOperator] for available operators.
type F map[string]any

// A represents a slice/array of filters to be applied to an Astra DB query.
// Use this in conjunction with [F] if you want to pass filters as
// they appear in JSON data.
//
// Example:
//
//	filters := filter.F{
//		"$and": filter.A{
//			filter.F{"$or": filter.A{
//				filter.F{"is_checked_out": false},
//				filter.F{"number_of_pages": filter.F{"$lt": 300}},
//			}},
//			filter.F{"$or": filter.A{
//				filter.F{"genres": filter.F{"$in": filter.A{"Fantasy", "Romance"}}},
//				filter.F{"publication_year": filter.F{"$gte": 2002}},
//			}},
//		},
//	}
type A []any

// FilterOperator represents the operation type (Eq, Gt, etc.)
type FilterOperator string

const (
	OpAnd              FilterOperator = "$and"
	OpOr               FilterOperator = "$or"
	OpNot              FilterOperator = "$not"
	OpGreaterThan      FilterOperator = "$gt"
	OpGreaterThanEqual FilterOperator = "$gte"
	OpLessThan         FilterOperator = "$lt"
	OpLessThanEqual    FilterOperator = "$lte"
	OpEqual            FilterOperator = "$eq"
	OpNotEqual         FilterOperator = "$ne"
	OpIn               FilterOperator = "$in"
	OpNotIn            FilterOperator = "$nin"
	OpExists           FilterOperator = "$exists"
	OpAll              FilterOperator = "$all"
	OpSize             FilterOperator = "$size"
)

// Filter represents a collection of filters.
type Filter struct {
	// The operator. Such as "$or"
	op FilterOperator
	// The field to perform an operation on. Example: "_id".
	field string
	// The value to filter for based on `op`.
	value any
	// Child filters. Should never be populated if field/value are also populated.
	children []Filter
}

func (f Filter) MarshalJSON() ([]byte, error) {
	if len(f.children) > 0 {
		// We have child commands. Create a map and marshal them like this:
		// "$or": [...]
		filters := make(map[FilterOperator]any)
		filters[f.op] = f.children
		return json.Marshal(filters)
	}
	if len(f.field) > 0 {
		if len(f.op) == 0 || f.op == OpEqual {
			// We have a default filter which is the same as equals. Marshal it into something like:
			// "_id": 1
			filters := make(map[string]any)
			filters[f.field] = f.value
			return json.Marshal(filters)
		}
		// We have another op. Marshal it into something like:
		// "number_of_pages": { "$lt": 300 }
		filters := make(map[string]map[FilterOperator]any)
		filters[f.field] = map[FilterOperator]any{f.op: f.value}
		return json.Marshal(filters)
	}
	// Nothing we can do here.
	return json.Marshal(nil)
}

func Eq(key string, val any) Filter {
	return Filter{
		op:    OpEqual,
		field: key,
		value: val,
	}
}

func Lt(key string, val any) Filter {
	return Filter{
		op:    OpLessThan,
		field: key,
		value: val,
	}
}

func Gte(key string, val any) Filter {
	return Filter{op: OpGreaterThanEqual, field: key, value: val}
}

func In(key string, vals ...any) Filter {
	return Filter{op: OpIn, field: key, value: vals}
}

func And(children ...Filter) Filter {
	return Filter{
		op:       OpAnd,
		children: children,
	}
}

func Or(children ...Filter) Filter {
	return Filter{
		op:       OpOr,
		children: children,
	}
}

func (f *Filter) Eq(key string, val any) Filter {
	return Filter{
		field: key,
		value: val,
	}
}
