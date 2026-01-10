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

package astradb

import (
	"testing"
)

// Example response when your application is resuming
const resumingResponse = "{\"message\":\"Your database is resuming from hibernation and will be available in the next few minutes.\"}"

func TestCommandDBResuming(t *testing.T) {
	cmd := command{}
	_, _, err := cmd.ExtractErrors(503, []byte(resumingResponse), nil)
	if err == nil {
		t.Error("Expected error but got none")
	}
}

// Example response when already exists
const createAlreadyExistsResponse = "{\"status\":{\"insertedIds\":[]},\"errors\":[{\"message\":\"Document already exists with the given _id\",\"errorCode\":\"DOCUMENT_ALREADY_EXISTS\",\"id\":\"4055f085-68d8-4c2d-8d91-90a0722b5fef\",\"title\":\"Document already exists with the given _id\",\"family\":\"REQUEST\",\"scope\":\"DOCUMENT\"}]}"

func TestCommandAlreadyExistsErr(t *testing.T) {
	cmd := command{}
	_, _, err := cmd.ExtractErrors(200, []byte(createAlreadyExistsResponse), nil)
	t.Logf("err value:\n%s", err)
	if err == nil {
		t.Error("Expected error but got none")
	}
}

// Example response with warnings
const warningsResponse = "{\"data\":{\"documents\":[{\"number_of_pages\":281,\"author\":\"Harper Lee\",\"genres\":[\"Fiction\",\"Classic\"],\"rating\":4.8,\"title\":\"To Kill a Mockingbird\",\"is_checked_out\":false},{\"number_of_pages\":328,\"author\":\"George Orwell\",\"genres\":[\"Dystopian\",\"Science Fiction\"],\"rating\":4.7,\"title\":\"1984\",\"is_checked_out\":true},{\"number_of_pages\":279,\"author\":\"Jane Austen\",\"genres\":[\"Romance\",\"Classic\"],\"rating\":4.6,\"title\":\"Pride and Prejudice\",\"is_checked_out\":false}],\"nextPageState\":null},\"status\":{\"sortedRowCount\":6,\"projectionSchema\":{\"title\":{\"type\":\"text\"},\"author\":{\"type\":\"text\"},\"genres\":{\"type\":\"list\",\"valueType\":\"text\"},\"is_checked_out\":{\"type\":\"boolean\"},\"number_of_pages\":{\"type\":\"int\"},\"rating\":{\"type\":\"float\"}},\"warnings\":[{\"errorCode\":\"IN_MEMORY_SORTING_DUE_TO_NON_PARTITION_SORTING\",\"message\":\"The command used columns in the sort clause that are not part of the partition sorting, and so the query was sorted in memory.\\n      \\nThe table default_keyspace.go_test_books has the partition sorting columns: [None].\\nThe command sorted on the columns: rating.\\n\\nThe command was executed using in memory sorting rather than taking advantage of the partition sorting on disk. This can have performance implications on large tables.\\n\\nSee documentation for best practices for sorting.\",\"family\":\"REQUEST\",\"scope\":\"WARNING\",\"title\":\"Sorting by non partition sorting columns\",\"id\":\"13ca45dd-79de-4e56-b8e9-286482a21bd7\"},{\"errorCode\":\"ZERO_FILTER_OPERATIONS\",\"message\":\"Zero filters were provided in the filter for this query. \\n\\nProviding zero filters will return all rows in the table, which may have poor performance when the table is large. For the best performance, include one or more filters using the primary key or indexes.\\n\\nThe table default_keyspace.go_test_books has the primary key: title(text).\\nAnd has indexes on the columns: [None].\\n\\nThe query was executed without taking advantage of the primary key or indexes on the table, this can have performance implications on large tables.\\n\\nSee documentation for best practices for filtering.\",\"family\":\"REQUEST\",\"scope\":\"WARNING\",\"title\":\"Zero operations provided in query filter\",\"id\":\"f0fab1f8-906b-411f-8cd1-736fd8a3d9e2\"}]}}"

func TestCommandWarnings(t *testing.T) {
	cmd := command{}
	_, warnings, err := cmd.ExtractErrors(200, []byte(warningsResponse), nil)
	if err != nil {
		t.Errorf("Did not expect error but got: %v", err)
	}
	const expected = 2
	if len(warnings) != expected {
		t.Errorf("Expected %d warnings but got: %d", expected, len(warnings))
	}
}
