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
	_, err := cmd.ExtractErrors(503, []byte(resumingResponse))
	if err == nil {
		t.Error("Expected error but got none")
	}
}

// Example response when already exists
const createAlreadyExistsResponse = "{\"status\":{\"insertedIds\":[]},\"errors\":[{\"message\":\"Document already exists with the given _id\",\"errorCode\":\"DOCUMENT_ALREADY_EXISTS\",\"id\":\"4055f085-68d8-4c2d-8d91-90a0722b5fef\",\"title\":\"Document already exists with the given _id\",\"family\":\"REQUEST\",\"scope\":\"DOCUMENT\"}]}"

func TestCommandAlreadyExistsErr(t *testing.T) {
	cmd := command{}
	_, err := cmd.ExtractErrors(200, []byte(createAlreadyExistsResponse))
	t.Logf("err value:\n%s", err)
	if err == nil {
		t.Error("Expected error but got none")
	}
}
