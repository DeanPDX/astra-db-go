package astradb_test

import (
	"context"
	"testing"

	astradb "github.com/datastax/astra-db-go"
)

// Example response from insertMany
const insertManyResponse = "{\"status\":{\"insertedIds\":[\"61c2a03a-c09e-42f5-82a0-3ac09e42f503\",\"c9a9cfc6-be22-408b-a9cf-c6be22908b11\",\"bacd85a8-8ed2-47f0-8d85-a88ed2b7f0d3\",\"5a1b4c50-96e1-4604-9b4c-5096e19604af\",\"95520407-62cb-446a-9204-0762cb946a48\",\"b5bf00a6-c6bd-4151-bf00-a6c6bdb151e9\",\"37155935-825c-4031-9559-35825cf03118\",\"80add809-295c-4d82-add8-09295ced82f3\",\"d96710b5-21f9-4eb8-a710-b521f90eb883\",\"3e58aad9-07a3-4b9b-98aa-d907a30b9b32\",\"60bc0e68-2b55-4ff8-bc0e-682b556ff888\",\"59bdd2e1-8d90-425c-bdd2-e18d90425c7b\",\"c5f4ee93-ed02-42e4-b4ee-93ed0212e4db\",\"2664f4d7-0b64-4678-a4f4-d70b64e678bb\",\"7551ba1f-cc20-4c32-91ba-1fcc200c3226\",\"b50ef75e-1052-4e1d-8ef7-5e10526e1d82\",\"8c6d94ed-41c6-4355-ad94-ed41c613550b\",\"05d47ef5-7544-4fe8-947e-f575440fe861\",\"7931a48a-e575-49e8-b1a4-8ae57559e82c\",\"dc62f18a-da74-4f5f-a2f1-8ada741f5f1b\",\"b230601c-8d84-446a-b060-1c8d84546afa\",\"7ffca2e2-20cb-4336-bca2-e220cb6336dc\",\"7ac46bc2-ae92-4d71-846b-c2ae92fd71cb\",\"fbdb4808-6b9c-4eef-9b48-086b9caeef19\",\"ff51f284-1841-459b-91f2-841841959b25\",\"6b658ccd-2fd0-4141-a58c-cd2fd0914129\",\"df09fcb5-51d0-4e21-89fc-b551d0fe2113\",\"9751e7bc-ce21-4205-91e7-bcce21020524\",\"82ce476e-df90-4605-8e47-6edf90760540\",\"64b511e9-156e-4731-b511-e9156e87314c\"]}}"

// Example response when create/delete happens
const createDeleteResponse = "{\"status\":{\"ok\":1}}"

func TestNullDB(t *testing.T) {
	var db *astradb.Db = nil
	c := db.Collection("nildb")
	_, err := c.CountDocuments(context.Background(), nil, 100)
	if err == nil {
		t.Errorf("Expected error. Got %v", err)
	}
}
