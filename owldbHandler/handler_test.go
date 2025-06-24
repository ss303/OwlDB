package owldbhandler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/RICE-COMP318-FALL24/owldb-p1group35/storage"
)

// TestHelper struct for reusable helper functions
type TestHelper struct {
	handler http.Handler
	t       *testing.T
}

// NewTestHelper initializes the helper
func NewTestHelper(handler http.Handler, t *testing.T) *TestHelper {
	return &TestHelper{handler: handler, t: t}
}

// MakeRequest is a utility to create HTTP requests and return the response
func (h *TestHelper) MakeRequest(method, url string, body io.Reader, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, url, body)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("accept", "application/json")
	w := httptest.NewRecorder()
	h.handler.ServeHTTP(w, req)
	return w
}

// AssertStatusCode is a helper function to validate HTTP status codes
func (h *TestHelper) AssertStatusCode(w *httptest.ResponseRecorder, expected int) {
	if w.Result().StatusCode != expected {
		h.t.Errorf("Expected status code %d, but got %d", expected, w.Result().StatusCode)
	}
}

// DecodeResponseBody reads the response body and unmarshals it into the provided struct
func (h *TestHelper) DecodeResponseBody(w *httptest.ResponseRecorder, v interface{}) {
	body, err := io.ReadAll(w.Result().Body)
	if err != nil {
		h.t.Fatalf("Failed to read response body: %v", err)
	}
	if err := json.Unmarshal(body, v); err != nil {
		h.t.Fatalf("Failed to unmarshal response body: %v", err)
	}
}

// PatchDocument applies a patch operation to a document
func (h *TestHelper) PatchDocument(patchOps []map[string]string, token string) {
	encoded, _ := json.Marshal(patchOps)
	r := bytes.NewReader(encoded)
	h.MakeRequest("PATCH", "http://localhost:3318/v1/database/doc", r, token)
}

// Test Cases

// Test_AddAndRetrieveDatabase tests creating a new database and retrieving it
func Test_AddAndRetrieveDatabase(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create the database
	w := helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")
	helper.AssertStatusCode(w, 201)

	// Retrieve the database
	w = helper.MakeRequest("GET", "http://localhost:3318/v1/database/", nil, "token1")
	helper.AssertStatusCode(w, 200)
}

// Test_AddPatchRetrieveDocument tests adding, patching, and retrieving a document
func Test_AddPatchRetrieveDocument(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	w := helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")
	helper.AssertStatusCode(w, 201)

	// Add the document
	doc := map[string]string{"Description": "Hello everyone"}
	encoded, _ := json.Marshal(doc)
	w = helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc", bytes.NewReader(encoded), "token1")
	helper.AssertStatusCode(w, 201)

	// Patch the document
	patchOps := []map[string]string{{"op": "ObjectAdd", "path": "/newField", "value": "newValue"}}
	helper.PatchDocument(patchOps, "token1")

	// Retrieve the document and check if patched
	w = helper.MakeRequest("GET", "http://localhost:3318/v1/database/doc", nil, "token1")
	helper.AssertStatusCode(w, 200)

	var docContent storage.DocumentContent
	helper.DecodeResponseBody(w, &docContent)
	if _, found := docContent.Content["newField"]; !found {
		t.Errorf("New field not added")
	}
}

// Test_ConcurrentAddingDocs tests concurrent addition of documents
func Test_ConcurrentAddingDocs(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	w := helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")
	helper.AssertStatusCode(w, 201)

	var wg sync.WaitGroup
	encoded, _ := json.Marshal(map[string]string{"Description": "Test doc"})

	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			helper.MakeRequest("PUT", fmt.Sprintf("http://localhost:3318/v1/database/doc%d", i), bytes.NewReader(encoded), "token1")
		}(i)
	}
	wg.Wait()

	// Validate all documents added
	for i := 0; i < 100; i++ {
		w := helper.MakeRequest("GET", fmt.Sprintf("http://localhost:3318/v1/database/doc%d", i), nil, "token1")
		helper.AssertStatusCode(w, 200)
	}
}

// Test_ConcurrentPatching tests concurrent patching of the same document
func Test_ConcurrentPatching(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")

	// Create the document
	doc := map[string]string{"Description": "Test doc"}
	encoded, _ := json.Marshal(doc)
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc", bytes.NewReader(encoded), "token1")

	var wg sync.WaitGroup
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			patchOps := []map[string]string{
				{"op": "ObjectAdd", "path": fmt.Sprintf("/field%d", i), "value": "newValue"},
			}
			helper.PatchDocument(patchOps, "token1")
		}(i)
	}
	wg.Wait()

	// Validate the patches
	w := helper.MakeRequest("GET", "http://localhost:3318/v1/database/doc", nil, "token1")
	helper.AssertStatusCode(w, 200)

	var docContent storage.DocumentContent
	helper.DecodeResponseBody(w, &docContent)

	if len(docContent.Content) != 101 { // Original "Description" + 100 fields
		t.Errorf("Expected 101 fields, got %d", len(docContent.Content))
	}
}

// Test_ConcurrentAddingDatabases tests concurrent creation of databases
func Test_ConcurrentAddingDatabases(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	var wg sync.WaitGroup
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			helper.MakeRequest("PUT", fmt.Sprintf("http://localhost:3318/v1/database%d", i), nil, "token1")
		}(i)
	}
	wg.Wait()

	// Validate all databases added
	for i := 0; i < 100; i++ {
		w := helper.MakeRequest("GET", fmt.Sprintf("http://localhost:3318/v1/database%d/", i), nil, "token1")
		helper.AssertStatusCode(w, 200)
	}
}

// Test_ConcurrentDocumentDeletion tests concurrent deletion of the same document
func Test_ConcurrentDocumentDeletion(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create the document
	doc := map[string]string{"Description": "Test doc"}
	encoded, _ := json.Marshal(doc)
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc", bytes.NewReader(encoded), "token1")

	var wg sync.WaitGroup
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			helper.MakeRequest("DELETE", "http://localhost:3318/v1/database/doc", nil, "token1")
		}()
	}
	wg.Wait()

	// Validate the deletion
	w := helper.MakeRequest("GET", "http://localhost:3318/v1/database/doc", nil, "token1")
	helper.AssertStatusCode(w, 404)
}

// Test_ConcurrentFetchingAndUpdatingDocs tests concurrent fetching and updating of multiple documents
func Test_ConcurrentFetchingAndUpdatingDocs(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	w := helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")
	helper.AssertStatusCode(w, 201)

	// Create initial documents
	for i := 0; i < 50; i++ {
		encoded, _ := json.Marshal(map[string]string{"Description": fmt.Sprintf("Document %d", i)})
		helper.MakeRequest("PUT", fmt.Sprintf("http://localhost:3318/v1/database/doc%d", i), bytes.NewReader(encoded), "token1")
	}

	var wg sync.WaitGroup
	wg.Add(100)

	// Concurrently fetch and update documents
	for i := 0; i < 50; i++ {
		go func(i int) {
			defer wg.Done()
			// Fetch the document
			helper.MakeRequest("GET", fmt.Sprintf("http://localhost:3318/v1/database/doc%d", i), nil, "token1")
		}(i)

		go func(i int) {
			defer wg.Done()
			// Update the document
			updatedData := map[string]string{"Description": fmt.Sprintf("Updated Document %d", i)}
			encoded, _ := json.Marshal(updatedData)
			helper.MakeRequest("PUT", fmt.Sprintf("http://localhost:3318/v1/database/doc%d", i), bytes.NewReader(encoded), "token1")
		}(i)
	}

	wg.Wait()

	// Validate that all documents were updated
	for i := 0; i < 50; i++ {
		w := helper.MakeRequest("GET", fmt.Sprintf("http://localhost:3318/v1/database/doc%d", i), nil, "token1")
		helper.AssertStatusCode(w, 200)

		var docContent storage.DocumentContent
		helper.DecodeResponseBody(w, &docContent)
		expectedDescription := fmt.Sprintf("Updated Document %d", i)
		if docContent.Content["Description"] != expectedDescription {
			t.Errorf("Expected description '%s', got '%s'", expectedDescription, docContent.Content["Description"])
		}
	}
}

// Test_GettingNonExistentDoc tests fetching a non-existent document
func Test_GettingNonExisttDoc(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Try fetching a non-existent document
	w := helper.MakeRequest("GET", "http://localhost:3318/v1/database/nonexistent_doc", nil, "token1")
	helper.AssertStatusCode(w, 404)
}

// Test_AddingDocToNonExistentDB tests adding a document to a non-existent database
func Test_AddingDocToNonExisttDB(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Try adding a document to a non-existent database
	doc := map[string]string{"Description": "Test Doc"}
	encoded, _ := json.Marshal(doc)
	w := helper.MakeRequest("PUT", "http://localhost:3318/v1/nonexistent_database/doc", bytes.NewReader(encoded), "token1")
	helper.AssertStatusCode(w, 404)
}

// Test_GettingEmptyDatabase tests fetching from an empty database
func Test_GettingEmptyDb(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create a new empty database
	helper.MakeRequest("PUT", "http://localhost:3318/v1/emptydb", nil, "token1")

	// Try to fetch all documents in the empty database
	w := helper.MakeRequest("GET", "http://localhost:3318/v1/emptydb/", nil, "token1")
	helper.AssertStatusCode(w, 200)

	var response []storage.DocumentContent
	helper.DecodeResponseBody(w, &response)

	if len(response) != 0 {
		t.Errorf("Expected 0 documents, got %d", len(response))
	}
}

// Test_LargeDocumentUpload tests uploading a large document
func Test_LargeDocUpload(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create a new database
	helper.MakeRequest("PUT", "http://localhost:3318/v1/largedb", nil, "token1")

	// Create a large document
	largeData := map[string]string{}
	for i := 0; i < 10000; i++ {
		largeData[fmt.Sprintf("key%d", i)] = fmt.Sprintf("value%d", i)
	}
	encoded, _ := json.Marshal(largeData)

	// Upload the large document
	w := helper.MakeRequest("PUT", "http://localhost:3318/v1/largedb/large_doc", bytes.NewReader(encoded), "token1")
	helper.AssertStatusCode(w, 201)
}

// Test_Concurrent_UpdatingSameDoc tests concurrent updates to the same document
func Test_Concurrent_UpdateSameDoc(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create the database and document
	helper.MakeRequest("PUT", "http://localhost:3318/v1/concurrentdb", nil, "token1")
	doc := map[string]string{"Description": "Initial"}
	encoded, _ := json.Marshal(doc)
	helper.MakeRequest("PUT", "http://localhost:3318/v1/concurrentdb/same_doc", bytes.NewReader(encoded), "token1")

	var wg sync.WaitGroup
	wg.Add(100)

	// Concurrently update the same document
	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			updatedData := map[string]string{"Description": fmt.Sprintf("Update %d", i)}
			encoded, _ := json.Marshal(updatedData)
			helper.MakeRequest("PUT", "http://localhost:3318/v1/concurrentdb/same_doc", bytes.NewReader(encoded), "token1")
		}(i)
	}
	wg.Wait()

	// Fetch the final version of the document
	w := helper.MakeRequest("GET", "http://localhost:3318/v1/concurrentdb/same_doc", nil, "token1")
	helper.AssertStatusCode(w, 200)

	var docContent storage.DocumentContent
	helper.DecodeResponseBody(w, &docContent)
	fmt.Println("Final Document:", docContent)
}

// Test_Concurrent_InsertAndRemoveSameDocNoOverwrite tests concurrent insertion and removal of the same document without overwriting
func Test_Concurrent_InsertRemoveSameDocNoOverwrite(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Initial document
	doc := map[string]string{"Description": "Hello everyone"}
	encoded, _ := json.Marshal(doc)

	// Create the database
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")

	var wg sync.WaitGroup
	numRequests := 10000
	wg.Add(numRequests)

	// Concurrent insertions and deletions (nooverwrite mode)
	for i := 0; i < numRequests/2; i++ {
		go func() {
			defer wg.Done()
			helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc?mode=nooverwrite", bytes.NewReader(encoded), "token1")
		}()
	}

	for i := 0; i < numRequests/2; i++ {
		go func() {
			defer wg.Done()
			helper.MakeRequest("DELETE", "http://localhost:3318/v1/database/doc", nil, "token1")
		}()
	}

	wg.Wait()

	// Ensure there is at most one document in the database
	w := helper.MakeRequest("GET", "http://localhost:3318/v1/database/", nil, "token1")
	helper.AssertStatusCode(w, 200)

	var docs []map[string]interface{}
	helper.DecodeResponseBody(w, &docs)
	if len(docs) > 1 {
		t.Error("Error in adding and removing document")
	}
}

// Test_Concurrent_InsertAndRemoveSameDocOverwrite tests concurrent insertion and removal of the same document with overwriting
func Test_Concurrent_InsertRemoveSameDocOverwrite(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Initial document
	doc := map[string]string{"Description": "Hello everyone"}
	encoded, _ := json.Marshal(doc)

	// Create the database
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")

	var wg sync.WaitGroup
	numRequests := 10000
	wg.Add(numRequests)

	// Concurrent insertions and deletions (overwrite mode)
	for i := 0; i < numRequests/2; i++ {
		go func() {
			defer wg.Done()
			helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc?mode=overwrite", bytes.NewReader(encoded), "token1")
		}()
	}

	for i := 0; i < numRequests/2; i++ {
		go func() {
			defer wg.Done()
			helper.MakeRequest("DELETE", "http://localhost:3318/v1/database/doc", nil, "token1")
		}()
	}

	wg.Wait()

	// Ensure there is at most one document in the database
	w := helper.MakeRequest("GET", "http://localhost:3318/v1/database/", nil, "token1")
	helper.AssertStatusCode(w, 200)

	var docs []map[string]interface{}
	helper.DecodeResponseBody(w, &docs)
	if len(docs) > 1 {
		t.Error("Error in adding and removing document")
	}
}

// Test_Concurrent_AddingCollections tests concurrent creation of collections
func Test_Concurrent_AddingCollections(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create initial database and document
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")
	doc := map[string]string{"Description": "Hello everyone"}
	encoded, _ := json.Marshal(doc)
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc", bytes.NewReader(encoded), "token1")

	// Concurrently create 1000 collections
	var wg sync.WaitGroup
	requestCount := 1000
	wg.Add(requestCount)

	for i := 0; i < requestCount; i++ {
		go func(i int) {
			defer wg.Done()
			helper.MakeRequest("PUT", fmt.Sprintf("http://localhost:3318/v1/database/doc/col%d/", i), nil, "token1")
		}(i)
	}

	wg.Wait()

	// Ensure all collections were added
	for i := 0; i < requestCount; i++ {
		w := helper.MakeRequest("GET", fmt.Sprintf("http://localhost:3318/v1/database/doc/col%d/", i), nil, "token1")
		helper.AssertStatusCode(w, 200)
	}
}

// Test_Concurrent_AddingDocsWithErrors tests concurrent adding of documents with errors
func Test_Concurrent_AddDocsWithErrors(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Document content
	doc := map[string]string{"Description": "Hello everyone"}
	encoded, _ := json.Marshal(doc)

	// Create initial database
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")

	// Concurrently add valid and invalid documents (half go to nonexistent database)
	var wg sync.WaitGroup
	wg.Add(100)

	for i := 0; i < 50; i++ {
		go func(i int) {
			defer wg.Done()
			helper.MakeRequest("PUT", fmt.Sprintf("http://localhost:3318/v1/database/doc%d", i), bytes.NewReader(encoded), "token1")
		}(i)
	}

	for i := 50; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			helper.MakeRequest("PUT", fmt.Sprintf("http://localhost:3318/v1/database2/doc%d", i), bytes.NewReader(encoded), "token1")
		}(i)
	}

	wg.Wait()

	// Ensure all valid documents were added
	for i := 0; i < 50; i++ {
		w := helper.MakeRequest("GET", fmt.Sprintf("http://localhost:3318/v1/database/doc%d", i), nil, "token1")
		helper.AssertStatusCode(w, 200)
	}
}

// Test_Concurrent_OverwritingDocs tests concurrent overwriting of documents
func Test_Concurrent_OverwritDocs(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create initial document
	doc := map[string]string{"Description": "Hello everyone"}
	encoded, _ := json.Marshal(doc)
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")

	// Concurrently add 100 documents
	var wg sync.WaitGroup
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			helper.MakeRequest("PUT", fmt.Sprintf("http://localhost:3318/v1/database/doc%d", i), bytes.NewReader(encoded), "token1")
		}(i)
	}

	wg.Wait()

	// Overwrite the documents concurrently
	updatedDoc := map[string]string{"Description": "Goodbye Everyone"}
	encoded2, _ := json.Marshal(updatedDoc)
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			helper.MakeRequest("PUT", fmt.Sprintf("http://localhost:3318/v1/database/doc%d", i), bytes.NewReader(encoded2), "token1")
		}(i)
	}

	wg.Wait()

	// Verify the overwrites
	for i := 0; i < 100; i++ {
		w := helper.MakeRequest("GET", fmt.Sprintf("http://localhost:3318/v1/database/doc%d", i), nil, "token1")
		helper.AssertStatusCode(w, 200)

		var docContent storage.DocumentContent
		helper.DecodeResponseBody(w, &docContent)
		if docContent.Content["Description"] != "Goodbye Everyone" {
			t.Errorf("Document %d was not correctly overwritten", i)
		}
	}
}

// Test_Concurrent_AddingDatabases tests concurrent addition of databases
func Test_Concurrent_AddDatabases(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Concurrently create 100 databases
	var wg sync.WaitGroup
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			helper.MakeRequest("PUT", fmt.Sprintf("http://localhost:3318/v1/database%d", i), nil, "token1")
		}(i)
	}

	wg.Wait()

	// Ensure all databases were added
	for i := 0; i < 100; i++ {
		w := helper.MakeRequest("GET", fmt.Sprintf("http://localhost:3318/v1/database%d/", i), nil, "token1")
		helper.AssertStatusCode(w, 200)
	}
}

// Test_Concurrent_AddingDatabasesWithErrors tests concurrent addition of databases with errors
func Test_Concurrent_AddDatabasesWithErrors(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Concurrently add 100 databases, with errors (some will overwrite existing ones)
	var wg sync.WaitGroup
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			helper.MakeRequest("PUT", fmt.Sprintf("http://localhost:3318/v1/database%d", i/2), nil, "token1")
		}(i)
	}

	wg.Wait()

	// Ensure only unique databases were added
	for i := 0; i < 50; i++ {
		w := helper.MakeRequest("GET", fmt.Sprintf("http://localhost:3318/v1/database%d/", i), nil, "token1")
		helper.AssertStatusCode(w, 200)
	}
}

// Test_Concurrent_GetAndPut tests concurrent GET and PUT requests for the same document
func Test_Concurrent_GetPut(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create the database
	helper.MakeRequest("PUT", "http://localhost:3318/v1/testdb", nil, "token1")

	// Concurrently create and fetch documents
	var wg sync.WaitGroup
	wg.Add(200)

	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			doc := map[string]string{"Data": fmt.Sprintf("Document %d", i)}
			encoded, _ := json.Marshal(doc)
			helper.MakeRequest("PUT", fmt.Sprintf("http://localhost:3318/v1/testdb/doc%d", i), bytes.NewReader(encoded), "token1")
		}(i)

		go func(i int) {
			defer wg.Done()
			helper.MakeRequest("GET", fmt.Sprintf("http://localhost:3318/v1/testdb/doc%d", i), nil, "token1")
		}(i)
	}

	wg.Wait()

	// Verify all documents exist
	for i := 0; i < 100; i++ {
		w := helper.MakeRequest("GET", fmt.Sprintf("http://localhost:3318/v1/testdb/doc%d", i), nil, "token1")
		helper.AssertStatusCode(w, 200)
	}
}

// Test_UpdatingNonExistentDoc tests updating a non-existent document
func Test_UpdatingNonExistDoc(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Try updating a non-existent document
	doc := map[string]string{"Description": "Updated Doc"}
	encoded, _ := json.Marshal(doc)
	w := helper.MakeRequest("PUT", "http://localhost:3318/v1/database/nonexistent_doc", bytes.NewReader(encoded), "token1")
	helper.AssertStatusCode(w, 404)
}

// Test_AddingCollection tests adding a collection to a document
func Test_AddCollection(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create the database and document
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")
	doc := map[string]string{"Description": "Test Doc"}
	encoded, _ := json.Marshal(doc)
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc", bytes.NewReader(encoded), "token1")

	// Add a collection to the document
	w := helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc/col1/", nil, "token1")
	helper.AssertStatusCode(w, 201)

	// Retrieve the collection
	w = helper.MakeRequest("GET", "http://localhost:3318/v1/database/doc/col1/", nil, "token1")
	helper.AssertStatusCode(w, 200)
}

// Test_DeletingCollection tests deleting a collection from a document
func Test_DeleteCollection(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create the database and document
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")
	doc := map[string]string{"Description": "Test Doc"}
	encoded, _ := json.Marshal(doc)
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc", bytes.NewReader(encoded), "token1")

	// Add a collection
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc/col1/", nil, "token1")

	// Delete the collection
	w := helper.MakeRequest("DELETE", "http://localhost:3318/v1/database/doc/col1/", nil, "token1")
	helper.AssertStatusCode(w, 204)

	// Verify collection deletion
	w = helper.MakeRequest("GET", "http://localhost:3318/v1/database/doc/col1/", nil, "token1")
	helper.AssertStatusCode(w, 404)
}

// Test_AddingDocToCollection tests adding a document to a collection
func Test_AddDocToCollection(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create the database and document
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")
	doc := map[string]string{"Description": "Test Doc"}
	encoded, _ := json.Marshal(doc)
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc", bytes.NewReader(encoded), "token1")

	// Add a collection
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc/col1/", nil, "token1")

	// Add a document to the collection
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc/col1/doc2", bytes.NewReader(encoded), "token1")

	// Verify the document was added
	w := helper.MakeRequest("GET", "http://localhost:3318/v1/database/doc/col1/doc2", nil, "token1")
	helper.AssertStatusCode(w, 200)
}

// Test_AddingSameNameDB tests adding a database with the same name
func Test_AddSameNameDB(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create a database
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")

	// Try to create the same database again
	w := helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")
	helper.AssertStatusCode(w, 400)
}

// Test_CreatingDoc tests creating a document in a database
func Test_CreateDoc(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create a database
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")

	// Create a document in the database
	doc := map[string]string{"Description": "Test Doc"}
	encoded, _ := json.Marshal(doc)
	w := helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc", bytes.NewReader(encoded), "token1")
	helper.AssertStatusCode(w, 201)
}

// Test_OverwritingDoc tests overwriting an existing document in a database
func Test_OverwriteDoc(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create a database
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")

	// Create a document
	doc := map[string]string{"Description": "Test Doc"}
	encoded, _ := json.Marshal(doc)
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc", bytes.NewReader(encoded), "token1")

	// Overwrite the document
	updatedDoc := map[string]string{"Description": "Updated Doc"}
	encoded2, _ := json.Marshal(updatedDoc)
	w := helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc", bytes.NewReader(encoded2), "token1")
	helper.AssertStatusCode(w, 200)
}

// Test_DocIntoImaginaryCollection tests adding a document into a non-existent collection
func Test_DocInImaginaryCollection(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Try to add a document into a non-existent collection
	doc := map[string]string{"Description": "Test Doc"}
	encoded, _ := json.Marshal(doc)
	w := helper.MakeRequest("PUT", "http://localhost:3318/v1/database2/doc", bytes.NewReader(encoded), "token1")
	helper.AssertStatusCode(w, 404)
}

// Test_RequestDocImaginaryCollection tests requesting a document from a non-existent collection
func Test_ReqDocImaginaryCollection(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Try to request a document from a non-existent collection
	w := helper.MakeRequest("GET", "http://localhost:3318/v1/database2/doc", nil, "token1")
	helper.AssertStatusCode(w, 404)
}

// Test_RequestImaginaryCollection tests requesting a non-existent collection
func Test_ReqImaginaryCollection(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Try to request a non-existent collection
	w := helper.MakeRequest("GET", "http://localhost:3318/v1/database2/", nil, "token1")
	helper.AssertStatusCode(w, 404)
}

// Test_Concurrent_AddingDocs tests concurrent addition of documents
func Test_Concurrent_AddDocs(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create a database
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")

	// Concurrently add 100 documents
	var wg sync.WaitGroup
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			doc := map[string]string{"Description": fmt.Sprintf("Document %d", i)}
			encoded, _ := json.Marshal(doc)
			helper.MakeRequest("PUT", fmt.Sprintf("http://localhost:3318/v1/database/doc%d", i), bytes.NewReader(encoded), "token1")
		}(i)
	}

	wg.Wait()

	// Verify all documents were added
	for i := 0; i < 100; i++ {
		w := helper.MakeRequest("GET", fmt.Sprintf("http://localhost:3318/v1/database/doc%d", i), nil, "token1")
		helper.AssertStatusCode(w, 200)
	}
}

// Test_Concurrent_PostingDocs tests concurrent posting of documents
func Test_Concurrent_PostingDocs(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create a database
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")

	// Concurrently post 100 documents
	var wg sync.WaitGroup
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			doc := map[string]string{"Description": fmt.Sprintf("Document %d", i)}
			encoded, _ := json.Marshal(doc)
			helper.MakeRequest("POST", "http://localhost:3318/v1/database/", bytes.NewReader(encoded), "token1")
		}(i)
	}

	wg.Wait()

	// Verify all documents were posted
	w := helper.MakeRequest("GET", "http://localhost:3318/v1/database/", nil, "token1")
	helper.AssertStatusCode(w, 200)

	var docs []storage.DocumentContent
	helper.DecodeResponseBody(w, &docs)

	if len(docs) != 100 {
		t.Errorf("Expected 100 documents, but received %d", len(docs))
	}
}

// Test_Concurrent_PatchingDocAddField tests concurrent patching of the same document to add fields
func Test_Concurrent_PatchingDocAddField(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create a database and a document
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")
	doc := map[string]string{"Description": "Hello everyone"}
	encoded, _ := json.Marshal(doc)
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc", bytes.NewReader(encoded), "token1")

	// Concurrently patch the document to add fields
	var wg sync.WaitGroup
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			patchOps := []map[string]string{
				{"op": "ObjectAdd", "path": fmt.Sprintf("/brad%d", i), "value": "hello"},
			}
			encodedPatch, _ := json.Marshal(patchOps)
			helper.MakeRequest("PATCH", "http://localhost:3318/v1/database/doc", bytes.NewReader(encodedPatch), "token1")
		}(i)
	}

	wg.Wait()

	// Verify all patches went through
	w := helper.MakeRequest("GET", "http://localhost:3318/v1/database/doc", nil, "token1")
	helper.AssertStatusCode(w, 200)

	var docContent storage.DocumentContent
	helper.DecodeResponseBody(w, &docContent)

	if len(docContent.Content) != 101 { // 100 added fields + original description
		t.Error("Not all patches went through")
	}
}

// Test_Concurrent_PatchingDocAddArray tests concurrent patching of the same document to add to an array
func Test_Concurrent_PatchingDocAddArray(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create a database and a document
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")
	doc := map[string][]string{"Description": {"Cool"}}
	encoded, _ := json.Marshal(doc)
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc", bytes.NewReader(encoded), "token1")

	// Concurrently patch the document to add to the array
	var wg sync.WaitGroup
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			patchOps := []map[string]string{
				{"op": "ArrayAdd", "path": "/Description", "value": fmt.Sprintf("hello%d", i)},
			}
			encodedPatch, _ := json.Marshal(patchOps)
			helper.MakeRequest("PATCH", "http://localhost:3318/v1/database/doc", bytes.NewReader(encodedPatch), "token1")
		}(i)
	}

	wg.Wait()

	// Verify all patches went through
	w := helper.MakeRequest("GET", "http://localhost:3318/v1/database/doc", nil, "token1")
	helper.AssertStatusCode(w, 200)

	var docContent storage.DocumentContent
	helper.DecodeResponseBody(w, &docContent)

	arrayUncasted := docContent.Content["Description"].([]interface{})
	if len(arrayUncasted) != 101 { // 100 added elements + original
		t.Error("Not all elements were added to the array")
	}
}

// Test_Concurrent_PatchingDocRemoveArray tests concurrent patching to remove from an array
func Test_Concurrent_PatchingDocRemoveArray(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create a database and a document
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")
	doc := map[string][]string{"Description": {"Cool"}}
	encoded, _ := json.Marshal(doc)
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database/doc", bytes.NewReader(encoded), "token1")

	// Add elements to the array
	var wg sync.WaitGroup
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			patchOps := []map[string]string{
				{"op": "ArrayAdd", "path": "/Description", "value": fmt.Sprintf("hello%d", i)},
			}
			encodedPatch, _ := json.Marshal(patchOps)
			helper.MakeRequest("PATCH", "http://localhost:3318/v1/database/doc", bytes.NewReader(encodedPatch), "token1")
		}(i)
	}

	wg.Wait()

	// Remove elements from the array
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			patchOps := []map[string]string{
				{"op": "ArrayRemove", "path": "/Description", "value": fmt.Sprintf("hello%d", i)},
			}
			encodedPatch, _ := json.Marshal(patchOps)
			helper.MakeRequest("PATCH", "http://localhost:3318/v1/database/doc", bytes.NewReader(encodedPatch), "token1")
		}(i)
	}

	wg.Wait()

	// Verify all elements were removed
	w := helper.MakeRequest("GET", "http://localhost:3318/v1/database/doc", nil, "token1")
	helper.AssertStatusCode(w, 200)

	var docContent storage.DocumentContent
	helper.DecodeResponseBody(w, &docContent)

	arrayUncasted := docContent.Content["Description"].([]interface{})
	if len(arrayUncasted) != 1 { // Only the original element should remain
		t.Error("Not all elements were removed from the array")
	}
}

// Test_Concurrent_AddingDocsCheckingOrder tests the order of documents in a concurrent environment
func Test_Concurrent_AddDocsCheckingOrder(t *testing.T) {
	handler, _ := New("../storage/anyschema.json", "../nametotoken.json")
	helper := NewTestHelper(handler, t)

	// Create a database
	helper.MakeRequest("PUT", "http://localhost:3318/v1/database", nil, "token1")

	// Concurrently add 100 documents
	var wg sync.WaitGroup
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			doc := map[string]string{"Description": fmt.Sprintf("Document %d", i)}
			encoded, _ := json.Marshal(doc)
			helper.MakeRequest("PUT", fmt.Sprintf("http://localhost:3318/v1/database/doc%d", i), bytes.NewReader(encoded), "token1")
		}(i)
	}

	wg.Wait()

	// Verify the order using interval query
	w := helper.MakeRequest("GET", "http://localhost:3318/v1/database/?interval=[doc99,z]", nil, "token1")
	helper.AssertStatusCode(w, 200)

	var docs []storage.DocumentContent
	helper.DecodeResponseBody(w, &docs)

	if len(docs) != 1 {
		t.Error("Only one document should be lexically greater than or equal to doc99")
	}

	if docs[0].Path != "/v1/database/doc99" {
		t.Error("Expected doc99 to be the only document returned")
	}
}
