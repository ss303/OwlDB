package storage

import (
	"encoding/json"
	"math/rand/v2"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/RICE-COMP318-FALL24/owldb-p1group35/jsondata"
	"github.com/RICE-COMP318-FALL24/owldb-p1group35/skiplist"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// Checks if a slice contains a specific element
func hasElement(slice []map[string]interface{}, target map[string]interface{}) bool {
	for _, item := range slice {
		if reflect.DeepEqual(item, target) {
			return true
		}
	}
	return false
}

// Generates a random string of specified length
func generateString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	randomBytes := make([]byte, length)

	// Seed the random number generator
	seedSrc := rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))
	randomizer := rand.New(seedSrc)

	for i := range randomBytes {
		randomBytes[i] = charset[randomizer.IntN(len(charset))]
	}
	return string(randomBytes)
}

// Creates a random JSON object with numKeys and marshals it
func createRandomJSON(numKeys int) []byte {
	keys := make([]string, 0)
	values := make([]interface{}, 0)

	for i := 0; i < numKeys; i++ {
		keys = append(keys, generateString(5))
		randomValue := rand.Float64()

		switch {
		case randomValue < 0.2:
			values = append(values, rand.IntN(100)) // Random integer
		case randomValue < 0.4:
			values = append(values, generateString(6)) // Random string
		case randomValue < 0.6:
			intSlice := make([]int, 0)
			for j := 0; j < rand.IntN(10); j++ {
				intSlice = append(intSlice, rand.IntN(100))
			}
			values = append(values, intSlice) // Slice of integers
		case randomValue < 0.8:
			strSlice := make([]string, 0)
			for j := 0; j < rand.IntN(5); j++ {
				strSlice = append(strSlice, generateString(4))
			}
			values = append(values, strSlice) // Slice of strings
		default:
			values = append(values, randomValue > 0.65) // Boolean value
		}
	}

	jsonMap := make(map[string]interface{})
	for i := 0; i < numKeys; i++ {
		jsonMap[keys[i]] = values[i]
	}

	encodedJSON, _ := json.Marshal(jsonMap)
	return encodedJSON
}

// Simple mock request for database operations
type MockRequest struct {
	Method      string
	URI         []string
	Data        []byte
	User        string
	min         string
	max         string
	NoOverwrite bool
}

func (req MockRequest) GetType() string {
	return req.Method
}

func (req MockRequest) GetPath() []string {
	return req.URI
}

func (req MockRequest) GetContent() []byte {
	return req.Data
}

func (req MockRequest) GetStartKey() string {
	return req.min
}

func (req MockRequest) GetEndKey() string {
	return req.max
}

func (req MockRequest) GetNoOverwrite() bool {
	return req.NoOverwrite
}

func (req MockRequest) GetValidator() jsondata.Validator {
	compiler := jsonschema.NewCompiler()
	schema, err := compiler.Compile("./anyschema.json")
	if err != nil {
		return nil
	}
	return schema
}

func (req MockRequest) GetUsername() string {
	return req.User
}

// Test for adding a single document
func Test_AddSingleDocument(t *testing.T) {
	/*skiplist := skiplist.NewSkipList[string, Document](10, "", "\U0010FFFF")
	database := Database{Path: "/v1/data", Documents: skiplist, Name: "testDB"}

	jsonContent := createRandomJSON(50)
	uri := []string{"data", "testdoc"}

	insertReq := MockRequest{Method: "PUT", URI: uri, Data: jsonContent, User: "Alice"}
	database.Handle(insertReq)

	// Verify the document was added correctly
	getReq := MockRequest{Method: "GET", URI: uri, Data: nil, User: "Alice"}
	fetchedContent, _ := database.Handle(getReq)

	var expected map[string]interface{}
	json.Unmarshal(jsonContent, &expected)
	result := fetchedContent.(DocumentContent).Content

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Document not inserted properly: Expected %v, got %v", expected, result)
	}*/

	skiplist := skiplist.NewSkipList[string, Document](10, "", "\U0010FFFF")
	db := Database{Path: "/v1/hello", Documents: skiplist, Name: "hello"}

	new_content := createRandomJSON(100)

	path_list := make([]string, 0)
	path_list = append(path_list, "hello", "braddoc")

	request := MockRequest{Method: "PUT", URI: path_list, Data: new_content, User: "Brad"}
	db.Handle(request)

	request_get := MockRequest{Method: "GET", URI: path_list, Data: nil, User: "Brad"}
	content, _ := db.Handle(request_get)
	var unmarshaled_old_content map[string]interface{}
	json.Unmarshal(new_content, &unmarshaled_old_content)
	converted_content := content.(DocumentContent).Content

	if !reflect.DeepEqual(converted_content, unmarshaled_old_content) {
		t.Error("Document inserted incorreclty")
	}
}

// Test for adding multiple documents
func Test_AddMultipleDocuments(t *testing.T) {
	skiplist := skiplist.NewSkipList[string, Document](10, "", "\U0010FFFF")
	database := Database{Path: "/v1/data", Documents: skiplist, Name: "testDB"}

	var documents [][]byte
	for i := 0; i < 100; i++ {
		documents = append(documents, createRandomJSON(10))
		uri := []string{"data", generateString(6)}
		req := MockRequest{Method: "PUT", URI: uri, Data: documents[i], User: "Alice"}
		database.Handle(req)
	}

	// Validate that all documents were inserted
	fetchedDocs, _ := database.get("", "")
	fetchedContent := make([]map[string]interface{}, 0)
	for _, doc := range fetchedDocs {
		fetchedContent = append(fetchedContent, doc.Content)
	}

	expectedContent := make([]map[string]interface{}, 0)
	for _, doc := range documents {
		var temp map[string]interface{}
		json.Unmarshal(doc, &temp)
		expectedContent = append(expectedContent, temp)
	}

	for _, item := range expectedContent {
		if !hasElement(fetchedContent, item) {
			t.Error("Not all documents inserted correctly.")
		}
	}
}

// Test for removing multiple documents
func Test_RemoveMultipleDocuments(t *testing.T) {
	skiplist := skiplist.NewSkipList[string, Document](10, "", "\U0010FFFF")
	database := Database{Path: "/v1/data", Documents: skiplist, Name: "testDB"}

	var paths [][]string
	var documents [][]byte
	for i := 0; i < 50; i++ {
		documents = append(documents, createRandomJSON(15))
		uri := []string{"data", generateString(6)}
		paths = append(paths, uri)
		insertReq := MockRequest{Method: "PUT", URI: uri, Data: documents[i], User: "Bob"}
		database.Handle(insertReq)
	}

	// Now, remove all the documents
	for i := 0; i < 50; i++ {
		removeReq := MockRequest{Method: "DELETE", URI: paths[i], Data: nil, User: "Bob"}
		database.Handle(removeReq)
	}

	// Verify all documents are removed
	remainingDocs, _ := database.get("", "")
	if len(remainingDocs) > 0 {
		t.Error("Documents were not all deleted as expected.")
	}
}

// Test for concurrent addition of documents
func Test_ConcurrentAddDocuments(t *testing.T) {
	skiplist := skiplist.NewSkipList[string, Document](10, "", "\U0010FFFF")
	database := Database{Path: "/v1/data", Documents: skiplist, Name: "testDB"}

	var wg sync.WaitGroup
	wg.Add(1000)

	var documents [][]byte
	var requests []MockRequest

	for i := 0; i < 1000; i++ {
		documents = append(documents, createRandomJSON(10))
		uri := []string{"data", generateString(7)}
		req := MockRequest{Method: "PUT", URI: uri, Data: documents[i], User: "Charlie"}
		requests = append(requests, req)
	}

	for i := 0; i < 1000; i++ {
		go func(req MockRequest) {
			database.Handle(req)
			wg.Done()
		}(requests[i])
	}
	wg.Wait()

	// Validate all documents were inserted concurrently
	fetchedDocs, _ := database.get("", "")
	fetchedContent := make([]map[string]interface{}, 0)
	for _, doc := range fetchedDocs {
		fetchedContent = append(fetchedContent, doc.Content)
	}

	expectedContent := make([]map[string]interface{}, 0)
	for _, doc := range documents {
		var temp map[string]interface{}
		json.Unmarshal(doc, &temp)
		expectedContent = append(expectedContent, temp)
	}

	for _, item := range expectedContent {
		if !hasElement(fetchedContent, item) {
			t.Error("Some documents were not inserted concurrently as expected.")
		}
	}
}

// Test for updating an existing document
func Test_UpdateDocument(t *testing.T) {
	skiplist := skiplist.NewSkipList[string, Document](10, "", "\U0010FFFF")
	database := Database{Path: "/v1/data", Documents: skiplist, Name: "testDB"}

	// Add initial document
	initialContent := createRandomJSON(10)
	uri := []string{"data", "docToUpdate"}
	insertReq := MockRequest{Method: "PUT", URI: uri, Data: initialContent, User: "Alice"}
	database.Handle(insertReq)

	// Update the document with new content
	newContent := createRandomJSON(10)
	updateReq := MockRequest{Method: "PUT", URI: uri, Data: newContent, User: "Alice", NoOverwrite: false}
	database.Handle(updateReq)

	// Fetch and validate the updated content
	getReq := MockRequest{Method: "GET", URI: uri, Data: nil, User: "Alice"}
	fetchedContent, _ := database.Handle(getReq)

	var expected map[string]interface{}
	json.Unmarshal(newContent, &expected)
	var old map[string]interface{}
	json.Unmarshal(initialContent, &old)
	result := fetchedContent.(DocumentContent).Content

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Document was not updated correctly: Expected %v, old %v, got %v", expected, old, result)
	}
}

// Test for concurrent deletion of documents
func Test_ConcurrentDeleteDocuments(t *testing.T) {
	skiplist := skiplist.NewSkipList[string, Document](10, "", "\U0010FFFF")
	database := Database{Path: "/v1/data", Documents: skiplist, Name: "testDB"}

	var wg sync.WaitGroup
	wg.Add(500)

	var documents [][]byte
	var paths [][]string
	var requests []MockRequest

	// Insert documents to delete later
	for i := 0; i < 500; i++ {
		documents = append(documents, createRandomJSON(5))
		uri := []string{"data", generateString(5)}
		paths = append(paths, uri)
		insertReq := MockRequest{Method: "PUT", URI: uri, Data: documents[i], User: "Charlie"}
		requests = append(requests, insertReq)
		database.Handle(insertReq)
	}

	// Concurrent deletion of documents
	for i := 0; i < 500; i++ {
		go func(req MockRequest, path []string) {
			deleteReq := MockRequest{Method: "DELETE", URI: path, Data: nil, User: "Charlie"}
			database.Handle(deleteReq)
			wg.Done()
		}(requests[i], paths[i])
	}
	wg.Wait()

	// Verify all documents are deleted
	fetchedDocs, _ := database.get("", "")
	if len(fetchedDocs) > 0 {
		t.Error("Not all documents were deleted concurrently.")
	}
}

// Test for inserting an empty document
func Test_InsertEmptyDocument(t *testing.T) {
	skiplist := skiplist.NewSkipList[string, Document](10, "", "\U0010FFFF")
	database := Database{Path: "/v1/data", Documents: skiplist, Name: "testDB"}

	uri := []string{"data", "emptyDoc"}

	// Insert an empty document
	emptyContent := []byte("{}")
	insertReq := MockRequest{Method: "PUT", URI: uri, Data: emptyContent, User: "Alice"}
	database.Handle(insertReq)

	// Fetch and validate the empty document
	getReq := MockRequest{Method: "GET", URI: uri, Data: nil, User: "Alice"}
	fetchedContent, _ := database.Handle(getReq)

	var expected map[string]interface{}
	json.Unmarshal(emptyContent, &expected)
	result := fetchedContent.(DocumentContent).Content

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Empty document was not handled correctly: Expected %v, got %v", expected, result)
	}
}

// Test for inserting documents with duplicate keys
func Test_InsertDuplicateKeys(t *testing.T) {
	skiplist := skiplist.NewSkipList[string, Document](10, "", "\U0010FFFF")
	database := Database{Path: "/v1/data", Documents: skiplist, Name: "testDB"}

	uri := []string{"data", "dupKeyDoc"}

	// Insert a document
	firstContent := createRandomJSON(5)
	insertReq := MockRequest{Method: "PUT", URI: uri, Data: firstContent, User: "Alice"}
	database.Handle(insertReq)

	// Insert a second document with the same key
	secondContent := createRandomJSON(5)
	duplicateReq := MockRequest{Method: "PUT", URI: uri, Data: secondContent, User: "Alice"}
	database.Handle(duplicateReq)

	// Fetch the document and check if the second document overwrote the first
	getReq := MockRequest{Method: "GET", URI: uri, Data: nil, User: "Alice"}
	fetchedContent, _ := database.Handle(getReq)

	var expected map[string]interface{}
	json.Unmarshal(secondContent, &expected)
	result := fetchedContent.(DocumentContent).Content

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Duplicate key handling failed: Expected %v, got %v", expected, result)
	}
}
