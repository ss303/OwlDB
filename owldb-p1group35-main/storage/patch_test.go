package storage

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/RICE-COMP318-FALL24/owldb-p1group35/jsondata"
)

// Helper function for unmarshalling JSON
func parseJSON(t *testing.T, jsonStr string) jsondata.JSONValue {
	var jsonValue jsondata.JSONValue
	err := json.Unmarshal([]byte(jsonStr), &jsonValue)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}
	return jsonValue
}

// Helper function to compare two JSON values
func verifyJSONEquality(t *testing.T, actual, expected jsondata.JSONValue) {
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("JSON mismatch.\nExpected: %v\nGot: %v", expected, actual)
	}
}

// Helper function to create JSON patch operations
func createPatchOp(op, path string, value interface{}) Patch {
	valueJSONValue, _ := jsondata.NewJSONValue(value)
	return Patch{
		Op:    op,
		Path:  path,
		Value: valueJSONValue,
	}
}

func TestPatch_AddToArray(t *testing.T) {
	docJSON := `{"numbers": [1, 2, 3]}`
	expectedJSON := `{"numbers": [1, 2, 3, 4]}`

	docJSONValue := parseJSON(t, docJSON)
	expectedJSONValue := parseJSON(t, expectedJSON)

	patchOp := createPatchOp("ArrayAdd", "/numbers", float64(4))

	updatedDoc, err := applyPatch(docJSONValue, patchOp)
	if err != nil {
		t.Fatalf("applyPatch failed: %v", err)
	}

	verifyJSONEquality(t, updatedDoc, expectedJSONValue)
}

func TestPatch_RemoveFromArray(t *testing.T) {
	docJSON := `{"numbers": [1, 2, 3, 4]}`
	expectedJSON := `{"numbers": [1, 3, 4]}`

	docJSONValue := parseJSON(t, docJSON)
	expectedJSONValue := parseJSON(t, expectedJSON)

	patchOp := createPatchOp("ArrayRemove", "/numbers", float64(2))

	updatedDoc, err := applyPatch(docJSONValue, patchOp)
	if err != nil {
		t.Fatalf("applyPatch failed: %v", err)
	}

	verifyJSONEquality(t, updatedDoc, expectedJSONValue)
}

func TestPatch_AddToObject(t *testing.T) {
	docJSON := `{"user": {"name": "John"}}`
	expectedJSON := `{"user": {"name": "John", "age": 30}}`

	docJSONValue := parseJSON(t, docJSON)
	expectedJSONValue := parseJSON(t, expectedJSON)

	patchOp := createPatchOp("ObjectAdd", "/user/age", float64(30))

	updatedDoc, err := applyPatch(docJSONValue, patchOp)
	if err != nil {
		t.Fatalf("applyPatch failed: %v", err)
	}

	verifyJSONEquality(t, updatedDoc, expectedJSONValue)
}

func TestPatch_InvalidOperation(t *testing.T) {
	docJSON := `{"numbers": [1, 2, 3]}`
	docJSONValue := parseJSON(t, docJSON)

	patchOp := createPatchOp("InvalidOp", "/numbers", float64(4))

	_, err := applyPatch(docJSONValue, patchOp)
	if err == nil {
		t.Fatalf("Expected error for invalid operation, but got none")
	}
}

func TestPatch_InvalidPath(t *testing.T) {
	docJSON := `{"numbers": [1, 2, 3]}`
	docJSONValue := parseJSON(t, docJSON)

	patchOp := createPatchOp("ArrayAdd", "numbers", float64(4)) // Invalid path

	_, err := applyPatch(docJSONValue, patchOp)
	if err == nil {
		t.Fatalf("Expected error for invalid path, but got none")
	}
}

func TestPatch_AddToNonArray(t *testing.T) {
	docJSON := `{"numbers": "not an array"}`
	docJSONValue := parseJSON(t, docJSON)

	patchOp := createPatchOp("ArrayAdd", "/numbers", float64(4))

	_, err := applyPatch(docJSONValue, patchOp)
	if err == nil {
		t.Fatalf("Expected error when applying ArrayAdd to non-array, but got none")
	}
}

func TestPatch_AddToNonObject(t *testing.T) {
	docJSON := `{"user": "not an object"}`
	docJSONValue := parseJSON(t, docJSON)

	patchOp := createPatchOp("ObjectAdd", "/user/name", "John")

	_, err := applyPatch(docJSONValue, patchOp)
	if err == nil {
		t.Fatalf("Expected error when applying ObjectAdd to non-object, but got none")
	}
}

func TestPatch_AddExistingObjectProperty(t *testing.T) {
	docJSON := `{"user": {"name": "John"}}`
	expectedJSON := `{"user": {"name": "John"}}`

	docJSONValue := parseJSON(t, docJSON)
	expectedJSONValue := parseJSON(t, expectedJSON)

	patchOp := createPatchOp("ObjectAdd", "/user/name", "Doe") // Property already exists

	updatedDoc, err := applyPatch(docJSONValue, patchOp)
	if err != nil {
		t.Fatalf("applyPatch failed: %v", err)
	}

	verifyJSONEquality(t, updatedDoc, expectedJSONValue) // Should remain unchanged
}

func TestPatch_AddExistingArrayValue(t *testing.T) {
	docJSON := `{"numbers": [1, 2, 3]}`
	expectedJSON := `{"numbers": [1, 2, 3]}`

	docJSONValue := parseJSON(t, docJSON)
	expectedJSONValue := parseJSON(t, expectedJSON)

	patchOp := createPatchOp("ArrayAdd", "/numbers", float64(2)) // Value already exists

	updatedDoc, err := applyPatch(docJSONValue, patchOp)
	if err != nil {
		t.Fatalf("applyPatch failed: %v", err)
	}

	verifyJSONEquality(t, updatedDoc, expectedJSONValue) // Should remain unchanged
}

func TestPatch_RemoveNonExistingArrayValue(t *testing.T) {
	docJSON := `{"numbers": [1, 2, 3]}`
	expectedJSON := `{"numbers": [1, 2, 3]}`

	docJSONValue := parseJSON(t, docJSON)
	expectedJSONValue := parseJSON(t, expectedJSON)

	patchOp := createPatchOp("ArrayRemove", "/numbers", float64(4)) // Value does not exist

	updatedDoc, err := applyPatch(docJSONValue, patchOp)
	if err != nil {
		t.Fatalf("applyPatch failed: %v", err)
	}

	verifyJSONEquality(t, updatedDoc, expectedJSONValue) // Should remain unchanged
}

func TestPatch_AddToRootArray(t *testing.T) {
	docJSON := `[1, 2, 3]`
	docJSONValue := parseJSON(t, docJSON)

	patchOp := createPatchOp("ArrayAdd", "", float64(4)) // Root path

	_, err := applyPatch(docJSONValue, patchOp)
	if err == nil {
		t.Fatalf("Expected error when applying ArrayAdd to root path, but got none")
	}
}
