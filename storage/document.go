package storage

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/RICE-COMP318-FALL24/owldb-p1group35/jsondata"
	"github.com/RICE-COMP318-FALL24/owldb-p1group35/skiplist"
)

// Document represents a JSON document with associated metadata and collections.
type Document struct {
	Path        string
	Contents    []byte
	Collections *skiplist.SkipList[string, Collection]
	Metadata    *Metadata
}

// Metadata holds metadata information for a document.
type Metadata struct {
	CreatedBy      string `json:"createdBy"`
	CreatedAt      int64  `json:"createdAt"`
	LastModifiedBy string `json:"lastModifiedBy"`
	LastModifiedAt int64  `json:"lastModifiedAt"`
}

type DocumentContent struct {
	Path     string                 `json:"path"`
	Content  map[string]interface{} `json:"doc"`
	Metadata Metadata               `json:"meta"`
}

type Patch struct {
	Op    string             `json:"op"`
	Path  string             `json:"path"`
	Value jsondata.JSONValue `json:"value"`
}

// GetPath returns the path of the document.
// Input: None
// Output: Path as string
func (doc *Document) GetPath() string {
	return doc.Path
}

// get retrieves the document content and metadata.
// Input: None
// Output: DocumentContent, error if any
func (doc *Document) get() (DocumentContent, error) {
	// Create copy of the content and the metadata
	contentCopy := make([]byte, len(doc.Contents))
	copy(contentCopy, doc.Contents)

	var contentJson map[string]interface{}
	if err := json.Unmarshal(contentCopy, &contentJson); err != nil {
		slog.Error("Failed to unmarshal document content", "path", doc.Path, "error", err)
		return DocumentContent{}, err
	}

	metadataCopy := *doc.Metadata
	metadataResp := Metadata{
		CreatedBy:      metadataCopy.CreatedBy,
		CreatedAt:      metadataCopy.CreatedAt,
		LastModifiedBy: metadataCopy.LastModifiedBy,
		LastModifiedAt: metadataCopy.LastModifiedAt,
	}

	docJSON := DocumentContent{
		Path:     doc.Path,
		Content:  contentJson,
		Metadata: metadataResp,
	}
	return docJSON, nil
}

// GetChild searches and retrieves a child collection by its name.
// Input: Collection name (string)
// Output: IChildNode, error if any
func (doc *Document) GetChild(colName string) (IChildNode, error) {
	// Search for collection by name
	if collection, exists := doc.Collections.Find(colName); exists {
		slog.Info("Collection found", "collection name", colName)
		return collection, nil
	}
	// If not found, log and print error message
	slog.Warn("Collection not found", "collection name", colName)
	return nil, fmt.Errorf("Collection '%s' not found", colName)
}

// CopyDoc creates a copy of the document, including its metadata.
// Input: Document (*Document)
// Output: Copy of the document (*Document), error if any
func CopyDoc(doc *Document) (*Document, error) {
	// Create copy of the content and the metadata
	contentCopy := make([]byte, len(doc.Contents))
	copy(contentCopy, doc.Contents)

	collectionsMap := skiplist.NewSkipList[string, Collection](10, "", "\U0010FFFF")
	metadataCopy := *doc.Metadata

	docCopy := Document{Path: doc.Path, Contents: contentCopy, Metadata: &metadataCopy, Collections: collectionsMap}

	return &docCopy, nil
}

// NewDocument creates a new document, validates its contents, and returns the document.
// Input: Path (string), Content ([]byte), CreatedBy (string), Validator (jsondata.Validator)
// Output: New Document (*Document), error if any
func NewDocument(path string, content []byte, createdBy string, validator jsondata.Validator) (*Document, error) {
	// Validate the contents against the schema.
	slog.Debug("Inside NewDoc:", "body", content)
	var rawContent any
	json.Unmarshal(content, &rawContent)
	jsonVal, err := jsondata.NewJSONValue(rawContent)
	if err != nil {
		return nil, err
	}

	err = jsonVal.Validate(validator)
	if err != nil {
		return nil, err
	}
	// Set metadata.
	now := time.Now().UnixMilli()
	metadata := &Metadata{
		CreatedBy:      createdBy,
		CreatedAt:      now,
		LastModifiedBy: createdBy,
		LastModifiedAt: now,
	}

	// Create the document with path support.
	doc := &Document{
		Path:        path, // Store the path where the document is located.
		Contents:    content,
		Metadata:    metadata,
		Collections: skiplist.NewSkipList[string, Collection](10, "", "\U0010FFFF"),
	}

	return doc, nil
}

// Update updates the metadata of a document.
// Input: ModifiedBy (string)
// Output: None
func (metadata *Metadata) Update(modifiedBy string) {
	metadata.LastModifiedBy = modifiedBy
	metadata.LastModifiedAt = time.Now().UnixMilli()
}

// DocCheckNoOverwrite checks if a document exists, and if not, returns the new document to be inserted.
// Input: New document (*Document)
// Output: Update check function (UpdateCheck)
func DocCheckNoOverwrite(newDoc *Document) skiplist.UpdateCheck[string, Document] {
	check := func(key string, currValue *Document, exists bool) (*Document, error) {
		if exists {
			return nil, fmt.Errorf("document already exists")
		} else {
			return newDoc, nil
		}
	}
	return check
}

// DocCheckOverwrite checks if a document exists, and if it does, overwrites it.
// Input: New document (*Document)
// Output: Update check function (UpdateCheck)
func DocCheckOverwrite(newDoc *Document) skiplist.UpdateCheck[string, Document] {
	check := func(key string, currValue *Document, exists bool) (*Document, error) {
		if exists {
			currTime := time.Now()
			currValue.Metadata.LastModifiedAt = currTime.UnixMilli()
			currValue.Contents = newDoc.Contents
			currValue.Metadata.LastModifiedBy = newDoc.Metadata.CreatedBy
			return nil, nil
		} else {
			return newDoc, nil
		}
	}
	return check
}

// DocPatchCheck validates and applies patch operations to a document.
// Input: Content ([]byte), Validator (jsondata.Validator), Name (string)
// Output: Update check function (UpdateCheck)
func DocPatchCheck(content []byte, validator jsondata.Validator, name string) skiplist.UpdateCheck[string, Document] {
	check := func(key string, currValue *Document, exists bool) (*Document, error) {
		if exists {
			err := currValue.PatchRequest(content, validator, name)
			if err != nil {
				return nil, err
			}
			return nil, nil
		} else {
			return nil, fmt.Errorf("object does not exist at this path")
		}
	}
	return check
}

// Handle processes an HTTP request for the document.
// Input: RequestPack (req)
// Output: Content (any), Status (status)
func (doc *Document) Handle(req RequestPack) (content any, stat status) {
	request := req.GetType()
	switch request {
	case "GET":
		return doc.HandleGet(req)
	case "PUT":
		return doc.HandlePut(req)
	case "DELETE":
		return doc.HandleDelete(req)
	default:
		slog.Warn("Invalid HTTP request method", "method", request)
		return nil, status{"Bad Request", fmt.Errorf("invalid HTTP request")}
	}
}

// HandleGet retrieves a collection from the document by name.
// Input: RequestPack (req)
// Output: Content (any), Status (status)
func (doc *Document) HandleGet(req RequestPack) (content any, stat status) {
	childName := req.GetPath()[len(req.GetPath())-1]
	col, exists := doc.Collections.Find(childName)
	if !exists {
		slog.Info("Collection %s does not exist", "collection_name", childName)
		return nil, status{"Does Not Exist", fmt.Errorf("Collection does not exist " + childName + ": not found")}
	}
	slog.Info("Collection found", "collection name", childName)
	response, err := col.get(req.GetStartKey(), req.GetEndKey())
	if err != nil {
		slog.Error("Internal error retrieving documents", "child_name", childName, "error", err)
		return nil, status{"Internal Error", fmt.Errorf("internal error retrieving documents")}
	}
	return response, status{"Get", nil}
}

// HandleDelete removes a collection from the document.
// Input: RequestPack (req)
// Output: Content (any), Status (status)
func (doc *Document) HandleDelete(req RequestPack) (content any, stat status) {
	childName := req.GetPath()[len(req.GetPath())-1]
	slog.Info("Attempting to delete collection", "childname", childName)
	_, err := doc.Collections.Delete(childName)

	if err != nil {
		slog.Warn("DELETE operation failed: collection not found", "collection_name", childName)
		return nil, status{"Does Not Exist", fmt.Errorf("Collection does not exist " + childName + ": not found")}
	}
	slog.Info("DELETE operation successful", "collection_name", childName)
	return nil, status{"Deleted", nil}
}

// HandlePut creates a new collection in the document.
// Input: RequestPack (req)
// Output: Content (any), Status (status)
func (doc *Document) HandlePut(req RequestPack) (content any, stat status) {
	childName := req.GetPath()[len(req.GetPath())-1]
	path := "/v1/" + strings.Join(req.GetPath(), "/")
	newCollection := Collection{Documents: skiplist.NewSkipList[string, Document](10, "", "\U0010FFFF"), Path: path, Name: childName}
	putCheckNoOverwrite := CollectionCheckNoOverwrite(&newCollection)
	_, err := doc.Collections.Upsert(childName, putCheckNoOverwrite)

	if err != nil {
		slog.Warn("Collection already exists", "child_name", childName)
		return nil, status{"Bad Request", fmt.Errorf("Collection already exists " + childName + ": exists")}
	}

	response := PutResponse{
		Path: path,
	}

	slog.Info("PUT operation successful", "child_name", childName, "path", path)
	return response, status{"Created", nil}
}

// PatchRequest applies a set of patch operations to the document's content and updates its metadata.
// Input: New content ([]byte), JSON validator (jsonValidator), Author name (string)
// Output: Error if any
func (doc *Document) PatchRequest(newContent []byte, jsonValidator jsondata.Validator, authorName string) error {
	// Step 1: Read Phase (with RLock)
	docContentCopy := make([]byte, len(doc.Contents))
	copy(docContentCopy, doc.Contents)

	// Unmarshal the content into a JSONValue
	var parsedJSONValue jsondata.JSONValue
	unmarshalErr := json.Unmarshal(docContentCopy, &parsedJSONValue)
	if unmarshalErr != nil {
		return fmt.Errorf("failed to unmarshal document content")
	}

	// Unmarshal the patch content into []PatchOperation
	var patchOperations []Patch
	unmarshalErr = json.Unmarshal(newContent, &patchOperations)
	if unmarshalErr != nil {
		return fmt.Errorf("failed to parse patch operations")
	}

	// Apply the patches sequentially
	for _, patchOp := range patchOperations {
		parsedJSONValue, unmarshalErr = applyPatch(parsedJSONValue, patchOp)
		if unmarshalErr != nil {
			return unmarshalErr
		}
	}

	// Validate the modified document
	validationErr := parsedJSONValue.Validate(jsonValidator)
	if validationErr != nil {
		return fmt.Errorf("validation failed after applying patches: %v", validationErr)
	}

	// Marshal the modified JSONValue back to []byte
	modifiedJSONContent, marshalErr := json.Marshal(parsedJSONValue)
	if marshalErr != nil {
		return fmt.Errorf("failed to marshal modified document")
	}

	// Step 2: Write Phase (with Lock)

	// Update the document's content, version, and metadata
	doc.Metadata.Update(authorName)
	doc.Contents = modifiedJSONContent

	// Patch applied successfully
	return nil
}

// Helper functions for PATCH

// applyPatch applies a specific patch operation to a JSONValue.
// Input: JSON document (jsonDoc), Patch operation (patch)
// Output: Modified JSONValue, error if any
func applyPatch(jsonDoc jsondata.JSONValue, patch Patch) (jsondata.JSONValue, error) {
	// Split the JSON pointer into tokens
	pathSegments, parseErr := parseJSONPointer(patch.Path)
	if parseErr != nil {
		return jsondata.JSONValue{}, fmt.Errorf("invalid JSON pointer: %s", patch.Path)
	}

	switch patch.Op {
	case "ArrayAdd":
		return applyArrayAdd(jsonDoc, pathSegments, patch.Value)
	case "ArrayRemove":
		return applyArrayRemove(jsonDoc, pathSegments, patch.Value)
	case "ObjectAdd":
		return applyObjectAdd(jsonDoc, pathSegments, patch.Value)
	default:
		return jsondata.JSONValue{}, fmt.Errorf("invalid operation: %s", patch.Op)
	}
}

// parseJSONPointer splits the JSON pointer into tokens.
// Input: Pointer string (pointer)
// Output: Array of path segments ([]string), error if any
func parseJSONPointer(pointer string) ([]string, error) {
	if pointer == "" {
		return []string{}, nil
	}
	if pointer[0] != '/' {
		return nil, fmt.Errorf("invalid JSON pointer: %s", pointer)
	}
	segments := strings.Split(pointer[1:], "/")
	for i, segment := range segments {
		segment = strings.ReplaceAll(segment, "~1", "/")
		segment = strings.ReplaceAll(segment, "~0", "~")
		segments[i] = segment
	}
	return segments, nil
}

// applyArrayAdd adds a value to an array if it's not already present.
// Input: JSON document (jsonDoc), Path segments ([]string), New value (newValue)
// Output: Modified JSONValue, error if any
func applyArrayAdd(jsonDoc jsondata.JSONValue, pathSegments []string, newValue jsondata.JSONValue) (jsondata.JSONValue, error) {
	if len(pathSegments) == 0 {
		return jsondata.JSONValue{}, fmt.Errorf("path refers to the whole document, which must be an array")
	}
	return modifyJSON(jsonDoc, pathSegments, func(currentValue jsondata.JSONValue) (jsondata.JSONValue, error) {
		arrayModifier := &arrayModifyVisitor{
			modifyFunc: func(array []jsondata.JSONValue) ([]jsondata.JSONValue, error) {
				// Check if the value is already in the array
				for _, element := range array {
					if element.Equal(newValue) {
						// Value already exists; do nothing
						return array, nil
					}
				}
				// Add the value to the array
				return append(array, newValue), nil
			},
		}
		return jsondata.Accept(currentValue, arrayModifier)
	})
}

// applyArrayRemove removes a value from an array if present.
// Input: JSON document (jsonDoc), Path segments ([]string), Value to remove (valueToRemove)
// Output: Modified JSONValue, error if any
func applyArrayRemove(jsonDoc jsondata.JSONValue, pathSegments []string, valueToRemove jsondata.JSONValue) (jsondata.JSONValue, error) {
	if len(pathSegments) == 0 {
		return jsondata.JSONValue{}, fmt.Errorf("path refers to the whole document, which must be an array")
	}
	return modifyJSON(jsonDoc, pathSegments, func(currentValue jsondata.JSONValue) (jsondata.JSONValue, error) {
		arrayModifier := &arrayModifyVisitor{
			modifyFunc: func(array []jsondata.JSONValue) ([]jsondata.JSONValue, error) {
				// Remove the value from the array
				updatedArray := []jsondata.JSONValue{}
				for _, element := range array {
					if !element.Equal(valueToRemove) {
						updatedArray = append(updatedArray, element)
					}
				}
				return updatedArray, nil
			},
		}
		return jsondata.Accept(currentValue, arrayModifier)
	})
}

// applyObjectAdd adds a property to an object if it doesn't already exist.
// Input: JSON document (jsonDoc), Path segments ([]string), New value (newValue)
// Output: Modified JSONValue, error if any
func applyObjectAdd(jsonDoc jsondata.JSONValue, pathSegments []string, newValue jsondata.JSONValue) (jsondata.JSONValue, error) {
	if len(pathSegments) == 0 {
		return jsondata.JSONValue{}, fmt.Errorf("path refers to the whole document, which must be an object")
	}
	propertyKey := pathSegments[len(pathSegments)-1]
	parentSegments := pathSegments[:len(pathSegments)-1]
	return modifyJSON(jsonDoc, parentSegments, func(currentValue jsondata.JSONValue) (jsondata.JSONValue, error) {
		objectModifier := &objectModifyVisitor{
			modifyFunc: func(object map[string]jsondata.JSONValue) (map[string]jsondata.JSONValue, error) {
				if _, exists := object[propertyKey]; exists {
					// Property already exists; do nothing
					return object, nil
				}

				// Add the property
				newObject := make(map[string]jsondata.JSONValue)
				for key, val := range object {
					newObject[key] = val
				}
				newObject[propertyKey] = newValue
				return newObject, nil
			},
		}
		return jsondata.Accept(currentValue, objectModifier)
	})
}

// modifyJSON recursively traverses the JSONValue and applies the modifyFunc at the target path.
// Input: JSON document (jsonDoc), Path segments ([]string), Modify function (modifyFunc)
// Output: Modified JSONValue, error if any
func modifyJSON(jsonDoc jsondata.JSONValue, pathSegments []string, modifyFunc func(jsondata.JSONValue) (jsondata.JSONValue, error)) (jsondata.JSONValue, error) {
	if len(pathSegments) == 0 {
		// We've reached the target node
		return modifyFunc(jsonDoc)
	}
	currentSegment := pathSegments[0]
	remainingSegments := pathSegments[1:]

	// Use a visitor to handle the current node
	navigator := &navigatorModifyVisitor{
		key:           currentSegment,
		remainingPath: remainingSegments,
		modifyFunc:    modifyFunc,
	}
	return jsondata.Accept(jsonDoc, navigator)
}

// arrayModifyVisitor modifies an array JSONValue
// Input: Modification function (modifyFunc)
// Output: Modified JSONValue, error if any

type arrayModifyVisitor struct {
	modifyFunc func([]jsondata.JSONValue) ([]jsondata.JSONValue, error)
}

func (v *arrayModifyVisitor) Map(object map[string]jsondata.JSONValue) (jsondata.JSONValue, error) {
	return jsondata.JSONValue{}, fmt.Errorf("expected array but found object")
}

func (v *arrayModifyVisitor) Slice(array []jsondata.JSONValue) (jsondata.JSONValue, error) {
	modifiedArray, err := v.modifyFunc(array)
	if err != nil {
		return jsondata.JSONValue{}, err
	}
	return jsondata.NewJSONValue(modifiedArray)
}

func (v *arrayModifyVisitor) Bool(b bool) (jsondata.JSONValue, error) {
	return jsondata.JSONValue{}, fmt.Errorf("expected array but found bool")
}

func (v *arrayModifyVisitor) Float64(f float64) (jsondata.JSONValue, error) {
	return jsondata.JSONValue{}, fmt.Errorf("expected array but found number")
}

func (v *arrayModifyVisitor) String(s string) (jsondata.JSONValue, error) {
	return jsondata.JSONValue{}, fmt.Errorf("expected array but found string")
}

func (v *arrayModifyVisitor) Null() (jsondata.JSONValue, error) {
	return jsondata.JSONValue{}, fmt.Errorf("expected array but found null")
}

// objectModifyVisitor modifies an object JSONValue
// Input: Modification function (modifyFunc)
// Output: Modified JSONValue, error if any

type objectModifyVisitor struct {
	modifyFunc func(map[string]jsondata.JSONValue) (map[string]jsondata.JSONValue, error)
}

func (v *objectModifyVisitor) Map(object map[string]jsondata.JSONValue) (jsondata.JSONValue, error) {
	modifiedObject, err := v.modifyFunc(object)
	if err != nil {
		return jsondata.JSONValue{}, err
	}
	return jsondata.NewJSONValue(modifiedObject)
}

func (v *objectModifyVisitor) Slice(array []jsondata.JSONValue) (jsondata.JSONValue, error) {
	return jsondata.JSONValue{}, fmt.Errorf("expected object but found array")
}

func (v *objectModifyVisitor) Bool(b bool) (jsondata.JSONValue, error) {
	return jsondata.JSONValue{}, fmt.Errorf("expected object but found bool")
}

func (v *objectModifyVisitor) Float64(f float64) (jsondata.JSONValue, error) {
	return jsondata.JSONValue{}, fmt.Errorf("expected object but found number")
}

func (v *objectModifyVisitor) String(s string) (jsondata.JSONValue, error) {
	return jsondata.JSONValue{}, fmt.Errorf("expected object but found string")
}

func (v *objectModifyVisitor) Null() (jsondata.JSONValue, error) {
	return jsondata.JSONValue{}, fmt.Errorf("expected object but found null")
}

// navigatorModifyVisitor navigates through the JSONValue and applies modifications
// Input: Key (string), Remaining path segments ([]string), Modification function (modifyFunc)
// Output: Modified JSONValue, error if any

type navigatorModifyVisitor struct {
	key           string
	remainingPath []string
	modifyFunc    func(jsondata.JSONValue) (jsondata.JSONValue, error)
}

func (v *navigatorModifyVisitor) Map(object map[string]jsondata.JSONValue) (jsondata.JSONValue, error) {
	child, exists := object[v.key]
	if !exists {
		return jsondata.JSONValue{}, fmt.Errorf("key '%s' not found in object", v.key)
	}

	// Recursively modify the child
	modifiedChild, err := modifyJSON(child, v.remainingPath, v.modifyFunc)
	if err != nil {
		return jsondata.JSONValue{}, err
	}

	// Create a new map with the modified child
	updatedMap := make(map[string]jsondata.JSONValue)
	for key, value := range object {
		if key == v.key {
			updatedMap[key] = modifiedChild
		} else {
			updatedMap[key] = value
		}
	}
	return jsondata.NewJSONValue(updatedMap)
}

func (v *navigatorModifyVisitor) Slice(array []jsondata.JSONValue) (jsondata.JSONValue, error) {
	return jsondata.JSONValue{}, fmt.Errorf("unexpected array while navigating")
}

func (v *navigatorModifyVisitor) Bool(b bool) (jsondata.JSONValue, error) {
	return jsondata.JSONValue{}, fmt.Errorf("unexpected bool while navigating")
}

func (v *navigatorModifyVisitor) Float64(f float64) (jsondata.JSONValue, error) {
	return jsondata.JSONValue{}, fmt.Errorf("unexpected number while navigating")
}

func (v *navigatorModifyVisitor) String(s string) (jsondata.JSONValue, error) {
	return jsondata.JSONValue{}, fmt.Errorf("unexpected string while navigating")
}

func (v *navigatorModifyVisitor) Null() (jsondata.JSONValue, error) {
	return jsondata.JSONValue{}, fmt.Errorf("unexpected null while navigating")
}
