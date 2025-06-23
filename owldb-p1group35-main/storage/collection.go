package storage

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/RICE-COMP318-FALL24/owldb-p1group35/skiplist"
)

// Document represents a JSON document with associated metadata and collections.
type Collection struct {
	Path      string
	Name      string
	Documents *skiplist.SkipList[string, Document]
}

// GetPath returns the path of the collection.
// Input: None
// Output: Path as string
func (c *Collection) GetPath() string {
	return c.Path
}

// GetName returns the name of the collection.
// Input: None
// Output: Name as string
func (c *Collection) GetName() string {
	return c.Name
}

// get retrieves a document by its name from the database.
// Input: Start key (string), End key (string)
// Output: Slice of DocumentContent, error if any
func (c *Collection) get(startKey string, endKey string) (content []DocumentContent, err error) {
	if endKey == "" {
		startKey = ""
		endKey = "\U0010FFFF"
	}
	docCopies, err := c.Documents.QueryCopies(startKey, endKey, CopyDoc)
	if err != nil {
		slog.Error("Failed to retrieve documents in ", "collection", c.GetName(), "error", err)
		return nil, err // Return error if content retrieval fails
	}

	contents := make([]DocumentContent, 0)
	// Iterate over all documents in the database
	for _, doc := range docCopies {
		// Get the content of each document
		content, err := doc.get() // Calls Document's Get_Content
		if err != nil {
			slog.Error("Failed to retrieve document content", "document", doc, "error", err)
			return nil, err // Return error if content retrieval fails
		}
		contents = append(contents, content)
	}

	slog.Info("Retrieved contents of all documents", "document_count", len(contents))
	return contents, nil
}

// GetChild searches and retrieves a child document by its name.
// Input: Document name (string)
// Output: IChildNode, error if any
func (col *Collection) GetChild(docName string) (IChildNode, error) {
	// Search for document by name
	if document, exists := col.Documents.Find(docName); exists {
		slog.Info("Document found", "document name", docName)
		return document, nil
	}
	// If not found, log and print error message
	slog.Warn("Document not found", "document name", docName)
	return nil, fmt.Errorf("Document '%s' not found", docName)
}

// CollectionCheckNoOverwrite checks if a collection exists, and if not, returns the new collection to be inserted.
// Input: New collection (*Collection)
// Output: Update check function (UpdateCheck)
func CollectionCheckNoOverwrite(newCol *Collection) skiplist.UpdateCheck[string, Collection] {
	check := func(key string, currValue *Collection, exists bool) (*Collection, error) {
		if exists {
			return nil, fmt.Errorf("database exists already")
		} else {
			return newCol, nil
		}
	}
	return check
}

// Handle processes an HTTP request for the collection.
// Input: RequestPack (req)
// Output: Content (any), Status (status)
func (c *Collection) Handle(req RequestPack) (content any, stat status) {
	request := req.GetType()
	switch request {
	case "GET":
		return c.HandleGet(req)
	case "PUT":
		return c.HandlePut(req)
	case "DELETE":
		return nil, c.HandleDelete(req)
	case "POST":
		return c.HandlePost(req)
	case "PATCH":
		return c.HandlePatch(req)
	default:
		slog.Warn("Invalid HTTP request method", "method", request)
		return nil, status{"Bad Request", fmt.Errorf("invalid HTTP request")}
	}
}

// HandleDelete removes a document from the collection.
// Input: RequestPack (req)
// Output: Status (status)
func (c *Collection) HandleDelete(req RequestPack) (stat status) {
	childName := req.GetPath()[len(req.GetPath())-1]
	removed, _ := c.Documents.Delete(childName)

	if !removed {
		slog.Warn("DELETE operation failed: document not found", "document_name", childName)
		return status{"Does Not Exist", fmt.Errorf("Document does not exist " + childName + " not found")}
	} else {
		slog.Info("DELETE operation successful", "document_name", childName)
		return status{"Deleted", nil}
	}
}

// HandleGet retrieves a document from the collection by name.
// Input: RequestPack (req)
// Output: Content (any), Status (status)
func (c *Collection) HandleGet(req RequestPack) (content any, stat status) {
	childName := req.GetPath()[len(req.GetPath())-1]
	childCopy, err := c.Documents.GetCopy(childName, CopyDoc)
	if err != nil {
		return nil, status{"Does Not Exist", err}
	}

	slog.Info("Document found", "document name", childName)
	response, err := childCopy.get()
	if err != nil {
		slog.Error("Internal error retrieving documents", "child_name", childName, "error", err)
		return nil, status{"Internal Error", fmt.Errorf("internal error retrieving documents")}
	}
	return response, status{"Get", nil}
}

// HandlePut inserts or updates a document in the collection.
// Input: RequestPack (req)
// Output: Content (any), Status (status)
func (c *Collection) HandlePut(req RequestPack) (content any, stat status) {
	childName := req.GetPath()[len(req.GetPath())-1]
	path := "/v1/" + strings.Join(req.GetPath(), "/")

	doc, err := NewDocument(path, req.GetContent(), req.GetUsername(), req.GetValidator())
	if err != nil {
		return nil, status{"Bad Request", err}
	}

	var putCheck skiplist.UpdateCheck[string, Document]
	if req.GetNoOverwrite() {
		putCheck = DocCheckNoOverwrite(doc)
	} else {
		putCheck = DocCheckOverwrite(doc)
	}

	var updated bool
	updated, err = c.Documents.Upsert(childName, putCheck)
	if err != nil {
		if updated {
			return nil, status{status_class: "Document not overwritten", err: err}
		} else {
			return nil, status{status_class: "Bad Request", err: err}
		}
	}

	response := PutResponse{
		Path: path,
	}

	var statusInfo status
	if updated {
		slog.Info("PUT operation successful: document overwritten", "document_name", childName, "path", path)
		statusInfo = status{"Overwritten", nil}
	} else {
		slog.Info("PUT operation successful: new document created", "document_name", childName, "path", path)
		statusInfo = status{"Created", nil}
	}

	return response, statusInfo
}

// generateRandomDocName generates a unique document name.
// Input: None
// Output: Generated document name (string)
func (c *Collection) generateRandomDocName() string {
	// Generate a random name using Unix timestamp or any other mechanism
	newDocName := fmt.Sprintf("doc_%d", time.Now().UnixNano())
	return newDocName
}

// HandlePost creates a new document with a generated unique name.
// Input: RequestPack (req)
// Output: Content (any), Status (status)
func (c *Collection) HandlePost(req RequestPack) (content any, stat status) {
	// Generate a unique document name (customize this function as needed)
	newDocName := c.generateRandomDocName()
	// Create the new document with the generated name
	path := "/v1/" + c.GetName() + "/" + newDocName
	doc, err := NewDocument(path, req.GetContent(), req.GetUsername(), req.GetValidator())
	if err != nil {
		slog.Error("POST operation failed: error creating new document", "error", err)
		return nil, status{"Bad Request", err}
	}
	putCheckNoOverwrite := DocCheckNoOverwrite(doc)

	_, err = c.Documents.Upsert(newDocName, putCheckNoOverwrite)

	// Keep trying to insert with new doc name until doc name is unique
	for err != nil {
		// Generate a unique document name (customize this function as needed)
		newDocName = c.generateRandomDocName()
		// Create the new document with the generated name
		path = "/v1/" + c.GetName() + "/" + newDocName
		doc, err = NewDocument(path, req.GetContent(), req.GetUsername(), req.GetValidator())
		if err != nil {
			slog.Error("POST operation failed: error creating new document", "error", err)
			return nil, status{"Bad Request", err}
		}
		putCheckNoOverwrite = DocCheckNoOverwrite(doc)

		_, err = c.Documents.Upsert(newDocName, putCheckNoOverwrite)

		if err != nil {
			return nil, status{"Bad Request", err}
		}
	}

	slog.Info("POST operation successful: new document created", "document_name", newDocName, "path", path)

	// Prepare response indicating the new document's path
	response := PutResponse{Path: path}
	return response, status{"Created", nil}
}

// HandlePatch applies a patch to an existing document.
// Input: RequestPack (req)
// Output: Content (any), Status (status)
func (c *Collection) HandlePatch(req RequestPack) (content any, stat status) {
	childName := req.GetPath()[len(req.GetPath())-1]
	patchCheck := DocPatchCheck(req.GetContent(), req.GetValidator(), req.GetUsername())

	_, err := c.Documents.Upsert(childName, patchCheck)

	response := PatchResponse{
		Uri: "/v1/" + strings.Join(req.GetPath(), "/"),
	}

	if err != nil {
		response.PatchFailed = true
		response.Message = err.Error()
	} else {
		response.PatchFailed = false
		response.Message = "patches applied"
	}

	return response, status{"Patched", nil}
}
