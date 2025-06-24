package storage

import (
	"fmt"
	"log/slog"

	"github.com/RICE-COMP318-FALL24/owldb-p1group35/jsondata"
)

// Storage represents the main structure containing the root node of the storage system.
type Storage struct {
	root *RootNode
}

// status holds information about the status of an operation, including class and error.
type status struct {
	status_class string
	err          error
}

// GetClass returns the status class of the status.
// Input: None
// Output: Status class as string
func (status status) GetClass() string {
	return status.status_class
}

// GetError returns the error associated with the status.
// Input: None
// Output: Error (error)
func (status status) GetError() error {
	return status.err
}

// RequestPack defines the interface for handling different types of requests.
type RequestPack interface {
	GetType() string
	GetPath() []string
	GetContent() []byte
	GetValidator() jsondata.Validator
	GetUsername() string
	GetStartKey() string
	GetEndKey() string
	GetNoOverwrite() bool
}

// PutResponse represents the response for a PUT operation.
type PutResponse struct {
	Path string `json:"uri"`
}

// PatchResponse represents the response for a PATCH operation.
type PatchResponse struct {
	Uri         string `json:"uri"`
	PatchFailed bool   `json:"patch_failed"`
	Message     string `json:"message"`
}

// NewStorageTree creates and returns a new storage tree with an initialized root node.
// Input: None
// Output: New Storage (*Storage)
func NewStorageTree() *Storage {
	root, _ := NewRoot()
	strTree := Storage{root: root}
	return &strTree
}

// GetParent retrieves the parent node based on the given path.
// Input: Path ([]string)
// Output: IChildNode, error if any
func (tree *Storage) GetParent(path []string) (IChildNode, error) {
	var currentObject IChildNode = tree.root

	for i, key := range path {
		if i == len(path)-1 {
			break
		}
		var err error

		currentObject, err = currentObject.GetChild(key)

		if err != nil {
			slog.Error("Error getting child in path", "key", key, "error", err)
			return nil, fmt.Errorf("containing collection/document does not exist")
		}
	}
	slog.Info("Parent object retrieved successfully", "path", path)
	// Return the object at the last position in the path
	return currentObject, nil
}

// HandleOperation processes an operation request and returns the result.
// Input: RequestPack (op_info)
// Output: Content (any), Status (status)
func (tree *Storage) HandleOperation(opInfo RequestPack) (content any, statInfo status) {
	path := opInfo.GetPath()

	parent, err := tree.GetParent(path)

	if err != nil {
		slog.Warn("Failed to get parent object", "path", path, "error", err)
		statInfo := status{status_class: "Does Not Exist", err: err}
		return nil, statInfo
	}

	// If the request type is POST, ensure it is handled correctly
	if opInfo.GetType() == "POST" {
		// Identify the target child for the POST operation
		childName := path[len(path)-1]
		child, err := parent.GetChild(childName)
		if err != nil {
			slog.Warn("Failed to get target child for POST operation", "child_name", childName, "error", err)
			statInfo := status{status_class: "Does Not Exist", err: err}
			return nil, statInfo
		}

		// Perform POST operation on the child
		info, status := child.Handle(opInfo)
		return info, status
	}

	// For all other request types, operate on the parent directly
	info, status := parent.Handle(opInfo)
	return info, status
}
