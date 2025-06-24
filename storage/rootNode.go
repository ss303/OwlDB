package storage

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/RICE-COMP318-FALL24/owldb-p1group35/skiplist"
)

// RootNode represents the root of the storage system, containing multiple databases.
type RootNode struct {
	Databases *skiplist.SkipList[string, Database]
	mu        sync.RWMutex
}

// NewRoot creates and returns a new RootNode with an empty list of databases.
// Input: None
// Output: New RootNode (*RootNode), error if any
func NewRoot() (*RootNode, error) {
	databaselist := skiplist.NewSkipList[string, Database](10, "", "\U0010FFFF")
	root := RootNode{Databases: databaselist}
	return &root, nil
}

// GetPath returns the path of the root node.
// Input: None
// Output: Path as string
func (root *RootNode) GetPath() string {
	return "/v1"
}

// GetChild retrieves a database by its name from the root node.
// Input: Database name (string)
// Output: IChildNode, error if any
func (root *RootNode) GetChild(dbName string) (IChildNode, error) {
	// Search for database by name
	if collection, exists := root.Databases.Find(dbName); exists {
		slog.Info("Database found", "Database name", dbName)
		return collection, nil
	}
	// If not found, log and print error message
	slog.Warn("Database not found", "Database name", dbName)
	return nil, fmt.Errorf("Database '%s' not found", dbName)
}

// Handle processes an HTTP request for the root node.
// Input: RequestPack (req)
// Output: Content (any), Status (status)
func (root *RootNode) Handle(req RequestPack) (content any, stat status) {
	request := req.GetType()
	switch request {
	case "GET":
		return root.HandleGet(req)
	case "PUT":
		return root.HandlePut(req)
	case "DELETE":
		return nil, root.HandleDelete(req)
	default:
		slog.Warn("Invalid HTTP request method", "method", request)
		return nil, status{"Bad Request", fmt.Errorf("invalid HTTP request")}
	}
}

// HandleGet retrieves a database from the root node by name.
// Input: RequestPack (req)
// Output: Content (any), Status (status)
func (root *RootNode) HandleGet(req RequestPack) (content any, stat status) {
	childName := req.GetPath()[len(req.GetPath())-1]
	db, exists := root.Databases.Find(childName)
	if !exists {
		slog.Info("Database %s does not exist", "database", childName)
		return nil, status{"Does Not Exist", fmt.Errorf("Database does not exist " + childName + ": Not Found")}
	}
	slog.Info("Database found", "Database name", childName)
	response, err := db.get(req.GetStartKey(), req.GetEndKey())
	if err != nil {
		slog.Error("Internal error retrieving documents", "child_name", childName, "error", err)
		return nil, status{"Internal Error", fmt.Errorf("internal error retrieving documents")}
	}
	slog.Info("GET operation successful", "child_name", childName)
	return response, status{"Get", nil}
}

// HandleDelete removes a database from the root node by name.
// Input: RequestPack (req)
// Output: Status (status)
func (root *RootNode) HandleDelete(req RequestPack) (stat status) {
	childName := req.GetPath()[len(req.GetPath())-1]
	removed, _ := root.Databases.Delete(childName)

	if !removed {
		slog.Warn("DELETE operation failed: Database not found", "database_name", childName)
		return status{"Does Not Exist", fmt.Errorf("Database does not exist " + childName + ": Not Found")}
	} else {
		slog.Info("DELETE operation successful", "database_name", childName)
		return status{"Deleted", nil}
	}
}

// HandlePut creates a new database in the root node.
// Input: RequestPack (req)
// Output: Content (any), Status (status)
func (root *RootNode) HandlePut(req RequestPack) (content any, stat status) {
	root.mu.Lock()
	defer root.mu.Unlock()

	childName := req.GetPath()[len(req.GetPath())-1]
	path := "/v1/" + strings.Join(req.GetPath(), "/")
	newDatabase := Database{Documents: skiplist.NewSkipList[string, Document](10, "", "\U0010FFFF"), Path: path, Name: childName}
	putCheckNoOverwrite := DatabaseCheckNoOverwrite(&newDatabase)
	_, err := root.Databases.Upsert(childName, putCheckNoOverwrite)

	if err != nil {
		slog.Warn("Database already exists", "child_name", childName)
		return nil, status{"Bad Request", fmt.Errorf("Database already exists " + childName + ": already exists")}
	}

	response := PutResponse{
		Path: path,
	}

	slog.Info("PUT operation successful", "child_name", childName, "path", path)
	return response, status{"Created", nil}
}
