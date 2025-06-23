package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/RICE-COMP318-FALL24/owldb-p1group35/jsondata"

	"github.com/RICE-COMP318-FALL24/owldb-p1group35/storage"
	"github.com/RICE-COMP318-FALL24/owldb-p1group35/subscription"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

type StatusInfo interface {
	GetClass() string
	GetError() error
}

type httpRequest struct {
	request     string
	path        []string
	content     []byte
	validator   jsondata.Validator
	username    string
	minKey      string
	maxKey      string
	noOverwrite bool
}

// GetType returns the HTTP request type
// Input: None
// Output: Request type string
func (http_req httpRequest) GetType() string {
	return http_req.request
}

// GetPath returns the path segments of the request
// Input: None
// Output: Path segments as a slice of strings
func (http_req httpRequest) GetPath() []string {
	return http_req.path
}

// GetContent returns the content of the request
// Input: None
// Output: Request content as byte slice
func (http_req httpRequest) GetContent() []byte {
	return http_req.content
}

// GetValidator returns the JSON validator for the request
// Input: None
// Output: JSON validator
func (http_req httpRequest) GetValidator() jsondata.Validator {
	return http_req.validator
}

// GetUsername returns the username associated with the request
// Input: None
// Output: Username string
func (http_req httpRequest) GetUsername() string {
	return http_req.username
}

// GetStartKey returns the start key of the request interval
// Input: None
// Output: Start key string
func (http_req httpRequest) GetStartKey() string {
	return http_req.minKey
}

// GetEndKey returns the end key of the request interval
// Input: None
// Output: End key string
func (http_req httpRequest) GetEndKey() string {
	return http_req.maxKey
}

// GetNoOverwrite returns whether no-overwrite mode is enabled
// Input: None
// Output: Boolean indicating no-overwrite mode
func (http_req httpRequest) GetNoOverwrite() bool {
	return http_req.noOverwrite
}

type status interface {
	GetClass() string
	GetError() error
}

type IStorage interface {
	HandleOperation() any
	status
}

type owldb struct {
	storage      *storage.Storage
	validator    jsondata.Validator
	mu           sync.RWMutex
	tokenToUser  map[string]authEntry
	subscription *subscription.SubscriberHandler
}

// GetSupportedRequests returns a list of supported HTTP methods for the given storage type
// Input: Storage type string
// Output: Slice of supported request methods
func GetSupportedRequests(storage_type string) []string {
	slice := make([]string, 0)
	if storage_type == "Database" {
		slice = append(slice, "GET", "PUT", "POST", "DELETE")
	} else if storage_type == "Document" {
		slice = append(slice, "GET", "PUT", "DELETE", "PATCH")
	} else if storage_type == "Collection" {
		slice = append(slice, "GET", "PUT", "DELETE", "POST")
	}

	// Log the supported requests for the given storage type
	slog.Info("Supported requests determined", "storage_type", storage_type, "supported_requests", slice)
	return slice
}

// GetStorageType returns the type of storage based on the path length
// Input: Path length integer
// Output: Storage type string
func GetStorageType(path_length int) string {
	storageType := ""
	if path_length == 1 {
		storageType = "Database"
	} else if path_length%2 == 0 {
		storageType = "Document"
	} else {
		storageType = "Collection"
	}

	// Log the determined storage type
	slog.Info("Determined storage type", "path_length", path_length, "storage_type", storageType)
	return storageType
}

// GetStatusCode returns the HTTP status code corresponding to the status class
// Input: Status class string
// Output: HTTP status code and a boolean indicating success or failure
func GetStatusCode(status_class string) (code int, success bool) {
	switch status_class {
	case "Created":
		return 201, true
	case "Get":
		return 200, true
	case "Bad Request":
		return 400, false
	case "Overwritten":
		return 200, true
	case "Does Not Exist":
		return 404, false
	case "Internal Error":
		return 400, false
	case "Deleted":
		return 204, true
	case "Patched":
		return 200, true
	case "Document not overwritten":
		return 412, false
	default:
		// Log an unexpected status class
		slog.Warn("Unknown status class encountered", "status_class", status_class)
		return 400, false
	}
}

// New initializes a new owldb instance with storage, validator, and subscriber handler
// Input: Schema file path, token file path
// Output: Pointer to owldb instance or error
func New(schemaFile string, tokenFile string) (*owldb, error) {
	store := storage.NewStorageTree()
	subscribe := subscription.NewHandler()
	schema, err := jsonschema.Compile(schemaFile)

	if err != nil {
		return nil, fmt.Errorf("schema file not found")
	}

	jsonfile, err := os.Open(tokenFile)

	if err != nil {
		return nil, fmt.Errorf("token file not found")
	}
	jsonbytes, _ := io.ReadAll(jsonfile)
	slog.Info("jsonbytes", "jsonbytes", jsonbytes)

	var auth_map map[string]string
	json.Unmarshal(jsonbytes, &auth_map)
	slog.Info("auth_map", "auth_map", auth_map)

	token_user_map := make(map[string]string, len(auth_map))
	for key, value := range auth_map {
		token_user_map[value] = key
	}
	slog.Info("token_to_user", "token_to_user", token_user_map)

	token_to_tokeninfo := make(map[string]authEntry, len(token_user_map))
	expiration_time := time.Now().Add(1 * time.Hour)
	for token, user := range token_user_map {
		new_info := authEntry{username: user, expiration: expiration_time}
		token_to_tokeninfo[token] = new_info
	}

	service := owldb{storage: store, validator: schema, tokenToUser: token_to_tokeninfo, subscription: subscribe}
	return &service, nil
}

// RequestValid validates the given HTTP request based on storage type and other parameters
// Input: Method string, storage type string, slash ending boolean, interval boolean, overwrite boolean
// Output: Boolean indicating if the request is valid, and error if not valid
func RequestValid(method string, storage_type string, slashEnd bool, interval bool, overwrite bool) (bool, error) {

	slog.Info("in requestvalid", "storage_type", storage_type, "slashEnd", slashEnd, "interval", interval, "overwrite", overwrite)
	// Validate that the request method is supported for the given storage type
	if !slices.Contains(GetSupportedRequests(storage_type), method) {
		return false, fmt.Errorf("invalid request type")
	}
	// Validate path requirements for Document storage type
	if storage_type == "Document" && (slashEnd || interval) {
		return false, fmt.Errorf("bad request path")
	}
	// Validate path requirements for Collection storage type
	if storage_type == "Collection" && (!slashEnd || overwrite) {
		return false, fmt.Errorf("bad request path")
	}
	// Validate path requirements for Database storage type
	if storage_type == "Database" && ((slashEnd && (method == "PUT" || method == "DELETE")) || overwrite) {
		return false, fmt.Errorf("bad request path")
	}
	if storage_type == "Database" && ((!slashEnd && (method == "GET" || method == "POST")) || overwrite) {
		return false, fmt.Errorf("bad request path")
	}
	return true, nil
}

// getInterval parses an interval string and returns it as a slice of strings
// Input: Interval string
// Output: Slice of strings representing the interval values
func getInterval(interval string) []string {
	// Trim brackets from the interval and split by comma
	interval = strings.Trim(interval, "[]")
	return strings.Split(interval, ",")
}

type PathStruct struct {
	URI string `json:"uri"`
}

// A generic way to extract the "Path" field from a variable of type 'any'.
// Input: JSON []byte
// Output: String and error
func extractPath(value []byte) (string, error) {
	// Use a map to extract the "path" field from the JSON bytes
	var pathObj PathStruct
	err := json.Unmarshal(value, &pathObj)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal into map: %v", err)
	}

	// Check if the "path" key exists and is a string
	path := pathObj.URI
	slog.Info("in extractPath", "path", path)
	return path[strings.LastIndex(path, "/")+1:], nil

}

// HandleStorage processes storage-related HTTP requests
// Input: HTTP response writer and request
// Output: None
func (owldb *owldb) HandleStorage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
	w.Header().Set("Content-Type", "application/json")

	requestPath := r.URL.Path
	requestBody, err := io.ReadAll(r.Body)

	if err != nil {
		slog.Error("Failed to read request body", "error", err)
		encodederr, _ := json.Marshal("Failed to read request body")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(encodederr)
		return
	}

	pathSegments := strings.Split(requestPath, "/")[2:]

	hasTrailingSlash := false
	if pathSegments[len(pathSegments)-1] == "" {
		pathSegments = pathSegments[:len(pathSegments)-1]
		hasTrailingSlash = true
	}

	storageType := GetStorageType(len(pathSegments))
	if r.Method == "OPTIONS" {
		slog.Info("Determined storage type for OPTIONS request", "storageType", storageType)

		// Get supported methods for the storage type and set response headers
		supportedMethods := GetSupportedRequests(storageType)
		methodString := strings.Join(supportedMethods, ", ")

		w.Header().Set("Allow", methodString)
		w.Header().Set("Access-Control-Allow-Methods", methodString)
		w.WriteHeader(http.StatusOK)
		return
	}

	minKey := ""
	maxKey := ""

	var hasInterval bool
	intervalParam := r.URL.Query().Get("interval")

	if intervalParam != "" {
		// Extract min and max keys from interval parameter
		interval_split := getInterval(intervalParam)
		hasInterval = true
		minKey = interval_split[0]
		maxKey = interval_split[1]
	}

	mode := r.URL.Query().Get("mode")

	var noOverwrite bool
	var subscribeMode bool

	if mode != "" {
		if mode == "nooverwrite" {
			noOverwrite = true
		} else if mode == "subscribe" {
			subscribeMode = true
		}
	}

	// Validate the request based on storage type and parameters
	isValid, err := RequestValid(r.Method, storageType, hasTrailingSlash, hasInterval, noOverwrite)

	if !isValid {
		encodederr, _ := json.Marshal(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		w.Write(encodederr)
		return
	}

	// Extract the authorization token from the request header
	var authToken string
	authToken, err = processAuthField(r.Header.Get("Authorization"))

	if err != nil {
		encodederr, _ := json.Marshal(err.Error())
		w.WriteHeader(http.StatusUnauthorized)
		w.Write(encodederr)
		return
	}

	// Authorize the user using the token
	var user string

	user, err = owldb.authorize(authToken)

	if err != nil {
		encodederr, _ := json.Marshal(err.Error())
		w.WriteHeader(http.StatusUnauthorized)
		w.Write(encodederr)
		return
	}
	slog.Info("Request Valid")

	// Create a new HTTP request details object
	reqDetails := httpRequest{
		request:     r.Method,
		path:        pathSegments,
		content:     requestBody,
		validator:   owldb.validator,
		username:    user,
		minKey:      minKey,
		maxKey:      maxKey,
		noOverwrite: noOverwrite,
	}

	// Perform the operation using the storage handler
	opResult, status := owldb.storage.HandleOperation(reqDetails)

	// Determine the HTTP status code from the operation status
	statusCode, success := GetStatusCode(status.GetClass())
	slog.Info("check status", "type", r.Method, "status code", statusCode, "success", success, "stat", status)

	if !success {
		slog.Warn("Operation on child failed", "statusClass", status.GetClass(), "errorMessage", status.GetError().Error())
		encodederr, _ := json.Marshal(status.GetError().Error())
		slog.Info("Failed to process request", "method", r.Method, "path", pathSegments, "statusCode", statusCode, "encodederr", encodederr)
		w.WriteHeader(statusCode)
		w.Write(encodederr)
		return
	}

	// Encode the operation result to JSON
	encodedResponse, err := json.Marshal(opResult)
	if err != nil {
		slog.Error("Failed to encode response", "error", err)
		encodederr, _ := json.Marshal("failed to encode response")
		slog.Info("Failed to process request", "method", r.Method, "path", pathSegments, "statusCode", statusCode, "encodederr", encodederr)
		w.WriteHeader(http.StatusBadRequest)
		w.Write(encodederr)
		return
	}

	slog.Info("response marshal", "opResult", opResult, "encodedResponse", encodedResponse)

	// Determine event type based on the HTTP method
	var eventType string
	var eventData []byte
	if r.Method == "DELETE" {
		eventType = "delete"
		eventPath := requestPath
		eventData, err = json.Marshal(eventPath)
		if err != nil {
			slog.Error("Failed to encode response", "error", err)
			encodederr, _ := json.Marshal("failed to encode response")
			slog.Info("Failed to process request", "method", r.Method, "path", pathSegments, "statusCode", statusCode, "encodederr", encodederr)
			w.WriteHeader(http.StatusBadRequest)
			w.Write(encodederr)
			return
		}
	} else if r.Method == "PUT" || r.Method == "POST" || r.Method == "PATCH" {
		eventType = "update"
		eventPath := pathSegments
		if r.Method == "POST" {
			postDoc, err := extractPath(encodedResponse)
			if err != nil {
				slog.Error("Failed to get post path", "error", err)
				encodederr, _ := json.Marshal("failed to get post path")
				w.WriteHeader(http.StatusBadRequest)
				w.Write(encodederr)
				return
			}
			eventPath = append(eventPath, postDoc)
			slog.Info("in getting post path", "eventPath", eventPath, "post doc name", postDoc)
		}
		slog.Info("update path", "eventPath", eventPath)
		reqSubscribe := httpRequest{
			request:     "GET",
			path:        eventPath,
			content:     requestBody,
			validator:   owldb.validator,
			username:    user,
			minKey:      minKey,
			maxKey:      maxKey,
			noOverwrite: noOverwrite,
		}
		subResult, subStatus := owldb.storage.HandleOperation(reqSubscribe)

		// Determine the HTTP status code from the operation status
		subStatusCode, subSuccess := GetStatusCode(subStatus.GetClass())
		slog.Info("check status", "type", "GET", "status code", subStatusCode, "success", subSuccess, "stat", subStatus)

		if !subSuccess {
			slog.Warn("Operation on child failed", "statusClass", subStatus.GetClass(), "errorMessage", subStatus.GetError().Error())
			encodederr, _ := json.Marshal(subStatus.GetError().Error())
			w.WriteHeader(subStatusCode)
			w.Write(encodederr)
			return
		}

		eventData, err = json.Marshal(subResult)
		if err != nil {
			slog.Error("Failed to encode response", "error", err)
			encodederr, _ := json.Marshal("failed to get post path")
			w.WriteHeader(http.StatusBadRequest)
			w.Write(encodederr)
			return
		}

	} else if r.Method == "GET" {
		if subscribeMode {
			// Handle subscription requests separately
			owldb.HandleSubscription(w, r)
			return
		}
		eventType = ""
	} else {
		// For other methods, there shouldn't be any notifications
		slog.Info("No notifications for this HTTP method", "method", r.Method)
		eventType = "" // Set eventType to an empty string to indicate no notification
	}

	// Check if there are subscribers to notify
	hasSubscribers := false
	if eventType != "" {
		if storageType == "Database" && owldb.subscription.HasClients(requestPath+"/") {
			err = owldb.subscription.Dispatch(requestPath+"/", eventData, true, eventType)
			if err != nil {
				slog.Error("Failed to notify all subscribers", "error", err)
			}
			hasSubscribers = true
		} else if owldb.subscription.HasClients(requestPath) {
			err = owldb.subscription.Dispatch(requestPath, eventData, true, eventType)
			if err != nil {
				slog.Error("Failed to notify all subscribers", "error", err)
			}
			hasSubscribers = true
		}

	}
	// Check for collection-level subscribers
	if storageType == "Document" && owldb.subscription.HasClients("/v1/"+strings.Join(pathSegments[:len(pathSegments)-1], "/")+"/") {
		err = owldb.subscription.Dispatch("/v1/"+strings.Join(pathSegments[:len(pathSegments)-1], "/")+"/", eventData, false, eventType)
		if err != nil {
			slog.Error("Failed to notify collection subscribers", "error", err)
		}
		hasSubscribers = true
	}
	if !hasSubscribers {
		slog.Info("No subscribers for resource, skipping notification", "resource", requestPath)
	}

	if owldb == nil {
		slog.Error("owldb instance is nil")
	}

	// Send the response back to the client
	slog.Info("Successfully processed request", "method", r.Method, "path", pathSegments, "statusCode", statusCode)
	w.WriteHeader(statusCode)
	w.Write(encodedResponse)
}

type flusher interface {
	http.ResponseWriter
	http.Flusher
}

// HandleSubscription handles HTTP requests for client subscriptions
// Input: HTTP response writer and request
// Output: None
func (owldb *owldb) HandleSubscription(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
	w.Header().Set("Content-Type", "application/json")

	if owldb == nil {
		slog.Error("owldb instance is nil")
	}

	// Check that the client is requesting to subscribe... if not, return error
	if r.URL.Query().Get("mode") != "subscribe" {
		http.Error(w, "Invalid request mode", http.StatusBadRequest)
		return
	}

	// Get resource path, if doesn't exist, return error
	resourcePath := r.URL.Path

	// Perform authorization
	authToken, err := processAuthField(r.Header.Get("Authorization"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	user, err := owldb.authorize(authToken)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	// Convert response writer to flusher
	flusher, ok := w.(flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusBadRequest)
		return
	}

	slog.Info("Converted to writeFlusher")

	// Set up event stream connection
	flusher.Header().Set("Content-Type", "text/event-stream")
	flusher.Header().Set("Cache-Control", "no-cache")
	flusher.Header().Set("Connection", "keep-alive")
	flusher.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Last-Event-ID")
	flusher.Header().Set("Access-Control-Allow-Origin", "*")
	flusher.WriteHeader(http.StatusOK)
	flusher.Flush()

	slog.Info("Sent headers")

	// Create a channel for the client
	subscriberChannel := make(chan string, 10)

	// Add resource path and channel to subscribers
	err = owldb.subscription.Register(resourcePath, subscriberChannel)
	if err != nil {
		slog.Error("Failed to add subscriber", "resourcePath", resourcePath, "error", err)
		http.Error(w, "Unable to add subscriber", http.StatusBadRequest)
		return
	}

	// Notify that the subscription was successful
	slog.Info("Subscriber added", "resourcePath", resourcePath, "username", user)

	ticker := time.NewTicker(15 * time.Second) // Keep-alive interval
	defer ticker.Stop()

	// Start a goroutine to listen for messages sent to the client
	go func() {
		defer close(subscriberChannel)

		for {
			select {
			case message := <-subscriberChannel:
				// Write message to the client
				if _, err := fmt.Fprintf(w, "%s\n", message); err != nil {
					slog.Warn("Failed to write to client", "error", err)
					owldb.subscription.Unregister(resourcePath, subscriberChannel)
					return
				}
				flusher.Flush()
			case <-ticker.C:
				// Send a keep-alive comment
				fmt.Fprintf(w, ": keep-alive\n\n")
				flusher.Flush()
			case <-r.Context().Done():
				// Handle client disconnection
				err := r.Context().Err()
				slog.Info("Client disconnected", "resourcePath", resourcePath, "username", user, "reason", err)
				owldb.subscription.Unregister(resourcePath, subscriberChannel)
				return
			}
		}
	}()

	// Keep the connection open
	<-r.Context().Done()
}
