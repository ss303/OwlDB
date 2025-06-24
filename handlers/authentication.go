package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"time"
)

type authEntry struct {
	username   string
	expiration time.Time
}

// Login request structure
type loginRequest struct {
	Token string `json:"token"`
}

// generateToken generates a random token string
// Input: None
// Output: Randomly generated token string
func generateToken() string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	token := make([]byte, 16)
	for i := range token {
		token[i] = letters[rand.Intn(len(letters))]
	}
	return string(token)
}

// authorize checks if the provided token is valid and not expired
// Input: token string
// Output: username if authorized, error if unauthorized
func (owldb *owldb) authorize(token string) (string, error) {
	owldb.mu.RLock()
	defer owldb.mu.RUnlock()
	slog.Info("tokenToUser", "tokenToUser", owldb.tokenToUser)
	user, ok := owldb.tokenToUser[token]
	if !ok || time.Now().After(user.expiration) {
		return "", fmt.Errorf("missing or invalid bearer token")
	}
	return user.username, nil
}

// login processes the login request and generates a bearer token for the user
// Input: requestData in byte format
// Output: loginRequest struct with generated token, or error
func (owldb *owldb) login(requestData []byte) (*loginRequest, error) {
	var userCredentials map[string]string
	err := json.Unmarshal(requestData, &userCredentials)
	if err != nil {
		return nil, fmt.Errorf("login request body in incorrect format")
	}

	// Check if the username is present
	username, exists := userCredentials["username"]
	if !exists || username == "" {
		// Return the error in JSON format with the specific message
		return nil, fmt.Errorf(`"No username in request body"`)
	}

	owldb.mu.Lock()
	defer owldb.mu.Unlock()

	bearerToken := generateToken()
	_, tokenExists := owldb.tokenToUser[bearerToken]
	for tokenExists {
		bearerToken = generateToken()
		_, tokenExists = owldb.tokenToUser[bearerToken]
	}

	expirationTime := time.Now().Add(1 * time.Hour)
	tokenDetails := authEntry{username: username, expiration: expirationTime}
	owldb.tokenToUser[bearerToken] = tokenDetails

	loginResponse := loginRequest{Token: bearerToken}
	return &loginResponse, nil
}

// logout invalidates the provided bearer token
// Input: authToken string
// Output: error if the token is missing or invalid
func (owldb *owldb) logout(authToken string) error {
	owldb.mu.Lock()
	defer owldb.mu.Unlock()
	_, exists := owldb.tokenToUser[authToken]
	if !exists {
		return fmt.Errorf("missing or invalid bearer token")
	}
	delete(owldb.tokenToUser, authToken)
	return nil
}

// processAuthField extracts the bearer token from the authorization header
// Input: authHeader string
// Output: token string or error if missing or invalid
func processAuthField(authHeader string) (string, error) {
	if authHeader == "" || len(authHeader) < 7 || authHeader[:7] != "Bearer " {
		return "", fmt.Errorf("missing or invalid bearer token")
	}
	return authHeader[7:], nil
}

// HandleAuth handles authentication requests for login (POST) and logout (DELETE)
// Input: HTTP response writer and request
// Output: None
func (owldb *owldb) HandleAuth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
	w.Header().Set("Content-Type", "application/json")

	reqMethod := r.Method
	requestBody, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("Failed to read request body", "error", err)
		encodederr, _ := json.Marshal("unable to read body")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(encodederr)
		return
	}

	if reqMethod == "POST" {
		// Handle login request
		loginResponse, err := owldb.login(requestBody)
		if err != nil {
			encodederr, _ := json.Marshal(err.Error())
			w.WriteHeader(http.StatusBadRequest)
			w.Write(encodederr)
			return
		}

		encodedResponse, err := json.Marshal(*loginResponse)
		if err != nil {
			slog.Error(err.Error())
			encodederr, _ := json.Marshal(err.Error())
			w.WriteHeader(http.StatusBadRequest)
			w.Write(encodederr)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(encodedResponse)
		return
	} else if reqMethod == "DELETE" {
		// Handle logout request
		authHeader := r.Header.Get("Authorization")
		authToken, err := processAuthField(authHeader)
		if err != nil {
			encodederr, _ := json.Marshal(err.Error())
			w.WriteHeader(http.StatusUnauthorized)
			w.Write(encodederr)
			return
		}

		err = owldb.logout(authToken)
		if err != nil {
			slog.Error(err.Error())
			encodederr, _ := json.Marshal(err.Error())
			w.WriteHeader(http.StatusUnauthorized)
			w.Write(encodederr)
			return
		}
		w.WriteHeader(204)
		return
	} else if reqMethod == "OPTIONS" {
		// Handle CORS preflight request
		allowedMethods := "POST, DELETE"
		w.Header().Set("Allow", allowedMethods)
		w.Header().Set("Access-Control-Allow-Methods", allowedMethods)
		w.WriteHeader(http.StatusOK)
		return
	} else {
		encodederr, _ := json.Marshal("bad request")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(encodederr)
		return
	}
}
