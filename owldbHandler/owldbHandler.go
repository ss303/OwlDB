package owldbhandler

import (
	"net/http"

	"github.com/RICE-COMP318-FALL24/owldb-p1group35/handlers"
)

func New(schemaFile string, tokenFile string) (http.Handler, error) {
	owldb, err := handlers.New(schemaFile, tokenFile)

	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()

	// Separate handlers for auth vs. data requests
	mux.HandleFunc("/auth", owldb.HandleAuth)
	mux.HandleFunc("/v1/", owldb.HandleStorage)

	return mux, nil
}
