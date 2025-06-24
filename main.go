// This file is a skeleton for your project. You should replace this
// comment with true documentation.

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	owldbhandler "github.com/RICE-COMP318-FALL24/owldb-p1group35/owldbHandler"
)

func main() {
	var server http.Server
	var err error

	portFlag := flag.Int("p", 3318, "port for the server to listen to")
	schemaFileFlag := flag.String("s", "", "file that contains JSON schema for validating documents")
	tokenFileFlag := flag.String("t", "", "file that contains a JSON object mapping usernames to tokens")
	flag.Parse()

	port := *portFlag
	tokenFile := *tokenFileFlag
	schemaFile := *schemaFileFlag
	slog.Info("Server configuration", "port: ", port, "schema: ", schemaFile, "token: ", tokenFile)

	handler, err := owldbhandler.New(schemaFile, tokenFile)

	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	server = http.Server{
		//Addr:    fmt.Sprintf(":%d", port),
		Addr:    fmt.Sprintf(":%d", port),
		Handler: handler,
	}

	// The following code should go last and remain unchanged.
	// Note that you must actually initialize 'server' and 'port'
	// before this.  Note that the server is started below by
	// calling ListenAndServe.  You must not start the server
	// before this.

	// signal.Notify requires the channel to be buffered
	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctrlc
		slog.Info("Shutting down server...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			slog.Error("Server forced to shutdown", "error", err)
		}
	}()

	// Start server
	slog.Info("Listening", "port", port)
	err = server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		slog.Error("Server closed", "error", err)
	} else {
		slog.Info("Server closed", "error", err)
	}
}
