package main

import (
	"github.com/conduitio/conduit/pkg/conduit"
	mysql "github.com/derElektroBesen/conduit-connector-mysql"
	postgres "github.com/derElektroBesen/conduit-connector-postgres"
)

func main() {
	// Get the default configuration, including all built-in
	// connectors
	cfg := conduit.DefaultConfig()

	// Add the HTTP connector to list of built-in
	// connectors
	cfg.ConnectorPlugins["mysql"] = mysql.Connector
	cfg.ConnectorPlugins["postgres"] = postgres.Connector

	e := &conduit.Entrypoint{}
	e.Serve(cfg)
}
