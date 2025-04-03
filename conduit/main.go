package main

import (
	"github.com/conduitio/conduit/cmd/conduit/cli"
	"github.com/conduitio/conduit/pkg/conduit"
	proc_builtin "github.com/conduitio/conduit/pkg/plugin/processor/builtin"
	mysql "github.com/derElektroBesen/conduit-connector-mysql"
	"github.com/derElektroBesen/mysql-migrator/conduit/processor"
)

func main() {
	// Get the default configuration, including all built-in
	// connectors
	cfg := conduit.DefaultConfig()

	// Add the HTTP connector to list of built-in
	// connectors
	cfg.ConnectorPlugins["mysql"] = mysql.Connector

	// Setup builtin processor
	proc_builtin.DefaultBuiltinProcessors["mysql-datatypes-processor"] = processor.NewMigrationProcessor

	cli.Run(cfg)
}
