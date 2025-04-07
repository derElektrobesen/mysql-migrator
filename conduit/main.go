package main

import (
	mysql "github.com/conduitio-labs/conduit-connector-mysql"

	"github.com/conduitio/conduit/pkg/conduit"
	proc_builtin "github.com/conduitio/conduit/pkg/plugin/processor/builtin"
	"github.com/derElektroBesen/mysql-migrator/conduit/command"
	"github.com/derElektroBesen/mysql-migrator/conduit/processor"
)

func main() {
	// Get the default configuration, including all built-in
	// connectors
	cfg := conduit.DefaultConfig()

	// Add the MySQL connector to list of built-in
	// connectors
	cfg.ConnectorPlugins["mysql"] = mysql.Connector

	// Setup builtin processor
	proc_builtin.DefaultBuiltinProcessors["mysql-datatypes-processor"] = processor.NewMigrationProcessor

	command.Run(cfg)
}
