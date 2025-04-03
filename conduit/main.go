package main

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit/cmd/conduit/cli"
	"github.com/conduitio/conduit/pkg/conduit"
	proc_builtin "github.com/conduitio/conduit/pkg/plugin/processor/builtin"
	mysql "github.com/derElektroBesen/conduit-connector-mysql"
	postgres "github.com/derElektroBesen/conduit-connector-postgres"
	"github.com/derElektroBesen/mysql-migrator/conduit/processor"
)

func newPSQLConnector(name string) sdk.Connector {
	c := postgres.Connector

	newSpec := c.NewSpecification
	c.NewSpecification = func() sdk.Specification {
		// renaming is required to prevent conflicts
		// with native postgresql plugin
		spec := newSpec()
		spec.Name = name

		return spec
	}

	return c
}

func main() {
	// Get the default configuration, including all built-in
	// connectors
	cfg := conduit.DefaultConfig()

	// Add the HTTP connector to list of built-in
	// connectors
	cfg.ConnectorPlugins["mysql"] = mysql.Connector

	const psqlConnectorName = "psql"
	cfg.ConnectorPlugins[psqlConnectorName] = newPSQLConnector(psqlConnectorName)

	// Setup builtin processor
	proc_builtin.DefaultBuiltinProcessors["mysql-datatypes-processor"] = processor.NewMigrationProcessor

	cli.Run(cfg)
}
