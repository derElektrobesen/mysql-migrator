package main

import (
	"fmt"
	"io/ioutil"
	"log"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit/pkg/conduit"
	mysql "github.com/derElektroBesen/conduit-connector-mysql"
	postgres "github.com/derElektroBesen/conduit-connector-postgres"
	"gopkg.in/yaml.v2"
)

func readYAMLConfig(filename string, dest *conduit.Config) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("unable to read file %q: %w", filename, err)
	}

	err = yaml.Unmarshal(data, &dest)
	if err != nil {
		return fmt.Errorf("unable to unmarshal config %q: %w", filename, err)
	}
	return nil
}

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

	// TODO: Merge config with command-lile arguments
	if err := readYAMLConfig(cfg.ConduitCfg.Path, &cfg); err != nil {
		log.Fatalf("unable read config: %s", err)
	}

	// Add the HTTP connector to list of built-in
	// connectors
	cfg.ConnectorPlugins["mysql"] = mysql.Connector

	const psqlConnectorName = "psql"
	cfg.ConnectorPlugins[psqlConnectorName] = newPSQLConnector(psqlConnectorName)

	e := &conduit.Entrypoint{}
	e.Serve(cfg)
}
