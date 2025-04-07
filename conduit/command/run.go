package command

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/conduitio/conduit/cmd/conduit/cecdysis"
	"github.com/conduitio/conduit/cmd/conduit/root"
	"github.com/conduitio/conduit/pkg/conduit"
	"github.com/conduitio/conduit/pkg/foundation/metrics"
	"github.com/conduitio/conduit/pkg/foundation/metrics/prometheus"
	"github.com/conduitio/ecdysis"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/graphite"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

type graphiteCfg struct {
	Enabled  bool          `yaml:"enabled"`
	Addr     string        `yaml:"addr"`
	Prefix   string        `yaml:"prefix"`
	Interval time.Duration `yaml:"interval"`
}

type statDecorator struct{}

func (statDecorator) Decorate(_ *ecdysis.Ecdysis, cmd *cobra.Command, c ecdysis.Command) error {
	// there is no any stat if there is no Execute method
	if _, ok := c.(ecdysis.CommandWithExecute); !ok {
		return nil
	}

	cfg := graphiteCfg{
		Interval: time.Minute,
	}

	flags := cmd.Flags()
	flags.BoolVarP(&cfg.Enabled, "graphite.enabled", "", false, "push statistics to graphite")
	flags.StringVarP(&cfg.Addr, "graphite.addr", "", "", "graphite gateway addr (host:port)")
	flags.StringVarP(&cfg.Prefix, "graphite.prefix", "", "", "graphite stat prefix")
	flags.DurationVarP(&cfg.Interval, "graphite.interval", "", cfg.Interval, "push stat interval")

	if v, ok := c.(ecdysis.CommandWithConfig); ok {
		oldPreRunCmd := cmd.PreRunE
		cmd.PreRunE = func(c *cobra.Command, args []string) error {
			if oldPreRunCmd != nil {
				if err := oldPreRunCmd(c, args); err != nil {
					return err
				}
			}

			return extendStatConfig(v.Config().Path, &cfg)
		}
	}

	oldRunFn := cmd.RunE
	cmd.RunE = func(c *cobra.Command, args []string) error {
		if err := extendStat(cmd.Context(), cfg); err != nil {
			return fmt.Errorf("unable to extend stat: %w", err)
		}

		return oldRunFn(c, args)
	}

	return nil
}

func extendStatConfig(path string, dest *graphiteCfg) error {
	// TODO: command line args should override config
	var cfg = struct {
		Graphite *graphiteCfg `yaml:"graphite"`
	}{
		Graphite: dest,
	}

	yamlFile, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("unable to read %q: %w", path, err)
	}

	err = yaml.Unmarshal(yamlFile, &cfg)
	if err != nil {
		return fmt.Errorf("unable to unmarshal %q: %w", path, err)
	}

	return nil
}

func extendStat(ctx context.Context, cfg graphiteCfg) error {
	if !cfg.Enabled {
		return nil
	}

	collector := prometheus.NewRegistry(nil)
	metrics.Register(collector)

	reg := prom.NewRegistry()
	reg.MustRegister(collector)

	b, err := graphite.NewBridge(&graphite.Config{
		URL:           cfg.Addr,
		Gatherer:      reg,
		Prefix:        cfg.Prefix,
		Interval:      cfg.Interval,
		Timeout:       10 * time.Second,
		ErrorHandling: graphite.ContinueOnError,
	})
	if err != nil {
		return fmt.Errorf("unable to create graphite bridge: %w", err)
	}

	go b.Run(ctx)

	return nil
}

func Run(cfg conduit.Config) {
	e := ecdysis.New(ecdysis.WithDecorators(
		cecdysis.CommandWithExecuteWithClientDecorator{},
		statDecorator{},
	))

	cmd := e.MustBuildCobraCommand(&root.RootCommand{
		Cfg: cfg,
	})
	cmd.CompletionOptions.DisableDefaultCmd = true

	// Don't want to show usage when there's some unexpected error executing the command
	// Help will still be shown via --help
	cmd.SilenceUsage = true

	if err := cmd.Execute(); err != nil {
		// error is already printed out
		os.Exit(1)
	}
	os.Exit(0)
}
