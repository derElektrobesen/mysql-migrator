package processor

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/conduitio/conduit/pkg/foundation/log"
	"github.com/derElektroBesen/mysql-migrator/conduit/processor/converters"
	"github.com/derElektroBesen/mysql-migrator/conduit/processor/repository"
)

//go:generate paramgen -output=processor_paramgen.go migrationProcessorConfig

type migrationProcessorConfig struct {
	// DSN is required to understand set fields allowed values.
	//
	// Format: scheme://username:password@host:port/dbname?param1=value1&param2=value2&...
	PostgresDSN string `json:"postgres_dsn" validate:"required"`

	// Collections is a list of allowed collections.
	// Unknown collections passed in processor will trigger an error
	Collections string `json:"collections" validate:"required"`
}

type migrationProcessor struct {
	sdk.UnimplementedProcessor

	logger log.CtxLogger

	collections []string
	repo        repository.Repository
	converters  map[string]map[string]converters.Converter
}

func NewMigrationProcessor(logger log.CtxLogger) sdk.Processor {
	return &migrationProcessor{
		logger: logger,
	}
}

func (migrationProcessor) Specification() (sdk.Specification, error) {
	return sdk.Specification{
		Name:       "mysql-datatypes-processor",
		Summary:    "Converts data from MySQL into a format that Postgres can understand",
		Version:    "v1.0.0",
		Parameters: migrationProcessorConfig{}.Parameters(),
	}, nil
}

func (p *migrationProcessor) Open(ctx context.Context) error {
	if err := p.repo.Open(ctx); err != nil {
		return fmt.Errorf("unable to open repository: %w", err)
	}

	col, err := p.repo.FetchCollections(ctx, p.collections)
	if err != nil {
		return fmt.Errorf("unable to fetch collections: %w", err)
	}

	sortedA := slices.Sorted(slices.Values(p.collections))
	sortedB := slices.Sorted(maps.Keys(col))
	if !slices.Equal(sortedA, sortedB) {
		return fmt.Errorf("not all collections found in database: %v expected, %v found",
			sortedA, sortedB)
	}

	p.converters = make(map[string]map[string]converters.Converter)

	for collectionName, fields := range col {
		conv := map[string]converters.Converter{}
		for fieldName := range fields.Fields() {
			cc := converterByType(fields.Field(fieldName))
			if cc != nil {
				conv[fieldName] = cc
			}
		}

		p.converters[collectionName] = conv
	}

	return nil
}

func (p *migrationProcessor) Configure(ctx context.Context, c config.Config) error {
	cfg := migrationProcessorConfig{}
	err := sdk.ParseConfig(ctx, c, &cfg, cfg.Parameters())
	if err != nil {
		return fmt.Errorf("unable to parse config: %w", err)
	}

	repo, err := repository.NewRepository(cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("failed creating repository: %w", err)
	}

	for _, c := range strings.Split(cfg.Collections, ",") {
		p.collections = append(p.collections, strings.TrimSpace(c))
	}

	p.repo = repo

	return nil
}

func (p *migrationProcessor) Process(_ context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	out := make([]sdk.ProcessedRecord, 0, len(records))
	for _, rec := range records {
		proc, err := p.processRecord(rec)
		if err != nil {
			return append(out, sdk.ErrorRecord{Error: err})
		}

		out = append(out, proc)
	}

	return out
}

func (p *migrationProcessor) processRecord(rec opencdc.Record) (sdk.ProcessedRecord, error) {
	col, err := rec.Metadata.GetCollection()
	if err != nil {
		return nil, fmt.Errorf("unable to get collection: %w", err)
	}

	conv := p.converters[col]
	if conv == nil {
		// This error is required because we can't understand unknown
		// collection fields types => converters aren't made
		return nil, fmt.Errorf("unknown collection %q", col)
	}

	payload, ok := rec.Payload.After.(opencdc.StructuredData)
	if !ok {
		return nil, fmt.Errorf("bad record type: %T, %T expected", rec.Payload.After, payload)
	}

	for name, c := range conv {
		field := payload[name]

		res, err := c.Convert(field)
		if err != nil {
			return nil, fmt.Errorf("failed converting field: %w", err)
		}

		payload[name] = res
	}

	return sdk.SingleRecord(rec), nil
}
