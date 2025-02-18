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

type collectionConfig struct {
	// BooleanFields is a list of references to collection boolean fields.
	// Reference to the field is a field name in .Payload.After structure.
	// This fields will be converted into boolean using
	// [strconv.ParseBool](https://pkg.go.dev/strconv#ParseBool) method.
	//
	// References should be separated with comma.
	// Spaces will be trimmed.
	// Example: field_a, field_b
	BooleanFields string `json:"boolean_fields"`

	// SetFields is a list of references to collection fields of type Set.
	// Reference to the field is a field name in .Payload.After structure.
	// [Mysql connector](https://github.com/conduitio-labs/conduit-connector-mysql)
	// represents values of type Set as a comma-separated strings.
	// Postgres requires brackets around a value.
	//
	// References should be separated with comma.
	// Spaces will be trimmed.
	// Example: field_a, field_b
	SetFields string `json:"set_fields"`

	// TimestampFields is a list of references to fields of type Timestamp.
	// Reference to the field is a field name in .Payload.After structure.
	// This processor is required for legacy MySQL v5.5 which supports
	// zero-timestamps (0000-00-00 00:00:00).
	// [Mysql connector](https://github.com/conduitio-labs/conduit-connector-mysql)
	// converts this kink of timestamps into golang 0-timestamp (0001-01-01 00:00:00).
	// It should be represented into null-values in Postgres.
	//
	// References should be separated with comma.
	// Spaces will be trimmed.
	// Example: field_a, field_b
	TimestampFields string `json:"timestamp_fields"`
}

type migrationProcessorConfig struct {
	Collections map[string]collectionConfig `json:"collections"`

	// DSN is required to understand set fields allowed values.
	// TODO: move this logic in config: at now configuration too
	// difficult from paramgen restrictions.
	//
	// Format: scheme://username:password@host:port/dbname?param1=value1&param2=value2&...
	PostgresDSN string `json:"mysql_db_dsn" validate:"required"`
}

type fieldConverter struct {
	fieldName string
	converter converters.Converter
}

type migrationProcessor struct {
	sdk.UnimplementedProcessor

	logger log.CtxLogger

	collections map[string][]fieldConverter
	repo        repository.Repository
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

	cols := slices.Collect(maps.Keys(p.collections))
	col, err := p.repo.FetchCollections(ctx, cols...)
	if err != nil {
		return fmt.Errorf("unable to fetch collections")
	}

	for k, v := range p.collections {
		cc := col.Fields(k)
		for _, f := range v {
			f.converter.SetFieldType(cc.Field(f.fieldName))
		}
	}

	return nil
}

func (p *migrationProcessor) Configure(ctx context.Context, c config.Config) error {
	cfg, err := p.parseConfig(ctx, c)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	p.collections = cfg

	return nil
}

func parseListOfReferences(list string) []string {
	references := []string{}
	for _, r := range strings.Split(list, ",") {
		r = strings.TrimSpace(r)
		references = append(references, r)
	}

	return references
}

func parseCollectionConfig(cfg collectionConfig) []fieldConverter {
	ret := []fieldConverter{}

	for _, x := range []struct {
		name    string
		src     string
		newConv func() converters.Converter
	}{
		{"BooleanFields", cfg.BooleanFields, converters.NewBooleanConverter},
		{"TimestampFields", cfg.TimestampFields, converters.NewSetConverter},
		{"SetFields", cfg.SetFields, converters.NewSetConverter},
	} {
		for _, r := range parseListOfReferences(x.src) {
			ret = append(ret, fieldConverter{
				fieldName: r,
				converter: x.newConv(),
			})
		}
	}

	return ret
}

func (p *migrationProcessor) parseConfig(ctx context.Context, c config.Config) (map[string][]fieldConverter, error) {
	cfg := migrationProcessorConfig{}
	err := sdk.ParseConfig(ctx, c, &cfg, cfg.Parameters())
	if err != nil {
		return nil, fmt.Errorf("unable to parse config: %w", err)
	}

	repo, err := repository.NewRepository(cfg.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("failed creating repository: %w", err)
	}

	p.repo = repo

	ret := make(map[string][]fieldConverter)
	for k, v := range cfg.Collections {
		ret[k] = parseCollectionConfig(v)
	}

	return ret, nil
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

	converters := p.collections[col]
	if converters == nil {
		return sdk.SingleRecord(rec), nil
	}

	payload, ok := rec.Payload.After.(opencdc.StructuredData)
	if !ok {
		return nil, fmt.Errorf("bad record type: %T, %T expected", rec.Payload.After, payload)
	}

	for _, c := range converters {
		field := payload[c.fieldName]

		res, err := c.converter.Convert(field)
		if err != nil {
			return nil, fmt.Errorf("failed converting field: %w", err)
		}

		payload[c.fieldName] = res
	}

	return sdk.SingleRecord(rec), nil
}
