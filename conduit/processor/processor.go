package processor

import (
	"context"
	"fmt"
	"strings"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/conduitio/conduit/pkg/foundation/log"
	"github.com/derElektroBesen/mysql-migrator/conduit/processor/converters"
)

//go:generate paramgen -output=processor_paramgen.go migrationProcessorConfig

type collectionConfig struct {
	// BooleanFields is a list of references to collection boolean fields.
	// This fields will be converted into boolean using
	// [strconv.ParseBool](https://pkg.go.dev/strconv#ParseBool) method.
	//
	// References should be separated with comma.
	// Example: .Payload.After.field_a, .Payload.After.field_b
	//
	// For more information about the format,
	// see [Referencing fields](https://conduit.io/docs/using/processors/referencing-fields).
	BooleanFields string `json:"boolean_fields"`

	// SetFields is a list of references to collection fields of type Set.
	// [Mysql connector](https://github.com/conduitio-labs/conduit-connector-mysql)
	// represents values of type Set as a comma-separated strings.
	// Postgres requires brackets around a value.
	//
	// References should be separated with comma.
	// Example: .Payload.After.field_a, .Payload.After.field_b
	//
	// For more information about the format,
	// see [Referencing fields](https://conduit.io/docs/using/processors/referencing-fields).
	SetFields string `json:"set_fields"`

	// TimestampFields is a list of references to collection fields of type
	// Timestamp.
	// This processor is required for legacy MySQL v5.5 which supports
	// zero-timestamps (0000-00-00 00:00:00).
	// [Mysql connector](https://github.com/conduitio-labs/conduit-connector-mysql)
	// converts this kink of timestamps into golang 0-timestamp (0001-01-01 00:00:00).
	// It should be represented into null-values in Postgres.
	//
	// References should be separated with comma.
	// Example: .Payload.After.field_a, .Payload.After.field_b
	//
	// For more information about the format,
	// see [Referencing fields](https://conduit.io/docs/using/processors/referencing-fields).
	TimestampFields string `json:"timestamp_fields"`
}

type migrationProcessorConfig struct {
	Collections map[string]collectionConfig `json:"collections"`

	// DSN is required to understand set fields allowed values.
	// TODO: move this logic in config: at now configuration too
	// difficult from paramgen restrictions
	MySQLDBDSN string `json:"mysql_db_dsn" validate:"required,regex=^[^:]+:.*@tcp\\([^:]+:\\d+\\)/\\S+"`
}

type fieldConverter struct {
	resolver  sdk.ReferenceResolver
	converter Converter
}

type migrationProcessor struct {
	sdk.UnimplementedProcessor

	logger log.CtxLogger

	collections map[string][]fieldConverter
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

func (p *migrationProcessor) Configure(ctx context.Context, c config.Config) error {
	cfg, err := parseConfig(ctx, c)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	p.collections = cfg

	return nil
}

func parseListOfReferences(list string) ([]sdk.ReferenceResolver, error) {
	var references []sdk.ReferenceResolver
	for _, r := range strings.Split(list, ",") {
		rr, err := sdk.NewReferenceResolver(r)
		if err != nil {
			return nil, fmt.Errorf("unable to parse reference %q: %w", r, err)
		}

		references = append(references, rr)
	}

	return references, nil
}

func parseCollectionConfig(cfg collectionConfig) ([]fieldConverter, error) {
	ret := []fieldConverter{}

	for _, x := range []struct {
		name      string
		src       string
		converter Converter
	}{
		{"BooleanFields", cfg.BooleanFields, converters.BooleanConverter{}},
		{"SetFields", cfg.SetFields, converters.SetConverter{}},
		{"TimestampFields", cfg.TimestampFields, converters.TimestampConverter{}},
	} {
		rr, err := parseListOfReferences(x.src)
		if err != nil {
			return []fieldConverter{}, fmt.Errorf("unable to parse %q: %w", x.name, err)
		}

		for _, r := range rr {
			ret = append(ret, fieldConverter{
				resolver:  r,
				converter: x.converter,
			})
		}
	}

	return ret, nil
}

func parseConfig(ctx context.Context, c config.Config) (map[string][]fieldConverter, error) {
	cfg := migrationProcessorConfig{}
	err := sdk.ParseConfig(ctx, c, &cfg, cfg.Parameters())
	if err != nil {
		return nil, err
	}

	ret := make(map[string][]fieldConverter)
	for k, v := range cfg.Collections {
		ret[k], err = parseCollectionConfig(v)
		if err != nil {
			return nil, fmt.Errorf("unable to parse collection %q: %w", k, err)
		}
	}

	return ret, nil
}

func (p *migrationProcessor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
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

	for _, c := range converters {
		field, err := c.resolver.Resolve(&rec)
		if err != nil {
			return nil, fmt.Errorf("failed resolving field: %w", err)
		}

		res, err := c.converter.Convert(field.Get())
		if err != nil {
			return nil, fmt.Errorf("failed converting field: %w", err)
		}

		if err := field.Set(res); err != nil {
			return nil, fmt.Errorf("failed setting field: %w", err)
		}
	}

	return sdk.SingleRecord(rec), nil
}
