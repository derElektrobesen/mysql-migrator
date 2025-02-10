package processor

import (
	"context"
	"fmt"

	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/conduitio/conduit/pkg/foundation/log"
)

//go:generate paramgen -output=processor_paramgen.go migrationProcessorConfig

type migrationProcessorConfig struct {
	BooleanFields []string `json:"boolean_fields"`
}

type migrationProcessor struct {
	sdk.UnimplementedProcessor

	logger log.CtxLogger
}

func NewMigrationProcessor(logger log.CtxLogger) sdk.Processor {
	return &migrationProcessor{
		logger: logger,
	}
}

func (migrationProcessor) Specification() (sdk.Specification, error) {
	return sdk.Specification{Name: "mysql-datatypes-processor", Version: "v1.0.0"}, nil
}

func (migrationProcessor) Open(context.Context) error {
	return fmt.Errorf("ghohohohoh")
}
