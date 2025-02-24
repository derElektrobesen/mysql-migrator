package converters

import (
	"fmt"
)

type timestampConverter struct {
	defaultConverter
}

func NewTimestampConverter() Converter {
	return withDefaultMiddlewares(&timestampConverter{})
}

func (timestampConverter) Convert(v any) (any, error) {
	tm, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("invalid timestamp %+v (%T): should be a string", tm, tm)
	}

	if tm == "0001-01-01T00:00:00Z" {
		return nil, nil
	}

	return v, nil
}
