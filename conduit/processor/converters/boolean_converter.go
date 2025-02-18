package converters

import (
	"fmt"
	"strconv"
)

type booleanConverter struct {
	defaultConverter
}

func NewBooleanConverter() Converter {
	return withDefaultMiddlewares(&booleanConverter{})
}

func (c booleanConverter) Convert(v any) (any, error) {
	r, err := strconv.ParseBool(c.toString(v))
	if err != nil {
		return nil, fmt.Errorf("unable to convert to boolean: %w", err)
	}
	return r, nil
}

func (booleanConverter) toString(value any) string {
	switch v := value.(type) {
	case []byte:
		return string(v)
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	default:
		return fmt.Sprintf("%v", value)
	}
}
