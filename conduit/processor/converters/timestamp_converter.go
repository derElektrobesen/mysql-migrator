package converters

import "fmt"

type TimestampConverter struct{}

func (TimestampConverter) Convert(v any) (any, error) {
	tm, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("invalid timestamp %+v: should be a string")
	}

	if tm == "0001-01-01T00:00:00Z" {
		return nil, nil
	}

	return v, nil
}
