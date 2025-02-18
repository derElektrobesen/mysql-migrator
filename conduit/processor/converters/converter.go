package converters

import "github.com/derElektroBesen/mysql-migrator/conduit/processor/repository"

type Converter interface {
	Convert(any) (any, error)
}

type defaultConverter struct{}

func (defaultConverter) SetFieldType(repository.FieldType) {}

type defaultMiddleware struct {
	c Converter
}

func withDefaultMiddlewares(c Converter) Converter {
	return &defaultMiddleware{
		c: c,
	}
}

func (m defaultMiddleware) Convert(v any) (any, error) {
	if v == nil {
		return nil, nil
	}

	return m.c.Convert(v) //nolint:wrapcheck // there is no sense to wrap an error
}
