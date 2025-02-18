package converters

import "github.com/derElektroBesen/mysql-migrator/conduit/processor/repository"

type Converter interface {
	Convert(any) (any, error)
	SetFieldType(repository.FieldType)
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

	return m.c.Convert(v)
}

func (m defaultMiddleware) SetFieldType(f repository.FieldType) {
	m.c.SetFieldType(f)
}
