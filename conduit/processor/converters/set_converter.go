package converters

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/derElektroBesen/mysql-migrator/conduit/processor/repository"
)

type setConverter struct {
	field repository.EnumDataType
}

func NewSetConverter(field repository.EnumDataType) Converter {
	return withDefaultMiddlewares(&setConverter{
		field: field,
	})
}

func (c *setConverter) Convert(v any) (any, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("string is expected, %T found", v)
	}

	if s == "" {
		if c.field.IsSuitable(s) {
			// empty string as allowed by set
			return `{""}`, nil
		}

		// empty string isn't allowed by set: return NULL
		return nil, nil
	}

	// Comma couldn't be a part of enum in mysql.
	// That's safe to split string just with comma
	res := make([]string, 0)
	for _, el := range strings.Split(s, ",") {
		if !c.field.IsSuitable(el) {
			return nil, fmt.Errorf("unexpected field found: %q", el)
		}

		res = append(res, strconv.Quote(el))
	}

	return "{" + strings.Join(res, ",") + "}", nil
}
