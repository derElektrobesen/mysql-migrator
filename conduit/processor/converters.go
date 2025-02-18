package processor

import (
	"github.com/derElektroBesen/mysql-migrator/conduit/processor/converters"
	"github.com/derElektroBesen/mysql-migrator/conduit/processor/repository"
)

func converterByType(t repository.FieldType) converters.Converter {
	switch tt := t.(type) {
	case repository.ArrayDataType:
		if e, ok := tt.NestedType().(repository.EnumDataType); ok {
			// Got set
			return converters.NewSetConverter(e)
		}

		return nil
	case repository.BooleanDataType:
		return converters.NewBooleanConverter()
	case repository.TimestampDataType:
		return converters.NewTimestampConverter()
	default:
		return nil
	}
}
