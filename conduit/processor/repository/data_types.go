package repository

import (
	"context"
	"fmt"
)

type FieldType interface {
	IsSuitable(value string) bool
}

type anyDataType struct{}

func (anyDataType) IsSuitable(string) bool {
	return true
}

func newAnyDataType() FieldType {
	return anyDataType{}
}

type arrayDataType struct {
	itemType FieldType
}

func newArrayDataType(ctx context.Context, r Repository, underlyingType string) (FieldType, error) {
	t, err := newSimpleDataType(ctx, r, underlyingType)
	if err != nil {
		return nil, fmt.Errorf("bad type: %w", err)
	}

	return arrayDataType{
		itemType: t,
	}, nil
}

func (t arrayDataType) IsSuitable(v string) bool {
	return t.itemType.IsSuitable(v)
}

type enumDataType struct {
	allowedValues map[string]bool
}

func newEnumDataType(ctx context.Context, r Repository, name string) (FieldType, error) {
	allowedValues, err := r.EnumRange(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to select enum range: %w", err)
	}

	m := make(map[string]bool)
	for _, v := range allowedValues {
		m[v] = true
	}

	return enumDataType{
		allowedValues: m,
	}, nil
}

func (t enumDataType) IsSuitable(v string) bool {
	return t.allowedValues[v]
}
