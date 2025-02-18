package repository

import (
	"context"
	"fmt"
)

type FieldKind int

const (
	UnknownField FieldKind = iota
	BooleanField
	ArrayField
	EnumField
	TimestampField
)

type FieldType interface{}

type AnyDataType struct{}

func newAnyDataType() FieldType {
	return AnyDataType{}
}

type ArrayDataType struct {
	itemType FieldType
}

func newArrayDataType(ctx context.Context, r Repository, underlyingType string) (FieldType, error) {
	t, err := newSimpleDataType(ctx, r, underlyingType)
	if err != nil {
		return nil, fmt.Errorf("bad type: %w", err)
	}

	return ArrayDataType{
		itemType: t,
	}, nil
}

func (t ArrayDataType) NestedType() FieldType {
	return t.itemType
}

type EnumDataType struct {
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

	return EnumDataType{
		allowedValues: m,
	}, nil
}

func (t EnumDataType) IsSuitable(v string) bool {
	return t.allowedValues[v]
}

type BooleanDataType struct{}

func newBooleanDataType() FieldType {
	return BooleanDataType{}
}

type TimestampDataType struct{}

func newTimestampDataType() FieldType {
	return TimestampDataType{}
}
