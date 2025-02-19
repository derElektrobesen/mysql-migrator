package repository

import (
	"context"
	"fmt"
	"iter"

	_ "github.com/jackc/pgx/stdlib"
	"github.com/jackc/pgx/v5"
	"github.com/jmoiron/sqlx"
)

type Repository interface {
	Open(ctx context.Context) error

	FetchCollections(ctx context.Context, collections []string) (Collections, error)
	TypeCategory(ctx context.Context, name string) (string, error)
	EnumRange(ctx context.Context, name string) ([]string, error)
}

type collection struct {
	fieldsTypes map[string]FieldType
}

type Collections map[string]collection

type FieldsCollection interface {
	Field(name string) FieldType
}

func (cc Collections) Fields(name string) FieldsCollection {
	return cc[name]
}

func (c collection) Field(name string) FieldType {
	return c.fieldsTypes[name]
}

func (c collection) Fields() iter.Seq[string] {
	return func(yield func(string) bool) {
		for k := range c.fieldsTypes {
			if !yield(k) {
				return
			}
		}
	}
}

type repository struct {
	db    *sqlx.DB
	dbCfg *pgx.ConnConfig
}

func NewRepository(dsn string) (Repository, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse given URL: %w", err)
	}

	return &repository{
		dbCfg: cfg,
	}, nil
}

func (r *repository) Open(ctx context.Context) error {
	db, err := sqlx.ConnectContext(ctx, "pgx", r.dbCfg.ConnString())
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}

	r.db = db

	return nil
}

type dataTypeRow struct {
	TableName  string `db:"table_name"`
	ColumnName string `db:"column_name"`
	DataType   string `db:"data_type"`
	UDTName    string `db:"udt_name"`
}

func (r *repository) FetchCollections(ctx context.Context, collections []string) (Collections, error) {
	query, args, err := sqlx.In(`
		SELECT
			column_name, data_type, udt_name, table_name
		FROM
			information_schema.columns
		WHERE
			table_catalog = ? AND table_name IN (?)
	`, r.dbCfg.Database, collections)

	if err != nil {
		return nil, fmt.Errorf("failed to prepare IN statement: %w", err)
	}

	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch collections types: %w", err)
	}

	c := make(Collections)

	for rows.Next() {
		v := dataTypeRow{}
		err := rows.StructScan(&v)
		if err != nil {
			return nil, fmt.Errorf("unable to scan row: %w", err)
		}

		if err := c.newDataType(ctx, v, r); err != nil {
			return nil, fmt.Errorf("unable to store data type: %w", err)
		}
	}

	return c, nil
}

func (r *repository) EnumRange(ctx context.Context, name string) ([]string, error) {
	res := []string{}
	err := r.db.SelectContext(ctx, &res,
		fmt.Sprintf(`select unnest(enum_range(null::%q)) as value`, name),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to select enum range: %w", err)
	}

	return res, nil
}

func (r *repository) TypeCategory(ctx context.Context, name string) (string, error) {
	var res []string
	err := r.db.SelectContext(ctx, &res,
		`SELECT typcategory FROM pg_type WHERE typname = $1`,
		name,
	)
	if err != nil {
		return "", fmt.Errorf("failed to select type category: %w", err)
	}

	if len(res) != 1 {
		return "", fmt.Errorf("bad type %q: typcategories found: %v", name, res)
	}

	return res[0], nil
}

func (cc Collections) newDataType(ctx context.Context, v dataTypeRow, r Repository) error {
	t, err := newDataType(ctx, r, v.DataType, v.UDTName)
	if err != nil {
		return fmt.Errorf("unable to make data type: %w", err)
	}

	c := cc[v.TableName]
	if c.fieldsTypes == nil {
		c.fieldsTypes = map[string]FieldType{}
	}

	c.fieldsTypes[v.ColumnName] = t
	cc[v.TableName] = c

	return nil
}

func newSimpleDataType(ctx context.Context, repo Repository, name string) (FieldType, error) {
	cat, err := repo.TypeCategory(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("unable to get category type: %w", err)
	}

	// https://www.postgresql.org/docs/current/catalog-pg-type.html
	switch cat {
	case "E": // enum
		return newEnumDataType(ctx, repo, name)
	default:
		return newAnyDataType(), nil
	}
}

func newDataType(ctx context.Context, repo Repository, name string, underlyingName string) (FieldType, error) {
	switch name {
	case "ARRAY":
		// this data type is composite
		return newArrayDataType(ctx, repo, underlyingName)
	case "boolean":
		return newBooleanDataType(), nil
	case "timestamp with timezone":
		return newTimestampDataType(), nil
	default:
		return newAnyDataType(), nil
	}
}
