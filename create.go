package gormdm

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func Create(db *gorm.DB) {
	stmt := db.Statement
	schema := stmt.Schema

	if stmt == nil {
		return
	}
	if schema == nil {
		if stmt.SQL.Len() == 0 {
			stmt.SQL.Grow(180)
			stmt.AddClauseIfNotExists(clause.Insert{})
			stmt.AddClause(ConvertToCreateValuesWithBackQuote(stmt))
			stmt.Build(stmt.BuildClauses...)
			// convert to raw
			stmt.ConnPool.ExecContext(stmt.Context, stmt.SQL.String(), stmt.Vars...)
		}
		return
	}

	if !stmt.Unscoped {
		for _, c := range schema.CreateClauses {
			stmt.AddClause(c)
		}
	}

	if stmt.SQL.String() == "" {
		values := ConvertToCreateValuesWithBackQuote(stmt)
		// json.RawMessage | []byte -> string
		for i := range values.Values {
			for j := range values.Values[i] {
				switch v := values.Values[i][j].(type) {
				case *json.RawMessage:
					values.Values[i][j] = string(*v)
				case json.RawMessage:
					values.Values[i][j] = string(v)
				case []byte:
					values.Values[i][j] = string(v)
				}
			}
		}
		onConflict, hasConflict := stmt.Clauses["ON CONFLICT"].Expression.(clause.OnConflict)
		if hasConflict {
			if len(onConflict.Columns) == 0 && len(db.Statement.Schema.PrimaryFields) > 0 {
				columnsMap := map[string]bool{}
				for _, column := range values.Columns {
					columnsMap[column.Name] = true
				}

				for _, field := range db.Statement.Schema.PrimaryFields {
					if _, ok := columnsMap[field.DBName]; !ok {
						hasConflict = false
					}
				}
			}
		}
		var setIdentityInsert bool
		var needReturnId bool
		if field := db.Statement.Schema.PrioritizedPrimaryField; field != nil && field.AutoIncrement {
			switch db.Statement.ReflectValue.Kind() {
			case reflect.Struct:
				_, isZero := field.ValueOf(db.Statement.Context, db.Statement.ReflectValue)
				setIdentityInsert = !isZero
				needReturnId = isZero
			case reflect.Slice, reflect.Array:
				for i := 0; i < db.Statement.ReflectValue.Len(); i++ {
					obj := db.Statement.ReflectValue.Index(i)
					if reflect.Indirect(obj).Kind() == reflect.Struct {
						_, isZero := field.ValueOf(db.Statement.Context, db.Statement.ReflectValue.Index(i))
						setIdentityInsert = !isZero
						needReturnId = isZero
						break
					}
				}
			}
		}
		if hasConflict {
			MergeCreate(db, onConflict, values)
		} else {
			var tableName = stmt.Table
			if tableName == "" {
				tableName = stmt.TableExpr.SQL
			}

			if setIdentityInsert {
				stmt.WriteString("SET IDENTITY_INSERT ")
				stmt.WriteQuoted(tableName)
				stmt.WriteString(" ON;")
			}
			stmt.AddClauseIfNotExists(clause.Insert{Table: clause.Table{Name: tableName}})
			stmt.AddClause(clause.Values{Columns: values.Columns, Values: values.Values})
			stmt.Build("INSERT", "VALUES")
			if setIdentityInsert {
				stmt.WriteString(";SET IDENTITY_INSERT ")
				stmt.WriteQuoted(tableName)
				stmt.WriteString(" OFF;")
			}
		}

		if !db.DryRun {
			var exec = func(db *gorm.DB) {
				result, err := db.Statement.ConnPool.ExecContext(db.Statement.Context, db.Statement.SQL.String(), db.Statement.Vars...)
				if db.AddError(err) == nil {
					db.RowsAffected, _ = result.RowsAffected()
					if needReturnId {
						insertId, _ := result.LastInsertId()
						if insertId <= 0 {
							var identity sql.NullInt64
							if err = db.Statement.ConnPool.QueryRowContext(db.Statement.Context, "SELECT @@IDENTITY").Scan(&identity); err == nil {
								if identity.Valid {
									insertId = identity.Int64
								}
							}
						}
						switch db.Statement.ReflectValue.Kind() {
						case reflect.Struct:
							db.AddError(db.Statement.Schema.PrioritizedPrimaryField.Set(db.Statement.Context, db.Statement.ReflectValue, insertId))
						case reflect.Slice, reflect.Array:
							for i := db.Statement.ReflectValue.Len() - 1; i >= 0; i-- {
								rv := db.Statement.ReflectValue.Index(i)
								if reflect.Indirect(rv).Kind() != reflect.Struct {
									break
								}

								_, isZero := db.Statement.Schema.PrioritizedPrimaryField.ValueOf(db.Statement.Context, rv)
								if isZero {
									db.AddError(db.Statement.Schema.PrioritizedPrimaryField.Set(db.Statement.Context, rv, insertId))
									insertId -= db.Statement.Schema.PrioritizedPrimaryField.AutoIncrementIncrement
								}
							}
						}
					}
				}
			}
			if db.SkipDefaultTransaction {
				// 保证查询 identity 的时候在同一个 session 里
				db.Connection(func(tx *gorm.DB) error {
					exec(tx)
					return nil
				})
			} else {
				exec(db)
			}
		}
	}
}

func MergeCreate(db *gorm.DB, onConflict clause.OnConflict, values clause.Values) {
	var setIdentityInsert bool
	if field := db.Statement.Schema.PrioritizedPrimaryField; field != nil && field.AutoIncrement {
		switch db.Statement.ReflectValue.Kind() {
		case reflect.Struct:
			_, isZero := field.ValueOf(db.Statement.Context, db.Statement.ReflectValue)
			setIdentityInsert = !isZero
		case reflect.Slice, reflect.Array:
			for i := 0; i < db.Statement.ReflectValue.Len(); i++ {
				obj := db.Statement.ReflectValue.Index(i)
				if reflect.Indirect(obj).Kind() == reflect.Struct {
					_, isZero := field.ValueOf(db.Statement.Context, db.Statement.ReflectValue.Index(i))
					setIdentityInsert = !isZero
					break
				}
			}
		}
	}
	var tableName = db.Statement.Table
	if tableName == "" {
		tableName = db.Statement.TableExpr.SQL
	}
	if setIdentityInsert {
		db.Statement.WriteString("SET IDENTITY_INSERT ")
		db.Statement.WriteQuoted(tableName)
		db.Statement.WriteString(" ON;")
	}
	db.Statement.WriteString("MERGE INTO ")
	db.Statement.WriteQuoted(db.Statement.Table)
	db.Statement.WriteString(" USING (")
	for index, vals := range values.Values {
		if index > 0 {
			db.Statement.WriteString(" UNION ALL ")
		}
		db.Statement.WriteString("SELECT ")
		for idx, v := range vals {
			if idx > 0 {
				db.Statement.WriteByte(',')
			}
			db.Statement.AddVar(db.Statement, v)
			db.Statement.WriteString(" AS ")
			db.Statement.WriteQuoted(values.Columns[idx].Name)
		}
		db.Statement.WriteString(" FROM DUAL")
	}
	db.Statement.WriteString(") AS excluded ON ( ")

	var where clause.Where
	if len(onConflict.Columns) > 0 {
		for _, col := range onConflict.Columns {
			where.Exprs = append(where.Exprs, clause.Eq{
				Column: clause.Column{Table: db.Statement.Table, Name: col.Name},
				Value:  clause.Column{Table: "excluded", Name: col.Name},
			})
		}
	} else {
		for _, field := range db.Statement.Schema.PrimaryFields {
			where.Exprs = append(where.Exprs, clause.Eq{
				Column: clause.Column{Table: db.Statement.Table, Name: field.DBName},
				Value:  clause.Column{Table: "excluded", Name: field.DBName},
			})
		}
	}
	where.Build(db.Statement)
	db.Statement.WriteString(" )")

	if len(onConflict.DoUpdates) > 0 {
		db.Statement.WriteString(" WHEN MATCHED THEN UPDATE SET ")
		// 添加 table
		// col + 1 => table.col + 1
		for i := range onConflict.DoUpdates {
			switch v := onConflict.DoUpdates[i].Value.(type) {
			case clause.Expr:
				stmt, err := sqlparser.Parse(fmt.Sprintf("select %s from dual", v.SQL))
				if err != nil {
					db.AddError(fmt.Errorf("parse sql error: %w", err))
					return
				}
				switch stmt := stmt.(type) {
				case *sqlparser.Select:
					builder := NewBuilder()
					stmt.SelectExprs.WalkSubtree(func(node sqlparser.SQLNode) (kontinue bool, err error) {
						if col, ok := node.(*sqlparser.ColName); ok {
							col.Qualifier.Name = sqlparser.NewTableIdent(db.Statement.Table)
						}
						return true, nil
					})
					builder.formatSelectExprs(stmt.SelectExprs)
					v.SQL = builder.String()
					onConflict.DoUpdates[i].Value = v
				}
			}
		}
		onConflict.DoUpdates.Build(db.Statement)
	}

	db.Statement.WriteString(" WHEN NOT MATCHED THEN INSERT (")

	written := false

	for _, column := range values.Columns {
		if db.Statement.Schema.PrioritizedPrimaryField == nil || !db.Statement.Schema.PrioritizedPrimaryField.AutoIncrement || db.Statement.Schema.PrioritizedPrimaryField.DBName != column.Name || setIdentityInsert {
			if written {
				db.Statement.WriteByte(',')
			}
			written = true
			db.Statement.WriteQuoted(column.Name)
		}
	}

	db.Statement.WriteString(") VALUES (")

	written = false
	for _, column := range values.Columns {
		if db.Statement.Schema.PrioritizedPrimaryField == nil || !db.Statement.Schema.PrioritizedPrimaryField.AutoIncrement || db.Statement.Schema.PrioritizedPrimaryField.DBName != column.Name || setIdentityInsert {
			if written {
				db.Statement.WriteByte(',')
			}
			written = true
			db.Statement.WriteQuoted(clause.Column{
				Table: "excluded",
				Name:  column.Name,
			})
		}
	}

	db.Statement.WriteString(")")
	db.Statement.WriteString(";")
	if setIdentityInsert {
		db.Statement.WriteString("SET IDENTITY_INSERT ")
		db.Statement.WriteQuoted(tableName)
		db.Statement.WriteString(" OFF;")
	}
}
