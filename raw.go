package gormdm

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/blastrain/vitess-sqlparser/tidbparser/ast"
	"github.com/blastrain/vitess-sqlparser/tidbparser/dependency/mysql"
	"github.com/blastrain/vitess-sqlparser/tidbparser/parser"
	"github.com/spf13/cast"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

var (
	// 匹配 `` 之间的内容
	regBackQuote = regexp.MustCompile("`.*?`")
	regBitValue  = regexp.MustCompile(`(b|B|_binary)\s?'\\?(\d|)'`)
	// regBitValue     = regexp.MustCompile("_binary '\u0001'")
	regMysqlColType = regexp.MustCompile("(\\w+)(\\((\\d+)\\))?( unsigned)?( zerofill)?")
	// 注释行 以 -- 开头 或 注释块 以 /* 开头，以 */ 结尾
	// regComment = regexp.MustCompile("(--.*\n)|(/\\*.*\\*/);")
	regComment = regexp.MustCompile("(--.*\n);?")
	// lock unlock
	regLock = regexp.MustCompile("(LOCK|lock|UNLOCK|unlock)\\s+(TABLES|tables)\\s+(.*);")
)

type doUpdates int

var (
	doUpdatesColumns doUpdates = 1
	doUpdatesAll     doUpdates = 2
	doUpdatesNone    doUpdates = 3
)

type BuildOption struct {
	gormDB              *gorm.DB
	forceIdentityInsert bool
	onConflictColumns   map[string][]string
}

func Mysql2DM(db *gorm.DB) {
	if v, ok := db.Get("gorm2dm:skip_parse"); ok {
		if _, ok := v.(bool); ok {
			return
		}
	}
	// json.RawMessage -> string
	for idx, arg := range db.Statement.Vars {
		if raw, ok := arg.(json.RawMessage); ok {
			db.Statement.Vars[idx] = string(raw)
		}
	}
	stmt := db.Statement
	if stmt.SQL.String() == "" {
		parseGormSTMT(stmt, true)
	} else {
		outputSql, err := NewBuilder(WithGormDB(db)).Parse(stmt.SQL.String())
		if db.AddError(err) != nil {
			return
		}
		stmt.SQL.Reset()
		stmt.SQL.WriteString(outputSql)
	}
}

func parseGormExpression(expr clause.Expression) clause.Expression {
	switch expression := expr.(type) {
	case clause.Expr:
		expression.SQL = removeBackQuote(expression.SQL, true)
		return expression
	case clause.AndConditions:
		for idx := range expression.Exprs {
			expression.Exprs[idx] = parseGormExpression(expression.Exprs[idx])
		}
		return expression
	case clause.OrConditions:
		for idx := range expression.Exprs {
			expression.Exprs[idx] = parseGormExpression(expression.Exprs[idx])
		}
		return expression
	case clause.Where:
		for idx := range expression.Exprs {
			expression.Exprs[idx] = parseGormExpression(expression.Exprs[idx])
		}
		return expression
	case clause.OrderBy:
		for idx := range expression.Columns {
			expression.Columns[idx].Column.Name = removeBackQuote(expression.Columns[idx].Column.Name, true)
		}
		return expression
	case clause.GroupBy:
		for idx := range expression.Columns {
			expression.Columns[idx].Name = removeBackQuote(expression.Columns[idx].Name, true)
		}
		return expression
	default:
		return expression
	}
}

func removeBackQuote(str string, toUpper bool) string {
	if str == clause.PrimaryKey || str == clause.CurrentTable || str == clause.Associations {
		return str
	}
	if toUpper {
		str = strings.ToUpper(str)
	}
	str = regBackQuote.ReplaceAllStringFunc(str, func(s string) string {
		s = strings.Trim(s, "`")
		var replacement string
		if IsReservedWord(s) {
			replacement = "\""
		}
		return fmt.Sprintf("%s%s%s", replacement, strings.Trim(s, "`"), replacement)
	})
	if IsReservedWord(str) {
		return "\"" + str + "\""
	}
	return str
}

func parseGormSTMT(stmt *gorm.Statement, forceDBNameToUpper bool) {
	// DM 24 版本
	if stmt.Schema != nil && forceDBNameToUpper {
		for _, f := range stmt.Schema.Fields {
			f.DBName = strings.ToUpper(f.DBName)
		}
		for _, f := range stmt.Schema.PrimaryFields {
			f.DBName = strings.ToUpper(f.DBName)
		}
		for _, f := range stmt.Schema.FieldsWithDefaultDBValue {
			f.DBName = strings.ToUpper(f.DBName)
		}
		var newFieldsByDBName = make(map[string]*schema.Field, len(stmt.Schema.FieldsByDBName))
		for key, f := range stmt.Schema.FieldsByDBName {
			f.DBName = strings.ToUpper(f.DBName)
			newFieldsByDBName[strings.ToUpper(key)] = f
		}
		stmt.Schema.FieldsByDBName = newFieldsByDBName
		var newDBNames = make([]string, len(stmt.Schema.DBNames))
		for idx, name := range stmt.Schema.DBNames {
			newDBNames[idx] = strings.ToUpper(name)
		}
		stmt.Schema.DBNames = newDBNames
		if stmt.Schema.PrioritizedPrimaryField != nil {
			stmt.Schema.PrioritizedPrimaryField.DBName = strings.ToUpper(stmt.Schema.PrioritizedPrimaryField.DBName)
		}
	}

	// 主要是为了去除各种反引号
	clauses := stmt.Clauses

	if stmt.TableExpr != nil && stmt.TableExpr.SQL != "" {
		stmt.TableExpr.SQL = removeBackQuote(stmt.TableExpr.SQL, true)
	}
	for idx := range stmt.Selects {
		stmt.Selects[idx] = removeBackQuote(stmt.Selects[idx], true)
	}
	// dest
	switch dest := stmt.Dest.(type) {
	case map[string]interface{}:
		// FixMe 如果传的 map 里 key 是 struct 的 field 名，这里变成大写后会有问题。应该先尝试查找下是否在 dbNames 或 fieldNames 里？
		// 1. 去除 Key 里的反引号
		// 2. 将 key 转成大写，避免后续匹配不上 DBName，从而在 updated_at 场景下多添加了 updated_at 字段
		for key, value := range dest {
			newKey := removeBackQuote(key, true)
			if valExpr, ok := value.(clause.Expr); ok {
				valExpr.SQL = removeBackQuote(valExpr.SQL, true)
				value = valExpr
			}
			delete(dest, key)
			dest[newKey] = value
		}
		stmt.Dest = dest
	}

	for key, c := range clauses {
		c.Expression = parseGormExpression(c.Expression)
		clauses[key] = c
	}
}

func NewBuilder(options ...func(*BuildOption)) *Builder {
	var option = &BuildOption{}
	for _, opt := range options {
		opt(option)
	}
	return &Builder{Builder: &strings.Builder{}, schemas: make(map[string]*tableSchema), options: option}
}

func WithGormDB(db *gorm.DB) func(*BuildOption) {
	return func(option *BuildOption) {
		option.gormDB = db
	}
}

func WithForceIdentityInsert() func(*BuildOption) {
	return func(option *BuildOption) {
		option.forceIdentityInsert = true
	}
}

func WithOnConflictColumns(onConflictColumns map[string][]string) func(option *BuildOption) {
	return func(option *BuildOption) {
		option.onConflictColumns = make(map[string][]string, len(onConflictColumns))
		for table, columns := range onConflictColumns {
			table = strings.ToUpper(table)
			option.onConflictColumns[table] = columns
		}
	}
}

func (builder Builder) Parse(SQL string) (outputSQL string, err error) {
	// 过滤注释
	SQL = regComment.ReplaceAllString(SQL, "")
	// 过滤 lock unlock
	SQL = regLock.ReplaceAllString(SQL, "")
	var sqls []string
	// 如果可能是多条语句，先用 tidb parser 解析出来
	if strings.Contains(strings.TrimSuffix(SQL, ";"), ";") {
		// 先用 tidb parser 解析
		stmtList, err := parser.New().Parse(SQL, mysql.UTF8MB4Charset, mysql.UTF8DefaultCollation)
		if err != nil {
			return "", err
		}
		for _, stmt := range stmtList {
			// 忽略 set
			if _, ok := stmt.(*ast.SetStmt); ok {
				continue
			}
			sqls = append(sqls, stmt.Text())
		}
	} else {
		sqls = append(sqls, SQL)
	}
	for _, SQL := range sqls {
		err := builder.parseOneSql(SQL)
		if err != nil {
			return "", err
		}
		outputSQL = strings.TrimSpace(builder.String())
		builder.Reset()
		builder.WriteString(outputSQL)
		if !strings.HasSuffix(outputSQL, ";") {
			builder.WriteString(";\n\n")
		} else {
			builder.WriteString("\n\n")
		}
	}
	return strings.TrimSpace(builder.String()), nil
}

type Builder struct {
	*strings.Builder
	schemas map[string]*tableSchema
	options *BuildOption
}

type tableSchema struct {
	Columns      []*sqlparser.ColumnDef
	Constraints  []*sqlparser.Constraint
	Options      []*sqlparser.TableOption
	AutoIncrCols []*sqlparser.ColumnDef
}

func (builder Builder) parseOneSql(SQL string) (err error) {
	// 替换 b'1' 之类的 bit 值
	matches := regBitValue.FindAllStringSubmatch(SQL, -1)
	for _, match := range matches {
		if match[2] == "\u0001" {
			match[2] = "1"
		}
		SQL = strings.ReplaceAll(SQL, match[0], match[2])
	}
	stmt, err := sqlparser.Parse(SQL)
	if err != nil {
		return fmt.Errorf("parse sql error: %w， rawSql: %s", err, SQL)
	}
	switch stmtValue := stmt.(type) {
	case *sqlparser.Select:
		builder.formatSelect(stmtValue)
		return nil
	case *sqlparser.Insert:
		// insert ignore 转换成 merge into ，需要指定 conflict columns
		if stmtValue.Ignore != "" {
			if len(builder.options.onConflictColumns) == 0 {
				if builder.options.gormDB == nil || func() bool { _, b := builder.options.gormDB.Get("gorm2dm:on_conflict_columns"); return !b }() {
					return fmt.Errorf("on_conflict_columns should be set in `insert ignore` stmt, sql: %s", SQL)
				}
			}
		}
		// on duplicate 转换成 merge into ，需要指定 conflict columns
		if stmtValue.OnDup != nil {
			if len(builder.options.onConflictColumns) == 0 {
				if builder.options.gormDB == nil || func() bool { _, b := builder.options.gormDB.Get("gorm2dm:on_conflict_columns"); return !b }() {
					return fmt.Errorf("on_conflict_columns should be set in `on dup` stmt, sql: %s", SQL)
				}
			}
		}
		// replace into 转换成 merge into ，需要指定 conflict columns
		if strings.ToLower(stmtValue.Action) == "replace" {
			if len(builder.options.onConflictColumns) == 0 {
				if builder.options.gormDB == nil || func() bool { _, b := builder.options.gormDB.Get("gorm2dm:on_conflict_columns"); return !b }() {
					return fmt.Errorf("on_conflict_columns should be set in `replace into` stmt, sql: %s", SQL)
				}
			}
		}
		var identityInsert bool
		if s := builder.schemas[sqlparser.String(stmtValue.Table)]; s != nil {
			// 检查有没有对自增字段赋值
			for _, autoIncrCol := range s.AutoIncrCols {
				for _, insertCol := range stmtValue.Columns {
					if strings.EqualFold(autoIncrCol.Name, insertCol.String()) {
						identityInsert = true
						break
					}
				}
				if identityInsert {
					break
				}
			}
		}
		if builder.options.gormDB != nil {
			if v, ok := builder.options.gormDB.Get(IdentityInsertKey); ok {
				identityInsert = v.(bool)
			}
		}
		if builder.options.forceIdentityInsert {
			identityInsert = true
		}
		if identityInsert {
			builder.WriteString("set identity_insert ")
			builder.formatTableName(stmtValue.Table)
			builder.WriteString(" on;\n")
		}
		builder.formatInsert(stmtValue)
		if identityInsert {
			builder.WriteString(";\nset identity_insert ")
			builder.formatTableName(stmtValue.Table)
			builder.WriteString(" off;\n")
		}
		return nil
	case *sqlparser.Update:
		builder.formatUpdate(stmtValue)
		return nil
	case *sqlparser.Delete:
		builder.formatDelete(stmtValue)
		return nil
	case *sqlparser.DDL:
		switch stmtValue.Action {
		case sqlparser.DropStr:
			exists := ""
			if stmtValue.IfExists {
				exists = " if exists"
			}
			builder.WriteString(stmtValue.Action)
			builder.WriteString(" table")
			builder.WriteString(exists)
			builder.WriteString(" ")
			builder.formatTableName(stmtValue.Table)
		case sqlparser.RenameStr:
			builder.WriteString(stmtValue.Action)
			builder.WriteString(" table")
			builder.WriteString(" ")
			builder.formatTableName(stmtValue.Table)
			builder.WriteString(" ")
			builder.formatTableName(stmtValue.NewName)
		default:
			builder.WriteString(stmtValue.Action)
			builder.WriteString(" table")
			builder.WriteString(" ")
			builder.formatTableName(stmtValue.Table)
		}
		return nil
	case *sqlparser.CreateTable:
		var tableSchema = &tableSchema{}
		tableSchema.Columns = stmtValue.Columns
		tableSchema.Constraints = stmtValue.Constraints
		tableSchema.Options = stmtValue.Options

		builder.WriteString("create table ")
		builder.formatTableName(stmtValue.NewName)
		builder.WriteString(" (\n")
		var (
			onUpdates       [][2]string
			defaultIncrVale uint64
		)
		for _, option := range stmtValue.Options {
			if option.Type == sqlparser.TableOptionAutoIncrement {
				defaultIncrVale = option.UintValue
				break
			}
		}
		for i, col := range stmtValue.Columns {
			var (
				autoIncr bool
				notNull  bool
				def      string
				priKey   bool
			)
			for _, option := range col.Options {
				switch option.Type {
				case sqlparser.ColumnOptionAutoIncrement:
					autoIncr = true
					tableSchema.AutoIncrCols = append(tableSchema.AutoIncrCols, col)
				case sqlparser.ColumnOptionNotNull:
					notNull = true
				case sqlparser.ColumnOptionDefaultValue:
					def = option.Value
				case sqlparser.ColumnOptionPrimaryKey:
					priKey = true
				case sqlparser.ColumnOptionOnUpdate:
					onUpdates = append(onUpdates, [2]string{col.Name, option.Value})
				case sqlparser.ColumnOptionComment:
					continue
				case sqlparser.ColumnOptionNull:
					continue
				default:
					return fmt.Errorf("not support sql column option type: %s", option.Type)
				}
			}
			if IsReservedWord(col.Name) {
				builder.WriteString("\"")
				builder.WriteString(strings.ToUpper(col.Name))
				builder.WriteString("\"")
			} else {
				builder.WriteString(strings.ToUpper(col.Name))
			}
			builder.WriteString(" ")
			colType := mysqlTypeToDmType(stmtValue.NewName.Name.String(), strings.ToUpper(col.Name), autoIncr, col.Type)
			builder.WriteString(colType)

			if autoIncr {
				autoIncrStr := fmt.Sprintf(" identity(%d,1)", defaultIncrVale+1)
				builder.WriteString(autoIncrStr)
			}
			if def != "" {
				if strings.HasPrefix(colType, "timestamp") && strings.HasPrefix(def, "\"0000") {
					builder.WriteString(" default 1")
				} else {
					builder.WriteString(" default ")
					builder.WriteString(strings.ReplaceAll(def, "\"", "'"))
				}
			}
			if notNull {
				builder.WriteString(" not null")
			}
			if priKey {
				builder.WriteString(" primary key")
			}
			if i != len(stmtValue.Columns)-1 {
				builder.WriteString(",\n")
			}
		}
		builder.WriteString(");\n\n")
		// index
		var indexMap = make(map[string]struct{})
		var checkIndexKeysExists = func(keys []sqlparser.ColIdent) bool {
			var keysArr []string
			for _, key := range keys {
				keysArr = append(keysArr, key.String())
			}
			slices.Sort(keysArr)
			key := strings.Join(keysArr, ",")
			if _, ok := indexMap[key]; ok {
				return true
			}
			indexMap[key] = struct{}{}
			return false
		}
		for _, constraint := range stmtValue.Constraints {
			switch constraint.Type {
			case sqlparser.ConstraintPrimaryKey:
				if checkIndexKeysExists(constraint.Keys) {
					continue
				}
				builder.WriteString("alter table ")
				builder.formatTableName(stmtValue.NewName)
				builder.WriteString(" add constraint ")
				builder.WriteString(stmtValue.NewName.Name.String())
				builder.WriteString("_pk")
				if constraint.Name != "" {
					builder.WriteString("_")
					builder.WriteString(strings.ReplaceAll(constraint.Name, "-", "_"))
				}
				builder.WriteString(" primary key (")
				for i, key := range constraint.Keys {
					builder.formatColIdent(key)
					if i != len(constraint.Keys)-1 {
						builder.WriteString(",")
					}
				}
				builder.WriteString(");\n\n")
			case sqlparser.ConstraintIndex:
				if checkIndexKeysExists(constraint.Keys) {
					continue
				}
				builder.WriteString("create index ")
				builder.WriteString(stmtValue.NewName.Name.String())
				builder.WriteString("_")
				builder.WriteString(strings.ReplaceAll(constraint.Name, "-", "_"))
				builder.WriteString(" on ")
				builder.formatTableName(stmtValue.NewName)
				builder.WriteString("(")
				for i, key := range constraint.Keys {
					builder.formatColIdent(key)
					if i != len(constraint.Keys)-1 {
						builder.WriteString(",")
					}
				}
				builder.WriteString(");\n\n")
			case sqlparser.ConstraintUniq:
				if checkIndexKeysExists(constraint.Keys) {
					continue
				}
				builder.WriteString("create unique index ")
				builder.WriteString(stmtValue.NewName.Name.String())
				builder.WriteString("_")
				builder.WriteString(strings.ReplaceAll(constraint.Name, "-", "_"))
				builder.WriteString(" on ")
				builder.formatTableName(stmtValue.NewName)
				builder.WriteString("(")
				for i, key := range constraint.Keys {
					builder.formatColIdent(key)
					if i != len(constraint.Keys)-1 {
						builder.WriteString(",")
					}
				}
				builder.WriteString(");\n\n")
			case sqlparser.ConstraintForeignKey:
				// builder.WriteString("alter table ")
				// builder.formatTableName(stmtValue.NewName)
				// builder.WriteString(" add constraint ")
				// builder.WriteString(stmtValue.NewName.Name.String())
				// builder.WriteString("_foreign")
				// if constraint.Name != "" {
				// 	builder.WriteString("_")
				// 	builder.WriteString(strings.ReplaceAll(constraint.Name, "-", "_"))
				// }
				// builder.WriteString(" foreign key (")
				// for i, key := range constraint.Keys {
				// 	builder.formatColIdent(key)
				// 	if i != len(constraint.Keys)-1 {
				// 		builder.WriteString(",")
				// 	}
				// }
				// builder.WriteString(") references ")
				// // FIXME references 获取不到
				// builder.WriteString(");\n")
			default:
				return fmt.Errorf("not support sql constraint type: %s", constraint.Type)
			}
		}
		// trigger

		// create trigger tgr_utime
		// before update on test_trigger
		// for each row
		// begin
		// :new.u_time=current_timestamp();
		// end;
		for _, onUpdate := range onUpdates {
			col, update := onUpdate[0], onUpdate[1]
			builder.WriteString("create trigger tgr_")
			builder.WriteString(stmtValue.NewName.Name.String())
			builder.WriteString("_")
			builder.WriteString(col)
			builder.WriteString("\nbefore update on ")
			builder.WriteString(sqlparser.String(stmtValue.NewName))
			builder.WriteString("\nfor each row\nbegin\n")
			builder.WriteString("new.")
			if IsReservedWord(col) {
				builder.WriteString("\"")
				builder.WriteString(strings.ToUpper(col))
				builder.WriteString("\"")
			} else {
				builder.WriteString(strings.ToUpper(col))
			}
			builder.WriteString("=")
			builder.WriteString(update)
			builder.WriteString(";\n")
			builder.WriteString("end;\n\n")
		}

		// create trigger AUTO_INCREMENT=value
		/**
		--创建 BEFORE 触发器
		CREATE OR REPLACE TRIGGER TRG_INS_BEFORE
		BEFORE INSERT ON SVC_FILE."FILE"
		FOR EACH ROW
		BEGIN
		:NEW.ID:=:NEW.ID+5000012755;
		END;
		*/
		// for _, option := range tableSchema.Options {
		// 	if option.Type == sqlparser.TableOptionAutoIncrement {
		// 		builder.WriteString("CREATE OR REPLACE TRIGGER TRG_INS_BEFORE\n")
		// 		builder.WriteString("BEFORE INSERT ON ")
		// 		builder.formatTableName(stmtValue.NewName)
		// 		builder.WriteString("\nFOR EACH ROW\n")
		// 		builder.WriteString("BEGIN\n")
		// 		builder.WriteString(fmt.Sprintf(":NEW.ID:=:NEW.ID+%d;\n", option.UintValue))
		// 		builder.WriteString("END;\n\n")
		// 		break
		// 	}
		// }

		builder.schemas[sqlparser.String(stmtValue.NewName)] = tableSchema
		return nil
	case *sqlparser.Use:
		builder.WriteString("set schema ")
		builder.WriteString(stmtValue.DBName.String())
		return nil
	default:
		// err = fmt.Errorf("not support sql type: %T", stmtValue)
		builder.WriteString(SQL)
		return err
	}
}

func mysqlTypeToDmType(tableName, colName string, autoIncr bool, colType string) string {
	// - 数字类型不支持宽度
	// - 数字类型不支持 unsigned
	matches := regMysqlColType.FindAllStringSubmatch(strings.ToLower(colType), -1)
	var (
		ctype string
		size  string
	)
	if len(matches) > 0 {
		ctype = matches[0][1]
		if len(matches[0]) > 3 {
			size = matches[0][3]
		}
	}
	var unsigned bool
	if len(matches[0]) > 4 {
		unsigned = matches[0][4] != ""
	}
	switch strings.ToLower(ctype) {
	case "bigint":
		if unsigned {
			// unsigned  -> numeric(20,0)
			if autoIncr {
				// only bigint and int can be auto increment
				return "bigint"
			}
			return "numeric(20,0)"
		}
		return "bigint"
	case "int":
		if unsigned {
			// unsigned  -> numeric(10,0)
			if autoIncr {
				// only bigint and int can be auto increment
				return "bigint"
			}
			return "numeric(10,0)"
		}
		return "int"
	case "mediumint":
		if unsigned {
			// unsigned  -> numeric(7,0)
			if autoIncr {
				// only bigint and int can be auto increment
				return "int"
			}
			return "numeric(7,0)"
		}
		return "mediumint"
	case "smallint":
		if unsigned {
			// unsigned  -> numeric(5,0)
			if autoIncr {
				// only bigint and int can be auto increment
				return "int"
			}
			return "numeric(5,0)"
		}
		return "smallint"
	case "tinyint":
		if unsigned {
			// unsigned  -> numeric(3,0)
			if autoIncr {
				// only bigint and int can be auto increment
				return "int"
			}
			return "numeric(3,0)"
		}
		return "tinyint"
	case "decimal":
		return "decimal"
	case "float":
		return "float"
	case "double":
		return "double"
	case "bit":
		return "bit"
	case "datetime":
		return "timestamp(0)"
	case "year":
		return "varchar(10)"
	case "enum":
		return "varchar(128)"
	case "longtext":
		return "CLOB"
	case "mediumtext":
		return "TEXT"
	case "json":
		return fmt.Sprintf("varchar CONSTRAINT JSON_CHECK_%s_%s CHECK(%s IS JSON)", tableName, colName, colName)
	case "varchar":
		size := cast.ToInt(size)
		if size > 8188 {
			return "text"
		}
		return fmt.Sprintf("varchar(%d)", size)
	case "char":
		// 由于达梦会在 char 类型的数据后面自动填充空格以达到给定长度，这里直接将 char 转换成 varchar，避免业务上出现异常
		size := cast.ToInt(size)
		return fmt.Sprintf("varchar2(%d)", size)
	default:
		return colType
	}
}

func (builder Builder) formatDelete(delete *sqlparser.Delete) {
	builder.WriteString("delete ")
	// if delete.Targets != nil {
	// 	builder.formatTableNames(delete.Targets)
	// 	builder.WriteString(" ")
	// }
	builder.WriteString("from ")
	builder.formatTableExprs(delete.TableExprs)
	builder.formatWhere(delete.Where)
	builder.formatOrderBy(delete.OrderBy)
	builder.formatLimit(delete.Limit)
}

func (builder Builder) formatUpdate(update *sqlparser.Update) {
	builder.WriteString("update ")
	builder.formatTableExprs(update.TableExprs)
	builder.WriteString(" set ")
	builder.formatUpdateExprs(update.Exprs)
	builder.formatWhere(update.Where)
	builder.formatOrderBy(update.OrderBy)
	builder.formatLimit(update.Limit)

}

func (builder Builder) formatUpdateExprs(exprs sqlparser.UpdateExprs) {
	if len(exprs) == 0 {
		return
	}
	for idx, expr := range exprs {
		builder.formatUpdateExpr(expr)
		if idx != len(exprs)-1 {
			builder.WriteString(", ")
		}
	}
}

func (builder Builder) formatUpdateExpr(expr *sqlparser.UpdateExpr) {
	builder.formatColName(expr.Name)
	builder.WriteString(" = ")
	builder.formatExpr(expr.Expr)
}

func (builder Builder) formatInsert(insert *sqlparser.Insert) {
	if insert.OnDup != nil {
		builder.formatInsertOnDup(insert, doUpdatesColumns)
		return
	}
	if insert.Ignore != "" {
		builder.formatInsertOnDup(insert, doUpdatesNone)
		return
	}
	if strings.ToLower(insert.Action) == "replace" {
		builder.formatInsertOnDup(insert, doUpdatesAll)
		return
	}
	builder.WriteString(insert.Action)
	builder.WriteString(" ")
	builder.WriteString("into ")
	builder.formatTableName(insert.Table)
	builder.WriteString(" ")
	builder.formatColumns(insert.Columns)
	builder.WriteString(" ")
	builder.formatInsertRows(insert.Rows)
}

func (builder Builder) formatInsertOnDup(insert *sqlparser.Insert, doUpdates doUpdates) {
	var getOnConflictCols = func() []string {
		if len(builder.options.onConflictColumns) > 0 {
			if cols, ok := builder.options.onConflictColumns[strings.ToUpper(sqlparser.String(insert.Table))]; ok {
				return cols
			}
		}
		if builder.options.gormDB != nil {
			cols, ok := builder.options.gormDB.Get("gorm2dm:on_conflict_columns")
			if ok {
				return cols.([]string)
			}
		}
		return nil
	}
	cols := getOnConflictCols()
	if len(cols) == 0 {
		return
	}
	var onConflictCols []sqlparser.ColIdent
	for _, col := range cols {
		onConflictCols = append(onConflictCols, sqlparser.NewColIdent(col))
	}
	builder.WriteString("merge into ")
	builder.formatTableName(insert.Table)
	builder.WriteString(" using ")
	builder.WriteString("(")
	values := insert.Rows.(sqlparser.Values)
	for idx, row := range values {
		builder.WriteString("select ")
		for i, col := range row {
			builder.formatExpr(col)
			builder.WriteString(" as ")
			builder.formatColIdent(insert.Columns[i])
			if i != len(row)-1 {
				builder.WriteString(", ")
			}
		}
		builder.WriteString(" from dual")
		if idx != len(values)-1 {
			builder.WriteString(" union all ")
		}
	}
	builder.WriteString(") as excluded on (")
	var onConflictColMap = make(map[string]struct{})
	for idx, col := range onConflictCols {
		onConflictColMap[col.String()] = struct{}{}
		builder.formatTableName(insert.Table)
		builder.WriteString(".")
		builder.formatColIdent(col)
		builder.WriteString(" = excluded.")
		builder.formatColIdent(col)
		if idx != len(onConflictCols)-1 {
			builder.WriteString(" and ")
		}
	}
	builder.WriteString(")")
	switch doUpdates {
	case doUpdatesNone:
	// do not update when matched
	case doUpdatesColumns:
		builder.WriteString(" when matched then update set ")
		// add table for function expr
		// e.g. insert into test (id, name) values (1, 'test') on duplicate key update name = concat(name, '1') -> on duplicate key update name = concat(test.name, '1')
		for _, col := range insert.OnDup {
			if funcExpr, ok := col.Expr.(*sqlparser.FuncExpr); ok {
				for _, expr := range funcExpr.Exprs {
					if aliased, ok := expr.(*sqlparser.AliasedExpr); ok {
						if colName, ok := aliased.Expr.(*sqlparser.ColName); ok {
							colName.Qualifier = insert.Table
						}
						if comparison, ok := aliased.Expr.(*sqlparser.ComparisonExpr); ok {
							if colName, ok := comparison.Left.(*sqlparser.ColName); ok {
								colName.Qualifier = insert.Table
							}
							if colName, ok := comparison.Right.(*sqlparser.ColName); ok {
								colName.Qualifier = insert.Table
							}
						}
					}
				}
			}
		}
		builder.formatUpdateExprs(sqlparser.UpdateExprs(insert.OnDup))
	case doUpdatesAll:
		// update all when matched
		builder.WriteString(" when matched then update set ")
		for idx, c := range insert.Columns {
			if _, ok := onConflictColMap[c.String()]; ok {
				continue
			}
			builder.formatColIdent(c)
			builder.WriteString(" = ")
			builder.WriteString("excluded.")
			builder.formatColIdent(c)
			if idx != len(insert.Columns)-1 {
				builder.WriteString(", ")
			}
		}
	}
	builder.WriteString(" when not matched then insert ")
	builder.WriteString(" ")
	builder.formatColumns(insert.Columns)
	builder.WriteString(" ")
	builder.WriteString("values (")
	for idx, col := range insert.Columns {
		builder.WriteString("excluded.")
		builder.formatColIdent(col)
		if idx != len(insert.Columns)-1 {
			builder.WriteString(", ")
		}
	}
	builder.WriteString(")")
}

func (builder Builder) formatInsertRows(expr sqlparser.InsertRows) {
	switch insertExpr := expr.(type) {
	case *sqlparser.ParenSelect:
		builder.formatParenSelect(insertExpr)
	case *sqlparser.Select:
		builder.formatSelect(insertExpr)
	case *sqlparser.Union:
		builder.formatUnion(insertExpr)
	case sqlparser.SelectStatement:
		builder.formatSelectStatement(insertExpr)
	case sqlparser.Values:
		builder.formatValues(insertExpr)
	}
}

func (builder Builder) formatValues(expr sqlparser.Values) {
	builder.WriteString("values ")
	for idx, row := range expr {
		builder.formatValTuple(row)
		if idx != len(expr)-1 {
			builder.WriteString(", ")
		}
	}
}

func (builder Builder) formatValTuple(expr sqlparser.ValTuple) {
	builder.WriteString("(")
	builder.formatExprs(sqlparser.Exprs(expr))
	builder.WriteString(")")
}

func (builder Builder) formatExprs(exprs sqlparser.Exprs) {
	for idx, expr := range exprs {
		builder.formatExpr(expr)
		if idx != len(exprs)-1 {
			builder.WriteString(", ")
		}
	}
}

func (builder Builder) formatColumns(expr sqlparser.Columns) {
	if len(expr) == 0 {
		return
	}
	builder.WriteString("(")
	for idx, column := range expr {
		builder.formatColIdent(column)
		if idx != len(expr)-1 {
			builder.WriteString(", ")
		}
	}
	builder.WriteString(")")
}

func (builder Builder) formatTableNames(tables sqlparser.TableNames) {
	for idx, table := range tables {
		builder.formatTableName(table)
		if idx != len(tables)-1 {
			builder.WriteString(", ")
		}
	}
}

func (builder Builder) formatTableName(table sqlparser.TableName) {
	if table.Qualifier.String() != "" {
		if IsReservedWord(table.Qualifier.String()) {
			builder.WriteString("\"")
			builder.WriteString(strings.ToUpper(table.Qualifier.String()))
			builder.WriteString("\"")
		} else {
			builder.WriteString(strings.ToUpper(table.Qualifier.String()))
		}
		builder.WriteString(".")
	}
	if IsReservedWord(table.Name.String()) {
		builder.WriteString("\"")
		builder.WriteString(strings.ToUpper(table.Name.String()))
		builder.WriteString("\"")
	} else {
		builder.WriteString(strings.ToUpper(table.Name.String()))
	}
}

func (builder Builder) formatTableExpr(table sqlparser.TableExpr) {
	switch tableValue := table.(type) {
	case *sqlparser.AliasedTableExpr:
		switch tableValueExpr := tableValue.Expr.(type) {
		case sqlparser.TableName:
			builder.formatTableName(tableValueExpr)
		case *sqlparser.Subquery:
			builder.formatSubQuery(tableValueExpr)
		}
		if !tableValue.As.IsEmpty() {
			builder.WriteString(" as ")
			builder.formatTableIdent(tableValue.As)
		}
		if tableValue.Hints != nil {
			builder.WriteString(" ")
			builder.WriteString(sqlparser.String(tableValue.Hints))
		}
	case *sqlparser.JoinTableExpr:
		builder.formatTableExpr(tableValue.LeftExpr)
		builder.WriteString(" ")
		builder.WriteString(strings.ToUpper(tableValue.Join))
		builder.WriteString(" ")
		builder.formatTableExpr(tableValue.RightExpr)
		if tableValue.On != nil {
			builder.WriteString(" on ")
			builder.formatExpr(tableValue.On)
		} else {
			builder.WriteString(" on 1 = 1")
		}
	case *sqlparser.ParenTableExpr:
		builder.WriteString("(")
		for idx, expr := range tableValue.Exprs {
			builder.formatTableExpr(expr)
			if idx != len(tableValue.Exprs)-1 {
				builder.WriteString(", ")
			}
		}
		builder.WriteString(")")
	default:
		builder.WriteString(sqlparser.String(table))
	}
	return
}

func (builder Builder) formatExpr(expr sqlparser.Expr) {
	switch exprValue := expr.(type) {
	case *sqlparser.AndExpr:
		builder.formatAndExpr(exprValue)
	case *sqlparser.OrExpr:
		builder.formatOrExpr(exprValue)
	case *sqlparser.ComparisonExpr:
		builder.formatComparisonExpr(exprValue)
	case *sqlparser.BinaryExpr:
		builder.formatBinaryExpr(exprValue)
	case *sqlparser.BoolVal:
		builder.WriteString(sqlparser.String(exprValue))
	case *sqlparser.CaseExpr:
		builder.formatCaseExpr(exprValue)
	case *sqlparser.ColName:
		builder.formatColName(exprValue)
	case *sqlparser.NotExpr:
		builder.formatNot(exprValue)
	case *sqlparser.Subquery:
		builder.formatSubQuery(exprValue)
	case *sqlparser.GroupConcatExpr:
		builder.formatGroupConcat(exprValue)
	case *sqlparser.FuncExpr:
		builder.formatFuncExpr(exprValue)
	case *sqlparser.ParenExpr:
		builder.formatParenExpr(exprValue)
	case *sqlparser.IsExpr:
		builder.formatIsExpr(exprValue)
	case *sqlparser.SQLVal:
		if exprValue.Type == sqlparser.ValArg {
			builder.WriteString("?")
			return
		}
		builder.WriteString(sqlparser.String(expr))
	case sqlparser.ValTuple:
		builder.formatValTuple(exprValue)
	case *sqlparser.ValuesFuncExpr:
		builder.formatValuesFuncExpr(exprValue)
	default:
		builder.WriteString(sqlparser.String(expr))
	}
}

// TODO 待确定
func (builder Builder) formatValuesFuncExpr(expr *sqlparser.ValuesFuncExpr) {
	// values(xxx) -> EXCLUDED.xxx
	builder.WriteString("EXCLUDED.")
	builder.formatColIdent(expr.Name)
}

func (builder Builder) formatIsExpr(expr *sqlparser.IsExpr) {
	if expr == nil {
		return
	}
	builder.formatExpr(expr.Expr)
	builder.WriteString(" ")
	builder.WriteString(expr.Operator)
	return
}

func (builder Builder) formatParenExpr(expr *sqlparser.ParenExpr) {
	if expr == nil {
		return
	}
	builder.WriteString("(")
	builder.formatExpr(expr.Expr)
	builder.WriteString(")")
	return
}

func (builder Builder) formatFuncExpr(expr *sqlparser.FuncExpr) {
	if strings.ToUpper(expr.Name.String()) == "UNIX_TIMESTAMP" {
		if len(expr.Exprs) == 0 {
			expr.Exprs = append(expr.Exprs, &sqlparser.AliasedExpr{
				Expr: &sqlparser.FuncExpr{
					Qualifier: expr.Qualifier,
					Name:      sqlparser.NewColIdent("SYSDATE"),
					Distinct:  false,
					Exprs:     nil,
				},
				As: sqlparser.NewColIdent(""),
			})
		}
	}
	if strings.ToUpper(expr.Name.String()) == "IF" {
		cond := expr.Exprs[0].(*sqlparser.AliasedExpr).Expr
		caseExpr := &sqlparser.CaseExpr{
			Whens: []*sqlparser.When{&sqlparser.When{
				Cond: cond,
				Val:  expr.Exprs[1].(*sqlparser.AliasedExpr).Expr,
			}},
			Else: expr.Exprs[2].(*sqlparser.AliasedExpr).Expr,
		}
		builder.formatCaseExpr(caseExpr)
		return
	}
	if strings.ToUpper(expr.Name.String()) == "LAST_INSERT_ID" {
		builder.WriteString("@@IDENTITY")
		return
	}
	var isCount bool
	if strings.ToUpper(expr.Name.String()) == "COUNT" {
		isCount = true
	}
	if !expr.Qualifier.IsEmpty() {
		builder.formatTableIdent(expr.Qualifier)
	}
	builder.WriteString(expr.Name.String())
	builder.WriteString("(")
	if expr.Distinct {
		builder.WriteString("distinct ")
	}
	var shouldConcatCols = isCount && expr.Distinct && len(expr.Exprs) > 1
	if shouldConcatCols {
		builder.WriteString("CONCAT(")
	}
	builder.formatSelectExprs(expr.Exprs)
	if shouldConcatCols {
		builder.WriteString(")")
	}
	builder.WriteString(")")
	return
}

func (builder Builder) formatGroupConcat(expr *sqlparser.GroupConcatExpr) string {
	// FIXME group contact 达梦好像不支持
	return sqlparser.String(expr)
}

func (builder Builder) formatSubQuery(expr *sqlparser.Subquery) {
	builder.WriteString("(")
	builder.formatSelectStatement(expr.Select)
	builder.WriteString(")")
	return
}

func (builder Builder) formatNot(expr *sqlparser.NotExpr) {
	builder.WriteString("not ")
	builder.formatExpr(expr.Expr)
	return
}

func (builder Builder) formatColName(col *sqlparser.ColName) {
	if !col.Qualifier.IsEmpty() {
		builder.formatTableName(col.Qualifier)
		builder.WriteString(".")
	}
	builder.formatColIdent(col.Name)
	return
}

func (builder Builder) formatColIdent(col sqlparser.ColIdent) {
	if IsReservedWord(col.String()) {
		builder.WriteString("\"")
		builder.WriteString(strings.ToUpper(col.String()))
		builder.WriteString("\"")
	} else {
		builder.WriteString(strings.ToUpper(col.String()))
	}
	return
}

func (builder Builder) formatCaseExpr(expr *sqlparser.CaseExpr) {
	builder.WriteString("case ")
	if expr.Expr != nil {
		builder.formatExpr(expr.Expr)
		builder.WriteString(" ")
	}
	for _, when := range expr.Whens {
		builder.WriteString("when ")
		builder.formatExpr(when.Cond)
		builder.WriteString(" then ")
		builder.formatExpr(when.Val)
	}
	if expr.Else != nil {
		builder.WriteString(" else ")
		builder.formatExpr(expr.Else)
	}
	builder.WriteString(" end")
	return
}

func (builder Builder) formatBinaryExpr(expr *sqlparser.BinaryExpr) {
	builder.formatExpr(expr.Left)
	builder.WriteString(" ")
	builder.WriteString(expr.Operator)
	builder.WriteString(" ")
	builder.formatExpr(expr.Right)
	return
}

func (builder Builder) formatComparisonExpr(expr *sqlparser.ComparisonExpr) {
	builder.formatExpr(expr.Left)
	builder.WriteString(" ")
	builder.WriteString(expr.Operator)
	builder.WriteString(" ")
	builder.formatExpr(expr.Right)
	if expr.Escape != nil {
		builder.WriteString(" escape ")
		builder.formatExpr(expr.Escape)
	}
	return

}

func (builder Builder) formatAndExpr(expr *sqlparser.AndExpr) {
	builder.formatExpr(expr.Left)
	builder.WriteString(" and ")
	builder.formatExpr(expr.Right)
	return
}

func (builder Builder) formatOrExpr(expr *sqlparser.OrExpr) {
	builder.formatExpr(expr.Left)
	builder.WriteString(" or ")
	builder.formatExpr(expr.Right)
	return
}

func (builder Builder) formatTableIdent(table sqlparser.TableIdent) {
	if IsReservedWord(table.String()) {
		builder.WriteString("\"")
		builder.WriteString(strings.ToUpper(table.String()))
		builder.WriteString("\"")
	} else {
		builder.WriteString(strings.ToUpper(table.String()))
	}
}

func (builder Builder) formatSelectStatement(expr sqlparser.SelectStatement) {
	switch selectExpr := expr.(type) {
	case *sqlparser.Select:
		builder.formatSelect(selectExpr)
	case *sqlparser.Union:
		builder.formatUnion(selectExpr)
	case *sqlparser.ParenSelect:
		builder.formatParenSelect(selectExpr)
	}
	return
}

func (builder Builder) formatSelect(expr *sqlparser.Select) {
	builder.WriteString("select ")
	builder.WriteString(expr.Cache)
	builder.WriteString(expr.Distinct)
	builder.WriteString(expr.Hints)
	builder.formatSelectExprs(expr.SelectExprs)
	builder.WriteString(" from ")
	builder.formatTableExprs(expr.From)
	builder.formatWhere(expr.Where)
	builder.formatGroupBy(expr.GroupBy)
	builder.formatWhere(expr.Having)
	builder.formatOrderBy(expr.OrderBy)
	builder.formatLimit(expr.Limit)
	builder.WriteString(expr.Lock)
	return
}

func (builder Builder) formatUnion(expr *sqlparser.Union) {
	builder.formatSelectStatement(expr.Left)
	builder.WriteString(" ")
	builder.WriteString(expr.Type)
	builder.WriteString(" ")
	builder.formatSelectStatement(expr.Right)
	builder.formatOrderBy(expr.OrderBy)
	builder.formatLimit(expr.Limit)
	builder.WriteString(expr.Lock)
	return
}

func (builder Builder) formatParenSelect(expr *sqlparser.ParenSelect) {
	builder.WriteString("(")
	builder.formatSelectStatement(expr.Select)
	builder.WriteString(")")
	return
}

func (builder Builder) formatSelectExprs(expr sqlparser.SelectExprs) {
	for idx, selectExpr := range expr {
		switch selectExprVal := selectExpr.(type) {
		case *sqlparser.AliasedExpr:
			builder.formatAliasedExpr(selectExprVal)
		case *sqlparser.Nextval:
			builder.formatNextExpr(selectExprVal)
		case *sqlparser.StarExpr:
			builder.formatStarExpr(selectExprVal)
		}
		if idx != len(expr)-1 {
			builder.WriteString(", ")
		}
	}
	return
}

func (builder Builder) formatAliasedExpr(expr *sqlparser.AliasedExpr) {
	builder.formatExpr(expr.Expr)
	if !expr.As.IsEmpty() {
		builder.WriteString(" as ")
		builder.formatColIdent(expr.As)
	}
	return
}

func (builder Builder) formatNextExpr(expr *sqlparser.Nextval) {
	builder.WriteString("next ")
	builder.formatExpr(expr.Expr)
	builder.WriteString("values")
	return
}

func (builder Builder) formatStarExpr(expr *sqlparser.StarExpr) {
	if !expr.TableName.IsEmpty() {
		builder.formatTableName(expr.TableName)
		builder.WriteString(".")
	}
	builder.WriteString("*")
	return
}

func (builder Builder) formatTableExprs(expr sqlparser.TableExprs) {
	for idx, tableExpr := range expr {
		builder.formatTableExpr(tableExpr)
		if idx != len(expr)-1 {
			builder.WriteString(", ")
		}
	}
	return
}

func (builder Builder) formatWhere(expr *sqlparser.Where) {
	if expr == nil || expr.Expr == nil {
		return
	}
	builder.WriteString(" ")
	builder.WriteString(expr.Type)
	builder.WriteString(" ")
	builder.formatExpr(expr.Expr)
	return
}

func (builder Builder) formatGroupBy(expr sqlparser.GroupBy) {
	if len(expr) == 0 {
		return
	}
	builder.WriteString(" group by ")
	for idx, col := range expr {
		builder.formatExpr(col)
		if idx != len(expr)-1 {
			builder.WriteString(", ")
		}
	}
	return
}

func (builder Builder) formatOrderBy(expr sqlparser.OrderBy) {
	if len(expr) == 0 {
		return
	}
	builder.WriteString(" order by ")
	for idx, col := range expr {
		builder.formatOrder(col)
		if idx != len(expr)-1 {
			builder.WriteString(", ")
		}
	}
	return
}

func (builder Builder) formatOrder(expr *sqlparser.Order) {
	if _, ok := expr.Expr.(*sqlparser.NullVal); ok {
		builder.WriteString("null")
		return
	}
	builder.formatExpr(expr.Expr)
	builder.WriteString(" ")
	builder.WriteString(expr.Direction)
	return
}

func (builder Builder) formatLimit(expr *sqlparser.Limit) {
	if expr == nil {
		return
	}
	builder.WriteString(" limit ")
	builder.formatExpr(expr.Rowcount)
	if expr.Offset != nil {
		builder.WriteString(" offset ")
		builder.formatExpr(expr.Offset)
	}
	return
}
