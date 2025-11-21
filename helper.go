package gormdm

import (
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
)

func RawColumn(str string) (string, bool) {
	if HasNumberPrefix(str) {
		return "\"" + strings.ToUpper(str) + "\"", true
	}
	return str, false
}

func ConvertToCreateValuesWithBackQuote(stmt *gorm.Statement) (values clause.Values) {
	values = callbacks.ConvertToCreateValues(stmt)
	for idx, column := range values.Columns {
		values.Columns[idx].Name, values.Columns[idx].Raw = RawColumn(column.Name)
	}
	return values
}
