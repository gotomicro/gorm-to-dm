package gormdm

const (
	IdentityInsertKey    = "gorm2dm:identity_insert"
	OnConflictColumnsKey = "gorm2dm:on_conflict_columns"
	SkipParseKey         = "gorm2dm:skip_parse"
)

func SetIdentityInsert() (string, bool) {
	return IdentityInsertKey, true
}

func SetOnConflictColumns(cols ...string) (string, []string) {
	return OnConflictColumnsKey, cols
}

func SetSkipParse() (string, bool) {
	return SkipParseKey, true
}
