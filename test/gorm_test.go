package test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/stretchr/testify/assert"

	gormdm "github.com/gotomicro/gorm-to-dm"
)

type UserTabComments struct {
	TableName    string `gorm:"column:TABLE_NAME"`    // 表名
	TableComment string `gorm:"column:TABLE_COMMENT"` // 表注释
}

type QuotaDesc struct {
	// id
	ID int32 `json:"id" gorm:"primary_key"`
	// 配额标识
	Quota string `json:"quota"`
	// 数量 单位
	Unit sql.NullString `json:"unit"`
	// quota 说明
	Desc      sql.NullString `json:"desc"`
	CreatedAt time.Time      `json:"created_at"`
	DeletedAt sql.NullTime   `json:"deleted_at"`
}

func (QuotaDesc) TableName() string {
	return "quota_desc"
}

const getMaxFileIDSql = `
SELECT id
FROM audit.audits
ORDER BY id DESC
LIMIT 1
`

func Test_orm_sql_find(t *testing.T) {
	// 1.查询多少张表
	row := EgormDB.WithContext(context.Background()).Raw(getMaxFileIDSql).Row()
	var id int64
	err := row.Scan(&id)
	fmt.Printf("id----->%v\n", id)
	fmt.Printf("err----->%v\n", err)
	// EgormDB.Select("`TABLE_NAME`,`TABLE_COMMENT`").Find(&tables)
	// select TABLE_NAME,comments TABLE_COMMENT from user_tab_comments
	// assert.Equal(t, "##HISTOGRAMS_TABLE", tables[0].TableName)
	// assert.Equal(t, "##PLAN_TABLE", tables[1].TableName)
}

func Test_raw_sql_find(t *testing.T) {
	tables := make([]UserTabComments, 0)
	err := EgormDB.Raw("select `TABLE_NAME`,`COMMENTS as TABLE_COMMENT` from `user_tab_comments`").Scan(&tables).Error
	assert.NoError(t, err)
	// select TABLE_NAME,comments TABLE_COMMENT from user_tab_comments
	assert.Equal(t, "##HISTOGRAMS_TABLE", tables[0].TableName)
	assert.Equal(t, "##PLAN_TABLE", tables[1].TableName)
}

func Test_parse_sql(t *testing.T) {
	sql := "select `TABLE_NAME`,`COMMENTS as TABLE_COMMENT` from `user_tab_comments`"
	stmt, err := sqlparser.Parse(sql)
	fmt.Printf("err--------------->"+"%+v\n", err)
	spew.Dump(stmt)
}

func Test_raw_create_sql(t *testing.T) {
	sql := "insert into QUOTA_DESC (ID, QUOTA, UNIT, `DESC`, CREATED_AT, DELETED_AT) values (1, 'attachment_size_limit', 'byte', '附件大小限制', '2021-05-08 11:32:07', null), (2, 'image_size_limit', 'byte', '图片附件大小限制', '2021-05-08 11:52:26', null), (3, 'batch_download_files', '个', '单次批量下载文件个数', '2021-05-17 16:45:40', null), (4, 'batch_download_size', 'byte', '单次批量下载文件总大小', '2021-05-17 16:46:05', null), (5, 'batch_download_bandwidth', 'byte', '流量范围内不限速', '2021-05-17 16:46:30', null), (6, 'disk_volume', 'GB', '磁盘容量', '2021-05-31 16:31:43', null), (7, 'video_size_limit', 'byte', '视频大小限制', '2021-07-23 11:07:33', null), (8, 'combined_table_per_file', '个', '应用表格：合并工作表每个文件汇总表数量上线', '2021-08-18 16:51:18', null), (9, 'combined_table_external_tables', '个', '应用表格：合并工作表引用 table 数量上限', '2021-08-18 16:51:18', null), (10, 'locked_view_per_table', '个', '应用表格：每个数据表的锁定视图数量', '2021-08-24 14:42:59', null), (11, 'personal_view_per_table', '个', '应用表格：每个数据表的个人视图数量', '2021-08-24 14:42:59', null), (12, 'classic_docs_ole_obj_import', 'byte', '传统文档导入OLE对象', '2021-11-09 14:54:12', null), (13, 'row_count_per_table', '行', '应用表格：行数量', '2021-12-10 15:55:23', null), (14, 'column_count_per_table', '列', '应用表格：列数量', '2021-12-10 15:55:23', null), (15, 'gantt_view_per_file', '个', '应用表格：甘特图数量', '2021-12-10 15:55:23', null), (16, 'latest_row_histories_limit', '条', '应用表格：行动态历史', '2021-12-10 15:55:23', null), (17, 'snapshot_per_file', '个', '应用表格：手动保存的版本数量', '2021-12-10 15:55:23', null), (18, 'latest_cell_histories_limit', '条', '应用表格：单元格历史', '2021-12-14 14:25:38', null), (19, 'calendar_view_per_file', '个', '应用表格：日历视图', '2022-01-05 18:28:55', null), (20, 'create_file_count', '个', '新建文件数限制', '2022-02-14 14:34:20', null), (21, 'file_collaborator_limit', '个', '单个文件协作者条目数量限制', '2022-04-07 18:34:55', null);"
	err := EgormDB.Set(gormdm.SetIdentityInsert()).Exec(sql).Error
	assert.NoError(t, err)
}

func Test_raw_create_sql2(t *testing.T) {
	sql := "insert into user_ff (nickname,created_at,deleted_at) values ('hello',0,0)"
	err := EgormDB.Exec(sql).Error
	assert.NoError(t, err)
}

func Test_raw_create_sql3(t *testing.T) {
	sql := "insert into `user_ff` (`nickname`,`created_at`,`deleted_at`) values (?,?,?)"
	err := EgormDB.Exec(sql, "hello", 2, 3).Error
	assert.NoError(t, err)
}

type UserInfo struct {
	Uid       int64
	Nickname  string
	CreatedAt int
	DeletedAt int
}

func Test_raw_select_sql(t *testing.T) {
	output := UserInfo{}
	sql := "select * from `user_ff` where `nickname`='hello'"
	err := EgormDB.Raw(sql).Scan(&output).Error
	assert.NoError(t, err)
	fmt.Printf("output--------------->"+"%+v\n", output)

}

func Test_raw_select_orderby_rows(t *testing.T) {
	output := make([]UserInfo, 0)
	sql := "select * from `user_ff` where nickname='hello' order by created_at desc"
	rows, err := EgormDB.Raw(sql).Rows()
	assert.NoError(t, err)
	for rows.Next() {
		columns, err := rows.Columns()
		rows.Scan()
		assert.NoError(t, err)
		// UID NICKNAME CREATED_AT DELETED_AT
		fmt.Printf("columns--------------->"+"%+v\n", columns)
	}
	fmt.Printf("output--------------->"+"%+v\n", output)
}

func Test_raw_select_orderby_sql(t *testing.T) {
	output := make([]UserInfo, 0)
	sql := "select * from `user_ff` where nickname='hello' order by created_at desc"
	err := EgormDB.Raw(sql).Scan(&output).Error
	assert.NoError(t, err)
	fmt.Printf("output--------------->"+"%+v\n", output)
}

func Test_raw_update_sql(t *testing.T) {
	sql := "update `user_ff` set `nickname`='hello3' where created_at=2"
	err := EgormDB.Exec(sql).Error
	assert.NoError(t, err)
}

func Test_raw_delete_sql(t *testing.T) {
	sql := "delete from `user_ff`  where uid=2"
	err := EgormDB.Exec(sql).Error
	assert.NoError(t, err)
}

func TestOnConflict(t *testing.T) {
	type bean struct {
		Id   int64 `gorm:"primaryKey,autoIncrement"`
		Name string
		Age  int
	}

	type RecentFile struct {
		Id        int64
		UserId    int64
		FileId    int64
		Type      string // OPEN EDIT
		Timestamp int64
	}
	require.NoError(t, EgormDB.Table("recent_file").Clauses(
		clause.OnConflict{
			Columns: []clause.Column{{Name: "file_id"}, {Name: "user_id"}},
			DoUpdates: []clause.Assignment{{
				Column: clause.Column{Name: "timestamp"},
				Value:  clause.Expr{SQL: fmt.Sprintf("if(timestamp>%d, timestamp, %d)", 1, 1)},
			}, {
				Column: clause.Column{Name: "type"},
				Value:  clause.Expr{SQL: fmt.Sprintf("if(timestamp>%d, type, '%s')", 1, "aaa")},
			}},
		},
	).Create(&RecentFile{
		UserId:    1,
		FileId:    1,
		Type:      "aaa",
		Timestamp: 1,
	}).Error)
	require.NoError(t, EgormDB.Table("tab").Clauses(
		clause.OnConflict{
			Columns: []clause.Column{{Name: "id"}},
			DoUpdates: append(clause.AssignmentColumns([]string{"name"}), clause.Assignment{
				Column: clause.Column{Name: "age"},
				Value:  clause.Expr{SQL: "age + 1"},
			}),
		},
	).Create(&bean{
		Id:   1,
		Name: "hello",
		Age:  1,
	}).Error)
	require.NoError(t, EgormDB.Table("tab").Clauses(
		clause.OnConflict{
			Columns: []clause.Column{{Name: "id"}},
			DoUpdates: append(clause.AssignmentColumns([]string{"name"}), clause.Assignment{
				Column: clause.Column{Name: "age"},
				Value:  clause.Expr{SQL: fmt.Sprintf("if(age > %d, age, %d)", 1, 1)},
			}),
		},
	).Create(&bean{
		Id:   1,
		Name: "hello",
		Age:  2,
	}).Error)
	require.NoError(t, EgormDB.Table("tab").Clauses(
		clause.OnConflict{
			Columns: []clause.Column{{Name: "id"}},
			DoUpdates: append(clause.AssignmentColumns([]string{"name"}), clause.Assignment{
				Column: clause.Column{Name: "age"},
				Value:  clause.Expr{SQL: "if(age > values(age), age, values(age))"},
			}),
		},
	).Create(&bean{
		Id:   1,
		Name: "hello",
		Age:  1,
	}).Error)
}

func TestOnUpdateCurrentTimestamp(t *testing.T) {
	require.NoError(t, EgormDB.Exec(`drop table if exists team_advanced_permission`).Error)
	require.NoError(t, EgormDB.Exec(`
create table team_advanced_permission
(
    team_id     int               not null,
    target_id   bigint            not null,
    target_type varchar(32)       not null,
    permission  int               not null,
    allow       tinyint default 1 not null,
    created_at  int               not null,
    created_by  bigint            not null,
    updated_at  int               not null,
    updated_by  bigint            not null,
    deleted_at  int null,
    deleted_by  bigint null,
    primary key (team_id, target_id, target_type, permission)
);`).Error)
	require.NoError(t, EgormDB.Exec(`
create trigger team_advanced_permission_utime_trigger
before update on team_advanced_permission
for each row
begin
	select unix_timestamp(systimestamp) into :new.updated_at from dual;
end;
`).Error)

	type TeamAdvancedPermissionTargetType string

	const (
		TeamAdvancedPermissionTargetTypeTeam             TeamAdvancedPermissionTargetType = "team"
		TeamAdvancedPermissionTargetTypeDepartment       TeamAdvancedPermissionTargetType = "department"
		TeamAdvancedPermissionTargetTypeGroup            TeamAdvancedPermissionTargetType = "group"
		TeamAdvancedPermissionTargetTypeUser             TeamAdvancedPermissionTargetType = "user"
		TeamAdvancedPermissionTargetTypeFileRole         TeamAdvancedPermissionTargetType = "file_role"
		TeamAdvancedPermissionTargetTypeTeamRole         TeamAdvancedPermissionTargetType = "team_role"
		TeamAdvancedPermissionTargetTypeFileRoleOutsider TeamAdvancedPermissionTargetType = "file_role_outsider"
	)
	type ListTeamPermissionsRow struct {
		TeamID     int32                            `json:"team_id"`
		Permission int32                            `json:"permission"`
		TargetID   int64                            `json:"target_id"`
		TargetType TeamAdvancedPermissionTargetType `json:"target_type"`
		Allow      int8                             `json:"allow"`
		CreatedAt  int64
		CreatedBy  int64
		UpdatedAt  int64
		UpdatedBy  int64
		DeletedAt  sql.NullInt64
		DeletedBy  sql.NullInt64
	}

	var permissions = []ListTeamPermissionsRow{
		{
			TeamID:     1,
			Permission: 1,
			TargetID:   1,
			TargetType: TeamAdvancedPermissionTargetTypeDepartment,
			Allow:      1,
			CreatedAt:  time.Now().Unix(),
			CreatedBy:  1,
			UpdatedAt:  time.Now().Unix(),
			UpdatedBy:  1,
			DeletedAt: sql.NullInt64{
				Int64: 0,
				Valid: false,
			},
			DeletedBy: sql.NullInt64{
				Int64: 0,
				Valid: false,
			},
		},
	}
	require.NoError(t, EgormDB.Table("team_advanced_permission").Clauses(
		clause.OnConflict{
			Columns: []clause.Column{{Name: "team_id"}, {Name: "target_id"}, {Name: "target_type"}, {Name: "permission"}},
			DoUpdates: append(clause.AssignmentColumns([]string{"allow", "updated_at", "updated_by"}), []clause.Assignment{
				{Column: clause.Column{Name: "deleted_by"}, Value: sql.NullInt64{}}, {Column: clause.Column{Name: "deleted_at"}, Value: sql.NullInt64{}},
			}...),
		}).Create(&permissions).Error)
}

func TestLastInsertId(t *testing.T) {
	_, err := EgormDB.ConnPool.ExecContext(context.Background(), "drop table if exists tab")
	require.NoError(t, err)
	_, err = EgormDB.ConnPool.ExecContext(context.Background(), "create table tab (id bigint identity(1,1), name varchar(32) not null, age int not null, primary key (id))")
	require.NoError(t, err)
	type bean struct {
		Id   int64 `gorm:"primaryKey,autoIncrement"`
		Name string
		Age  int
	}
	var data = &bean{
		Id:   100,
		Name: "hello",
		Age:  1,
	}
	require.NoError(t, EgormDB.Table("tab").Create(&data).Error)
	data.Id = 0
	require.NoError(t, EgormDB.Table("tab").Create(&data).Error)
	assert.Equal(t, int64(101), data.Id)
	var datas = []bean{{Name: "hello111", Age: 1}, {Name: "hello2222", Age: 2}}
	require.NoError(t, EgormDB.Table("tab").Create(&datas).Error)
	assert.Equal(t, int64(102), datas[0].Id)
	assert.Equal(t, int64(103), datas[1].Id)
}

func TestGorm(t *testing.T) {
	type bean struct {
		Id   int64 `gorm:"primaryKey;autoIncrement"`
		Name string
	}
	require.NoError(t, EgormDB.Table("`tab`").Where("`id`=?", 1).Order("`id`").Group("`id`").Limit(1).Offset(0).Find(&bean{}).Error)
}

func TestColumnTag(t *testing.T) {
	type bean2 struct {
		BeanId  int64  `gorm:"column:id;primaryKey;autoIncrement"`
		NameAAA string `gorm:"column:name"`
	}
	var data bean2
	require.NoError(t, EgormDB.Table("`tab`").
		Set(gormdm.SetIdentityInsert()).
		Set(gormdm.SetOnConflictColumns("id", "name")).
		Exec("insert ignore into tab (id,name) values (1,'hello')").Error)
	require.NoError(t, EgormDB.Table("tab").Where("id=?", 1).First(&data).Error)
	assert.Equal(t, int64(1), data.BeanId)
	assert.Equal(t, "hello", data.NameAAA)
}

func TestGorm_update(t *testing.T) {
	require.NoError(t, EgormDB.Table("`tab`").Where("`id`=?", 1).UpdateColumns(map[string]interface{}{
		`name`: "hello",
		`id`:   gorm.Expr("`id` + 1"),
	}).Error)
}

func TestOnDup(t *testing.T) {
	require.NoError(t, EgormDB.
		Set("gorm2dm:on_conflict_columns", []string{"id", "name"}).
		Set(gormdm.SetIdentityInsert()).
		Exec("insert into tab (id,name,age) values (1,'hello',3),(2,'aaa',2) on duplicate key update age=values(age)").Error)
}

func TestOnDupFunc(t *testing.T) {
	require.NoError(t, EgormDB.
		Set("gorm2dm:on_conflict_columns", []string{"id", "name"}).
		Set(gormdm.SetIdentityInsert()).
		Exec("insert into tab (id,name,age) values (1,'hello',3) on duplicate key update age= if(age>1,values(age),age)").Error)
}

func TestReplace(t *testing.T) {
	require.NoError(t, EgormDB.
		Set("gorm2dm:on_conflict_columns", []string{"id", "name"}).
		Set(gormdm.SetIdentityInsert()).
		Exec("replace into tab (id,name,age) values (1,'hello',3)").Error)
}

func TestOnIgnore(t *testing.T) {
	require.NoError(t, EgormDB.
		Set("gorm2dm:on_conflict_columns", []string{"id", "name"}).
		Set(gormdm.SetIdentityInsert()).
		Exec("insert ignore into tab (id,name,age) values (1,'hello',3),(2,'aaa',2)").Error)
	require.Error(t, EgormDB.
		Set("gorm2dm:on_conflict_columns", []string{"id", "name"}).
		Set(gormdm.SetIdentityInsert()).
		Exec("insert into tab (id,name,age) values (1,'hello',3),(2,'aaa',2)").Error)
}

func TestFirst(t *testing.T) {
	type OrganizationReminder struct {
		TeamID    int64
		Info      string
		CreatedAt time.Time
		UpdatedAt time.Time
	}
	var reminder OrganizationReminder
	require.NoError(t, EgormDB.Where("team_id = ?", 1).First(&reminder).Error)
}

func TestRecordNotExists(t *testing.T) {
	_, err := EgormDB.ConnPool.ExecContext(context.Background(), "drop table if exists tab")
	require.NoError(t, err)
	_, err = EgormDB.ConnPool.ExecContext(context.Background(), "create table tab (id bigint identity(1,1), name varchar(32) not null, age int not null, primary key (id))")
	require.NoError(t, err)
	type bean struct {
		Id   int64 `gorm:"primaryKey,autoIncrement"`
		Name string
		Age  int
	}
	require.ErrorIs(t, EgormDB.Table("tab").Where("id = ?", 2).First(&bean{}).Error, gorm.ErrRecordNotFound)
}

func TestCreateByMap(t *testing.T) {
	_, err := EgormDB.ConnPool.ExecContext(context.Background(), "drop table if exists tab")
	require.NoError(t, err)
	_, err = EgormDB.ConnPool.ExecContext(context.Background(), "create table tab (id bigint identity(1,1), name varchar(32) not null, age int not null, primary key (id))")
	require.NoError(t, err)
	require.NoError(t, EgormDB.Table("tab").Create(map[string]interface{}{
		"name": "hello",
		"age":  1,
	}).Error)
	// check data
	var cnt int64
	require.NoError(t, EgormDB.Table("tab").Where("name = ?", "hello").Count(&cnt).Error)
	assert.Equal(t, int64(1), cnt)
}

func TestBit(t *testing.T) {
	_, err := EgormDB.ConnPool.ExecContext(context.Background(), "drop table if exists bit_test")
	require.NoError(t, err)
	_, err = EgormDB.ConnPool.ExecContext(context.Background(), "create table bit_test (id bigint , is_deleted bit not null)")
	require.NoError(t, err)
	err = EgormDB.Exec("insert into bit_test (id, is_deleted) values (1, b'1')").Error
	require.NoError(t, err)
	rows, err := EgormDB.Raw("select * from bit_test where id=?", 1).Rows()
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var (
			id        int64
			isDeleted bool
		)
		err = rows.Scan(&id, &isDeleted)
		require.NoError(t, err)
		assert.Equal(t, true, isDeleted)
	}
	rows, err = EgormDB.Raw("select * from bit_test where id=?", 1).Rows()
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var (
			id        int64
			isDeleted string
		)
		err = rows.Scan(&id, &isDeleted)
		require.NoError(t, err)
		assert.Equal(t, "1", isDeleted)
	}
}

func TestBytes(t *testing.T) {
	_, err := EgormDB.ConnPool.ExecContext(context.Background(), "drop table if exists bytes_test")
	require.NoError(t, err)
	_, err = EgormDB.ConnPool.ExecContext(context.Background(), "create table bytes_test (id bigint identity(1,1) , content text not null)")
	require.NoError(t, err)
	type BytesTest struct {
		Id      int64
		Content []byte
	}
	err = EgormDB.Table("bytes_test").Create(&BytesTest{
		Id:      1,
		Content: func() []byte { bytes, _ := json.Marshal(map[string]interface{}{"for": "aaa"}); return bytes }(),
	}).Error
	require.NoError(t, err)
	rows, err := EgormDB.Raw("select * from bytes_test where id=?", 1).Rows()
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var (
			id      int64
			content string
		)
		err = rows.Scan(&id, &content)
		require.NoError(t, err)
		assert.Contains(t, content, "{")
	}
}

func TestCreate(t *testing.T) {
	_, err := EgormDB.ConnPool.ExecContext(context.Background(), "drop table if exists AG_EVENT_LOG")
	require.NoError(t, err)
	sqlStr := `
drop table if exists AG_EVENT_LOG;

create table AG_EVENT_LOG (
ID bigint identity(4,1) not null,
MTIME bigint not null,
ANO_NUM bigint not null,
UID_LIST_STR text);

alter table AG_EVENT_LOG add constraint ag_event_log_pk primary key (ID);

create index ag_event_log_idx_mtime on AG_EVENT_LOG(MTIME);
`
	for _, sql := range strings.Split(sqlStr, ";\n\n") {
		sql = strings.TrimSpace(sql)
		fmt.Printf("sql----->%v\n", sql)
		if sql == "" {
			continue
		}

		_, err = EgormDB.ConnPool.ExecContext(context.Background(), sql)
		require.NoError(t, err)
	}

}
