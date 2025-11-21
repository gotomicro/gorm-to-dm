package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gormdm "github.com/gotomicro/gorm-to-dm"
)

func Test_AllSql(t *testing.T) {
	tests := []struct {
		Name    string
		DMSQL   string
		MySQL   string
		wantErr bool
	}{
		{
			Name:  "select",
			MySQL: "select * from `user_ff` where nickname='hello'",
			DMSQL: `select * from USER_FF where NICKNAME = 'hello';`,
		},
		{
			Name:  "select",
			MySQL: "select * from `user_ff` where `nickname`='hello' and age=1",
			DMSQL: `select * from USER_FF where NICKNAME = 'hello' and AGE = 1;`,
		},
		{
			Name:  "select",
			MySQL: "select * from tab.`user_ff` where `nickname`='hello' and age=1",
			DMSQL: `select * from TAB.USER_FF where NICKNAME = 'hello' and AGE = 1;`,
		},
		{
			Name:  "select",
			MySQL: "select id from tab.user_ff where `nickname`='hello' and age=1 and id in (select id from tab.user_ff where `nickname`='hello' and age=1)",
			DMSQL: `select ID from TAB.USER_FF where NICKNAME = 'hello' and AGE = 1 and ID in (select ID from TAB.USER_FF where NICKNAME = 'hello' and AGE = 1);`,
		},
		{
			Name:  "select",
			MySQL: "select id from user_ff where nickname = 'hello' and ( age = 1 or age = 2)",
			DMSQL: `select ID from USER_FF where NICKNAME = 'hello' and (AGE = 1 or AGE = 2);`,
		},
		{
			Name:  "select",
			MySQL: "select * from user_ff where nickname='aaa' order by id desc limit 1 offset 1",
			DMSQL: `select * from USER_FF where NICKNAME = 'aaa' order by ID desc limit 1 offset 1;`,
		},
		{
			Name:  "join",
			MySQL: "select * from tab1 t1 left join tab2 t2 on t1.id = t2.t1_id where t2.name is not null",
			DMSQL: `select * from TAB1 as T1 LEFT JOIN TAB2 as T2 on T1.ID = T2.T1_ID where T2.NAME is not null;`,
		},
		{
			Name:  "sumif",
			MySQL: `select sum(if(status = 1, 1, 0)) as count from user_ff where nickname='aaa'`,
			DMSQL: `select sum(case when STATUS = 1 then 1 else 0 end) as "COUNT" from USER_FF where NICKNAME = 'aaa';`,
		},
		{
			Name:  "ifnull",
			MySQL: `select ifnull(sum(size), 0) from user_usage where user_id = ?`,
			DMSQL: `select ifnull(sum("SIZE"), 0) from USER_USAGE where USER_ID = ?;`,
		},
		{
			Name:  "func",
			MySQL: "select unix_timestamp()",
			DMSQL: `select unix_timestamp(SYSDATE()) from DUAL;`,
		},
		{
			Name:  "insert",
			MySQL: "insert into `user_ff` (`nickname`,`created_at`,`deleted_at`) values ('hello',0,0)",
			DMSQL: `insert into USER_FF (NICKNAME, CREATED_AT, DELETED_AT) values ('hello', 0, 0);`,
		},
		{
			Name:  "insert",
			MySQL: "insert into `user_ff` (`status`) values (b'1')",
			DMSQL: `insert into USER_FF (STATUS) values (1);`,
		},
		{
			Name:  "insert",
			MySQL: "insert into `user_ff` (`updated_at`) values (unix_timestamp())",
			DMSQL: `insert into USER_FF (UPDATED_AT) values (unix_timestamp(SYSDATE()));`,
		},
		{
			Name:  "insert",
			MySQL: "INSERT INTO `user_ff` (`status`) VALUES (_binary '\\0');",
			DMSQL: `insert into USER_FF (STATUS) values (0);`,
		},
		{
			Name:  "insert",
			MySQL: "INSERT INTO `user_ff` (`status`) VALUES (_binary '\u0001'),(_binary '\\0');",
			DMSQL: `insert into USER_FF (STATUS) values (1), (0);`,
		},
		{
			Name:  "update",
			MySQL: "update `user_ff` set `nickname`='hello3' where created_at=2",
			DMSQL: `update USER_FF set NICKNAME = 'hello3' where CREATED_AT = 2;`,
		},
		{
			Name:  "delete",
			MySQL: "delete from `user_ff`  where uid=2",
			DMSQL: `delete from USER_FF where UID = 2;`,
		},
		{
			Name:  "count discount multiple cols",
			MySQL: "select count(distinct `uid`,`nickname`) from `user_ff`",
			DMSQL: `select count(distinct CONCAT(UID, NICKNAME)) from USER_FF;`,
		},
		{
			Name:  "identity",
			MySQL: "select LAST_INSERT_ID()",
			DMSQL: `select @@IDENTITY from DUAL;`,
		},
		{
			Name:  "identity",
			MySQL: "INSERT INTO `user_attribute_config` (`attribute_id`, `attribute_type`, `label`, `enabled`, `team_id`, `created_at`, `updated_at`) VALUES (?,?,?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)    ON DUPLICATE KEY UPDATE  `user_attribute_config`.`enabled` = VALUES(`enabled`)",
			DMSQL: `select @@IDENTITY from DUAL;`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			sql, _ := gormdm.NewBuilder().Parse(tt.MySQL)
			assert.Equal(t, tt.DMSQL, sql)
			fmt.Printf("sql--------------->"+"%+v\n", sql)
		})
	}
}

func TestDDL(t *testing.T) {
	// sql := "CREATE TABLE `announcement` (\n  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增 id',\n  `file_id` bigint(20) unsigned NOT NULL COMMENT '文件 id',\n  `content` text COMMENT '公告内容',\n  `user_id` bigint(20) unsigned NOT NULL COMMENT '公告创建者',\n  `created_at` int(11) NOT NULL COMMENT '公告创建时间',\n  `deleted_at` int(11) DEFAULT NULL COMMENT '公告删除时间',\n  PRIMARY KEY (`id`),\n  KEY `idx_file_id` (`file_id`)\n) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COMMENT='空间公告';"
	// 从 dump_mysql.sql 读取
	sqlBytes, err := os.ReadFile("dump_mysql.sql")
	require.NoError(t, err)
	sql := string(sqlBytes)
	dmSql, err := gormdm.NewBuilder().Parse(sql)
	require.NoError(t, err)
	fmt.Println(dmSql)
	os.WriteFile("dump_dm.sql", []byte(dmSql), 0666)
}

func TestDDL2(t *testing.T) {
	sql := "CREATE TABLE `cv_bd_depend` (\n  `iid` int(11) DEFAULT NULL,\n  `database` varchar(64) NOT NULL,\n  `table` varchar(128) NOT NULL,\n  `engine` varchar(128) NOT NULL,\n  `down_dep_database_table` text NOT NULL,\n  `up_dep_database_table` text NOT NULL,\n  `rows` bigint(20) NOT NULL DEFAULT '0',\n  `bytes` bigint(20) NOT NULL DEFAULT '0',\n  `utime` bigint(20) DEFAULT NULL COMMENT '更新时间',\n  UNIQUE KEY `uix_iid_database_table` (`iid`,`database`,`table`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
	dmSql, err := gormdm.NewBuilder().Parse(sql)
	require.NoError(t, err)
	fmt.Println(dmSql)
}
func TestDDL3(t *testing.T) {
	sql := "CREATE TABLE `membership` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n  `target_id` int(11) DEFAULT NULL,\n  `target_type` int(11) DEFAULT NULL,\n  `category` int(11) DEFAULT NULL,\n  `expired_at` datetime DEFAULT NULL,\n  `created_at` datetime DEFAULT NULL,\n  `updated_at` datetime DEFAULT NULL,\n  `is_official` tinyint(1) DEFAULT '0',\n  `deleted_at` datetime DEFAULT NULL,\n  `member_count` int(11) DEFAULT '0',\n  `edition_id` int(11) DEFAULT NULL COMMENT '版本 id',\n  `started_at` datetime(3) NOT NULL DEFAULT '0000-00-00 00:00:00.000' COMMENT '当前阶段会员开始的时间(中断后重新计算)',\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `uniq_membership_target_type_target_id` (`target_type`,`target_id`),\n  KEY `membership_category` (`category`),\n  KEY `membership_expired_at_target_type` (`expired_at`,`target_type`),\n  KEY `membership_target_type` (`target_type`),\n  KEY `target_id` (`target_id`)\n) ENGINE=InnoDB AUTO_INCREMENT=2095 DEFAULT CHARSET=latin1;"
	dmSql, err := gormdm.NewBuilder().Parse(sql)
	require.NoError(t, err)
	fmt.Println(dmSql)
}

func TestSetSchema(t *testing.T) {
	dmSql, err := gormdm.NewBuilder().Parse("use svc_file;")
	require.NoError(t, err)
	assert.Equal(t, "set schema svc_file;", dmSql)
}
