# Gorm To Dm

gorm mysql 协议转换为 达梦

## 怎么访问达梦
1 选择自定义驱动
2 安装达梦jar包

connect test_dm:dmpassword@127.0.0.1:25236

create user test_dm identified by dmpassword;

create table test_dm.user
(
    uid         bigint  identity(1,1) primary key,
    nickname varchar(128) default ''                not null ,
    created_at int             not null ,
    deleted_at int             null
);

SELECT DBMS_METADATA.GET_DDL('TABLE','USER','TEST_DM') FROM dual;
SP_TABLEDEF('test_dm','user');

SELECT DBMS_METADATA.GET_DDL('TABLE','EMPLOYEE','DMHR') FROM dual;

SELECT * FROM user;

## 注意事项
大多数场景下，直接引入该 lib 后即可将 gorm 或原生 sql 转换为达梦 sql，但是有一些特殊情况需要注意：
- 使用时，不能直接使用 `gorm` 的 `ConnPool`，因为这样没法使用 `gorm` 的 `hook`
- 由于达梦不支持 `ON DUPLICATE KEY UPDATE`，需要转换成 `merge into` 语句，而 `merge into` 语句需要指定 `on` 条件，所以需要我们手动指定 `on` 条件：
```golang
 EgormDB.
		Set(gormdm.SetOnConflictColumns("id", "name")).
		Set(gormdm.SetIdentityInsert()).
		Exec("insert ignore into tab (id,name,age) values (1,'hello',3),(2,'aaa',2)")
```
- 达梦在给自增字段赋值时，需要使用 `identity_insert`，所以需要我们手动指定 `identity_insert`：
```golang
 EgormDB.
        Set(gormdm.SetIdentityInsert()).
        Exec("insert into tab (id,name,age) values (1,'hello',3),(2,'aaa',2)")
```