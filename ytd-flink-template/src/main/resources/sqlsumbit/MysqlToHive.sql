CREATE TABLE ytd_user_group
(
    id           bigint primary key,
    created_time TIMESTAMP,
    desc         STRING,
    name         STRING,
    tenant_id    int,
    app_id       int
) WITH (
      'connector.type' = 'jdbc',
      'connector.url' = 'jdbc:mysql://192.168.2.131:3306/test?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=Asia/Shanghai',
      'connector.driver' = 'com.mysql.cj.jdbc.Driver',
      'connector.table' = 'ytd_user_group',
      'connector.username' = 'root',
      'connector.password' = 'ytdinfo123',
      'connector.write.flush.max-rows' = '1'
 );

CREATE
CATALOG myhive WITH (
    'type' = 'hive',
    'default-database' = 'di_354',
    'hive-conf-dir' = 'hdfs://ytddata0.localdomain:8020/streamx/flink/flink-libs/'
);

USE CATALOG myhive;


SET table.sql-dialect=hive;

CREATE  TABLE if not exists  fs_table6 (
   id bigint,
   created_time TIMESTAMP,
   desc STRING,
   name STRING,
   tenant_id int,
   app_id int
) stored as parquet TBLPROPERTIES (
  'sink.partition-commit.delay'='10s',
  'sink.partition-commit.trigger'='partition-time',
  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',
  'sink.partition-commit.policy.kind'='metastore,success-file'
);

SET table.sql-dialect=default;

INSERT INTO  fs_table6 SELECT * FROM ytd_user_group;

