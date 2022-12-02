create table ytd_user_group
(
    id           bigint primary key,
    created_time STRING,
    desc         STRING,
    name         STRING,
    tenant_id    int,
    app_id       int
) with (
      'connector' = 'mysql-cdc',
      'hostname' = '10.0.0.201',
      'port' = '3306',
      'username' = 'flink',
      'password' = 'flink123',
      'database-name' = 'flink_test',
      'scan.startup.mode' = 'initial',
      'table-name' = 'ytd_user_group'
 );

CREATE
CATALOG myhive WITH (
    'type' = 'hive',
    'default-database' = 'act_data',
    'hive-conf-dir' = 'hdfs://ytddata00.localdomain:8020/applications/flink/flink-libs/conf'
);

USE CATALOG myhive;


SET table.sql-dialect=hive;

CREATE  TABLE if not exists  ytd_user_group_stream (
           id bigint,
           created_time STRING,
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

INSERT into  ytd_user_group_stream SELECT *  FROM ytd_user_group;
