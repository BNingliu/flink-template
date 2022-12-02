-- drop table IF EXISTS item_test;
-- drop table IF EXISTS hive_flink_table;
CREATE TABLE kafka_mysql_cdc
(
    before ROW(id           bigint ,
        created_time String,
        desc         STRING,
        name         STRING,
        tenant_id    int,
        app_id       int),
    after ROW (id           bigint ,
        created_time String,
        desc         STRING,
        name         STRING,
        tenant_id    int,
        app_id       int)
) WITH (
      'connector.type' = 'kafka',
      'connector.version' = 'universal', -- 指定Kafka连接器版本，不能为2.4.0，必须为universal，否则会报错
      'connector.topic' = 'ytd01', -- 指定消费的topic
      'connector.startup-mode' = 'latest-offset', -- 指定起始offset位置
      'connector.properties.zookeeper.connect' = '192.168.110.131:2181',
      'connector.properties.bootstrap.servers' = '192.168.110.131:9093,192.168.110.131:9094,192.168.110.131:9095',
--       'connector.properties.group.id' = 'student_1',
      'format.type' = 'json',
      'format.derive-schema' = 'true', -- 由表schema自动推导解析JSON
      'update-mode' = 'append'
      );

CREATE CATALOG myhive WITH (
    'type' = 'hive',
    'default-database' = 'di_354',
    'hive-conf-dir' = 'hdfs://ytddata0.localdomain:8020/streamx/flink/flink-libs/'
);
USE CATALOG myhive;

SET table.sql-dialect=hive;
CREATE  TABLE if not exists  myhive.di_354.fs_table5 (
    id bigint,
    created_time String,
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

INSERT INTO  fs_table5
SELECT after.* FROM kafka_mysql_cdc;
