create table a_por3
(
    distinct_id string not null,
    tag_value   string,
    base_day    string,
    g_key       string,
    tag_code    string
) with (
      'connector.type' = 'jdbc',
      'connector.url' = 'jdbc:mysql://192.168.2.131:3306/test?characterEncoding=UTF-8&useSSL=false',
      'connector.table' = 'a_por3',
      'connector.driver' = 'com.mysql.cj.jdbc.Driver',
      'connector.username' = 'root',
      'connector.write.flush.max-rows' = '2000' ,
      'connector.password' = 'ytdinfo123'
    );


CREATE CATALOG myhive WITH (
    'type' = 'hive',
    'default-database' = 'portrait_97',
    'hive-conf-dir' = 'hdfs://ytddata0.localdomain:8020/applications/flink/flink-libs/conf'
);

USE CATALOG myhive;
SET table.sql-dialect=hive;

CREATE TABLE  portrait_tag_0 (
         distinct_id string not null,
         tag_value   string,
         base_day    string,
         g_key       string,
         tag_code    string
) TBLPROPERTIES (
  'streaming-source.enable' = 'true',           -- option with default value, can be ignored.
  'streaming-source.partition.include' = 'all',  -- option with default value, can be ignored.
  'lookup.join.cache.ttl' = '12 h'
);

SET table.sql-dialect=default;

INSERT INTO a_por3
SELECT * FROM portrait_tag_0;

