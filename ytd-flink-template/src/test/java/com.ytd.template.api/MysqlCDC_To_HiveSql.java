package com.ytd.template.api;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

public class MysqlCDC_To_HiveSql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.setProperty("HADOOP_USER_NAME", "hive");

        env.setParallelism(1);
        //设置statebackend
        //rocksDB需要引入依赖flink-statebackend-rocksdb_2.11
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        //1.1 开启CK并指定状态后端为FS
//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://name01:8020/flink-checkpoints/30"));

        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(60000);  //头和头

        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少30000 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000L);//尾和头
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
        //关闭共享slot(可以观察到消息发送和接收数量)
        env.disableOperatorChaining();
        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        tableEnv.executeSql("create table ytd_user_group (" +
                "id bigint primary key," +
                "created_time String," +
                "desc STRING," +
                "name STRING," +
                "tenant_id int," +
                "app_id int" +
                ")with(" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = 'rm-uf65iwa3laojof99n6o.mysql.rds.aliyuncs.com'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = '123qweQWE'," +
                "'database-name' = 'data_integration'," +
                "'scan.startup.mode' = 'initial'," +
                "'table-name' = 'ytd_user_group'" +
                ")");
        Table table = tableEnv.sqlQuery("select * from ytd_user_group");

        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class).filter(f -> {

                    return f.f0.toString().equals("true");
                } );
        retractStream.print();

        String version = "3.1.0";
        String name = "myhive";
        String defaultDatabase = "di_20";
        String hiveConfDir = "hdfs://ytddata0.localdomain:8020/applications/flink/flink-libs/conf/";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog(name, hive);
        tableEnv.useCatalog(name);
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase(defaultDatabase);
        tableEnv.createTemporaryView("ytd_user_group", retractStream);

        //      如果hive中已经存在了相应的表，则这段代码省略
        String hiveSql = "CREATE external TABLE if not exists  fs_table7 (\n" +
                "  id bigint, \n" +
                "  created_time String, \n" +
                "  desc STRING, \n" +
                "  name STRING, \n" +
                "  tenant_id int," +
                " app_id int   \n" +
                ") " +
                "stored as parquet " +
                "TBLPROPERTIES (\n" +
//                "  'partition.time-extractor.timestamp-pattern'='$dt $h:00:00',\n" +
                "  'sink.partition-commit.delay'='10s',\n" +
                "  'sink.partition-commit.trigger'='partition-time',\n" +
                "  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',\n" +
                "  'sink.partition-commit.policy.kind'='metastore,success-file'" +
                ")";
        tableEnv.executeSql(hiveSql);


        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.from("ytd_user_group").printSchema();

        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.hive.fallback-mapred-reader", "true");

        StatementSet statementSet = tableEnv.createStatementSet();


        String insertSql = String.format(
                "SELECT f1.* FROM ytd_user_group ");
        insertSql = String.format("insert into fs_table7 SELECT f1.id,f1.created_time ,f1.desc ,f1.name,f1.tenant_id " +
                ",f1.app_id FROM ytd_user_group");
        tableEnv.executeSql(insertSql).print();


    }
}

