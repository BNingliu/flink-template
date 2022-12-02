package com.ytd.template.api;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class MySqlBinlogSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();


//   指定连接器在反序列化二进制日志事件期间应如何对异常做出反应。
//    , fail传播异常，该异常指示有问题的事件及其二进制日志偏移量，并导致连接器停止。
//    ,warn记录有问题的事件及其二进制日志偏移量，然后跳过该事件。
//    ,ignore跳过有问题的事件并且不记录任何内容。
        properties.setProperty("event.deserialization.failure.handling.mode", "warn");

//        指定连接器应如何对与内部模式表示中不存在的表相关的二进制日志事件作出反应。即内部表示与数据库不一致。
//        fail抛出一个异常，指示有问题的事件及其二进制日志偏移量，并导致连接器停止。
//        warn记录有问题的事件及其二进制日志偏移量并跳过该事件。
//        skip跳过有问题的事件并且不记录任何内容。
        properties.setProperty("inconsistent.schema.handling.mode", "warn");
//        一个布尔值，指定连接器是否应忽略格式错误或未知的数据库语句或停止处理，以便人们可以解决问题。安全的默认值为false. 跳过应该小心使用，因为它会在处理 binlog 时导致数据丢失或损坏。
//        properties.setProperty("database.history.skip.unparseable.ddl", "true");

//        properties.setProperty("database.history.store.only.monitored.tables.ddl", "false");

        //一个布尔值，指定连接器是否应记录所有 DDL 语句
        //true仅记录那些与 Debezium 正在捕获其更改的表相关的 DDL 语句。true请谨慎设置，因为如果您更改哪些表已捕获其更改，则可能需要丢失数据。
        //安全的默认值为false.
//        properties.setProperty("database.history.store.only.captured.tables.ddl", "true");


        env.setParallelism(1);
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.0.1.43")
                .port(3306)
                .databaseList("act_hubeiccb") // set captured database
                .tableList("act_hubeiccb.t_account") // set captured table
                .username("canal")
                .password("58bYr1hltcH2M6VB")
                .serverId("332")
                .includeSchemaChanges(false)
                .startupOptions(StartupOptions.latest())
                .deserializer(new MySqlBinlogSourceJson()) // converts SourceRecord to JSON String
                .debeziumProperties(properties)
                .build();

        // 设置debezium参数


        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(60000);  //头和头


        //关闭共享slot(可以观察到消息发送和接收数量)]
        env.disableOperatorChaining();

        //创建表环境

        DataStream<String> mySQL_source =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                        .setParallelism(1);
        //创建临时表
        mySQL_source.print();

        env.execute("Print MySQL Snapshot + Binlog");
    }
}


class MySqlBinlogSourceJson implements DebeziumDeserializationSchema {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
        //创建 JSON 对象用于封装最终返回值数据信息
        JSONObject resultJson = new JSONObject();

        try {
            //获取值信息并转换为 Struct 类型
            Struct value = (Struct) sourceRecord.value();
            //获取变化后的数据
            Struct after = value.getStruct("after");
            Struct before = value.getStruct("before");
            Struct source = value.getStruct("source");
            //获取库、表、时间
            if (source != null) {
                Schema sourceSchema = source.schema();
                for (Field field : sourceSchema.fields()) {
                    if (field.name().equals("ts_ms")) {
                        resultJson.put("ts", source.get(field));
                    }
                    if (field.name().equals("db")) {
                        resultJson.put("dbName", source.get(field));
                    }
                    if (field.name().equals("table")) {
                        resultJson.put("tableName", source.get(field));
                    }
                }
            }
            //创建 JSON 对象用于存储数据信息
            JSONObject data = new JSONObject();
            //若只有after,则表示插入;若只有before,说明删除;若既有before也有after,则表示修改
            if (after != null && before == null) {
                resultJson.put("op", "c");
                for (Field field : after.schema().fields()) {
                    Object o = after.get(field);
                    data.put(field.name(), o);
                }
                resultJson.put("only_insert", JSONObject.toJSONString(data));
            } else if (after != null && before != null) {
                resultJson.put("after", "after");
                data = new JSONObject();
                for (Field field : after.schema().fields()) {
                    Object o = after.get(field);
                    data.put(field.name(), o);
                }
                resultJson.put("after_update", JSONObject.toJSONString(data));

                resultJson.put("before", "before");
                data = new JSONObject();
                for (Field field : before.schema().fields()) {
                    Object o = before.get(field);
                    data.put(field.name(), o);
                }
                resultJson.put("before_update",JSONObject.toJSONString(data));

//                resultJson.put("data", data);
            } else if (after == null && before != null) {
                resultJson.put("op", "d");
                for (Field field : before.schema().fields()) {
                    Object o = before.get(field);
                    data.put(field.name(), o);
                }
                resultJson.put("before_delete", JSONObject.toJSONString(data));
            } else {
                resultJson = new JSONObject();
            }

        } catch (Exception e) {
            System.out.println(e.getMessage() + "代码行：----》" + JSONObject.toJSON(e.getStackTrace()));
        }
        if (resultJson != null && !resultJson.isEmpty()) {
            collector.collect(JSONObject.toJSONString(resultJson));
        }
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(String.class);
    }
}