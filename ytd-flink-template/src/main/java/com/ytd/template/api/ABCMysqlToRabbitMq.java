package com.ytd.template.api;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ytd.template.bean.RabbitMqSinkProperties;
import com.ytd.template.util.MQSinkFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

//苏州农行九张表同步到rabbitmq回流
public class ABCMysqlToRabbitMq {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();

        properties.setProperty("event.deserialization.failure.handling.mode", "warn");
        properties.setProperty("inconsistent.schema.handling.mode", "warn");

        env.setParallelism(2);
        //"core_abcsuzhou.t_account" 不用了

        MySqlSource actAbcsuzhou = MySqlSource.<String>builder()
                .hostname("10.0.1.121")
                .port(3306)
                .databaseList("act_abcsuzhou") // set captured database
                .tableList(
                         "act_abcsuzhou.t_poster_account," +
                         "act_abcsuzhou.t_poster," +
                         "act_abcsuzhou.t_account," +
                         "act_abcsuzhou.t_poster_account_achieve," +
                         "act_abcsuzhou.t_activity_reward_achieved_log," +
                         "act_abcsuzhou.t_point," +
                         "act_abcsuzhou.t_account_point_log,"+
                          "act_abcsuzhou.t_wechat_qrcode_record"
                ) // set captured table
                .username("canal")
                .password("YlX8ndVFJQV51JKl")
                .includeSchemaChanges(false)
                .startupOptions(StartupOptions.latest())
                .deserializer(new ABCMySqlBinlogSourceJson()) // converts SourceRecord to JSON String
                .debeziumProperties(properties)
                .build();
//
        MySqlSource<String> datastage = MySqlSource.<String>builder()
                .hostname("10.0.1.15")
                .port(3306)
                .databaseList("ytd-datastage") // set captured database
                .tableList("ytd-datastage.ytd_land_sznh_ygzs_es_group_detail") // set captured table
                .username("root")
                .password("2OgJFhAsvMvECHD")
                .includeSchemaChanges(false)
                .startupOptions(StartupOptions.latest())
                .deserializer(new ABCMySqlBinlogSourceJson()) // converts SourceRecord to JSON String
                .debeziumProperties(properties)
                .build();


        // 设置debezium参数


        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(60000);  //头和头


        //关闭共享slot(可以观察到消息发送和接收数量)]
        env.disableOperatorChaining();

        //创建表环境

        DataStream<String> abcsuzhou_source =
                env.fromSource(actAbcsuzhou, WatermarkStrategy.noWatermarks(), "abcsuzhou Source");

        DataStream<String> datastage_source =
                env.fromSource(datastage, WatermarkStrategy.noWatermarks(), "datastage Source");
        DataStream<String> unionStream = abcsuzhou_source.union(datastage_source);

        //创建临时表
//        unionStream.print();

        RabbitMqSinkProperties sinkProperties = RabbitMqSinkProperties.builder()
                .host("10.0.1.30")
                .port(5672)
                .userName("admin")
                .passWord("mRQTwfZOjQIYXIHY")
                .queue("szabc.act_abcsuzhou.t_account")
                .virtualHost("/sznh")
                .build();

        //将结果流用自定义的sink发送到rabbitmq
        unionStream.addSink(new MQSinkFunction(sinkProperties));

        env.execute("Print MySQL Snapshot + Binlog");
    }
}


class ABCMySqlBinlogSourceJson implements DebeziumDeserializationSchema {
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
            data.put("system_time",System.currentTimeMillis());
            //若只有after,则表示插入;若只有before,说明删除;若既有before也有after,则表示修改
            if (after != null && before == null) {
                data.put("op", "i");
                for (Field field : after.schema().fields()) {
                    Object o = after.get(field);
                    data.put(field.name(), o);
                }
                resultJson.put("table_details", JSONObject.toJSONString(data));
            } else if (after != null && before != null) {
                data.put("op", "u");
                for (Field field : after.schema().fields()) {
                    Object o = after.get(field);
                    data.put(field.name(), o);
                }
                resultJson.put("table_details", JSONObject.toJSONString(data));

            } else if (after == null && before != null) {
                data.put("op", "d");
                for (Field field : before.schema().fields()) {
                    Object o = before.get(field);
                    data.put(field.name(), o);
                }
                resultJson.put("table_details", JSONObject.toJSONString(data));
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