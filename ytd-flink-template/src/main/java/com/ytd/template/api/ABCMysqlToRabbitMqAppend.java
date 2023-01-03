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
import jodd.util.ObjectUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Properties;

//苏州农行九张表同步到rabbitmq回流
public class ABCMysqlToRabbitMqAppend {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();

        properties.setProperty("event.deserialization.failure.handling.mode", "warn");
        properties.setProperty("inconsistent.schema.handling.mode", "warn");

        env.setParallelism(1);

//        MySqlSource actAbcsuzhou = MySqlSource.<String>builder()
//                .hostname("10.0.1.121")
//                .port(3306)
//                .databaseList("act_abcsuzhou") // set captured database
//                .tableList(
//                         "act_abcsuzhou.t_poster_account"
//                ) // set captured table
//                .username("canal")
//                .password("YlX8ndVFJQV51JKl")
//                .includeSchemaChanges(false)
//                .startupOptions(StartupOptions.initial())
//                .deserializer(new ABCMySqlBinlogSourceJsonAppend()) // converts SourceRecord to JSON String
//                .debeziumProperties(properties)
//                .build();

        MySqlSource<String> actAbcsuzhou = MySqlSource.<String>builder()
                .hostname("10.0.1.15")
                .port(3306)
                .databaseList("ytd-datastage") // set captured database
                .tableList("ytd-datastage.ytd_land_sznh_ygzs_es_group_detail") // set captured table
                .username("root")
                .password("2OgJFhAsvMvECHD")
                .includeSchemaChanges(false)
                .startupOptions(StartupOptions.initial())
                .deserializer(new ABCMySqlBinlogSourceJson()) // converts SourceRecord to JSON String
                .debeziumProperties(properties)
                .build();

        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(60000);  //头和头


        //关闭共享slot(可以观察到消息发送和接收数量)]
        env.disableOperatorChaining();
        //创建表环境
        DataStream<String> abcsuzhou_source =
                env.fromSource(actAbcsuzhou, WatermarkStrategy.noWatermarks(), "abcsuzhou Source");

//        LocalDateTime now = LocalDateTime.now();
//        ZoneId zone = ZoneId.systemDefault();

//        SingleOutputStreamOperator<String> create_time = abcsuzhou_source.filter(f -> {
//            JSONObject jsonObject = JSONObject.parseObject(f);
//            JSONObject table_details =  JSONObject.parseObject(jsonObject.get("table_details").toString());
//            if(table_details.containsKey("create_time")&& !Objects.isNull(table_details.get("create_time"))){
//                Instant instant = Instant.ofEpochMilli((Long) table_details.get("create_time"));
//                LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zone);
//                Duration duration = Duration.between(localDateTime, now);
//                return duration.toDays()<= 3;
//            }
//            return false;
//        });
        RabbitMqSinkProperties sinkProperties = RabbitMqSinkProperties.builder()
                .host("10.0.1.30")
                .port(5672)
                .userName("admin")
                .passWord("mRQTwfZOjQIYXIHY")
                .queue("szabc.act_abcsuzhou.t_account")
                .virtualHost("/sznh")
                .build();

//        //将结果流用自定义的sink发送到rabbitmq
        abcsuzhou_source.addSink(new MQSinkFunction(sinkProperties));

        env.execute("Print MySQL Snapshot + Binlog");
    }
}


class ABCMySqlBinlogSourceJsonAppend implements DebeziumDeserializationSchema {
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