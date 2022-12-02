package com.ytd.template.util.mq;//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//


import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema.RMQCollector;

final class RMQDeserializationSchemaWrapper<OUT> implements RMQDeserializationSchema<OUT> {
    private final DeserializationSchema<OUT> schema;

    RMQDeserializationSchemaWrapper(DeserializationSchema<OUT> deserializationSchema) {
        this.schema = deserializationSchema;
    }

    public void deserialize(Envelope envelope, BasicProperties properties, byte[] body, RMQCollector<OUT> collector) throws IOException {
        collector.collect(this.schema.deserialize(body));
    }

    public TypeInformation<OUT> getProducedType() {
        return this.schema.getProducedType();
    }

    public void open(InitializationContext context) throws Exception {
        this.schema.open(context);
    }

    public boolean isEndOfStream(OUT nextElement) {
        return this.schema.isEndOfStream(nextElement);
    }
}
