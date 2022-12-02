package com.ytd.template.util.mq;//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MultipleIdsMessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema.RMQCollector;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YTDRMQSource<OUT> extends MultipleIdsMessageAcknowledgingSourceBase<OUT, String, Long> implements ResultTypeQueryable<OUT> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(YTDRMQSource.class);
    private final RMQConnectionConfig rmqConnectionConfig;
    protected final String queueName;
    private final boolean usesCorrelationId;
    protected RMQDeserializationSchema<OUT> deliveryDeserializer;
    protected transient Connection connection;
    protected transient Channel channel;
    protected transient QueueingConsumer consumer;
    protected transient boolean autoAck;
    private transient volatile boolean running;

    public YTDRMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema<OUT> deserializationSchema) {
        this(rmqConnectionConfig, queueName, false, deserializationSchema);
    }

    public YTDRMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, boolean usesCorrelationId, DeserializationSchema<OUT> deserializationSchema) {
        super(String.class);
        this.rmqConnectionConfig = rmqConnectionConfig;
        this.queueName = queueName;
        this.usesCorrelationId = usesCorrelationId;
        this.deliveryDeserializer = new RMQDeserializationSchemaWrapper(deserializationSchema);
    }

    public YTDRMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, RMQDeserializationSchema<OUT> deliveryDeserializer) {
        this(rmqConnectionConfig, queueName, false, deliveryDeserializer);
    }

    public YTDRMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, boolean usesCorrelationId, RMQDeserializationSchema<OUT> deliveryDeserializer) {
        super(String.class);
        this.rmqConnectionConfig = rmqConnectionConfig;
        this.queueName = queueName;
        this.usesCorrelationId = usesCorrelationId;
        this.deliveryDeserializer = deliveryDeserializer;
    }

    protected ConnectionFactory setupConnectionFactory() throws Exception {
        return this.rmqConnectionConfig.getConnectionFactory();
    }

    protected Connection setupConnection() throws Exception {
        return this.setupConnectionFactory().newConnection();
    }

    private Channel setupChannel(Connection connection) throws Exception {
        Channel chan = connection.createChannel();
        if (this.rmqConnectionConfig.getPrefetchCount().isPresent()) {
            chan.basicQos((Integer)this.rmqConnectionConfig.getPrefetchCount().get(), true);
        }

        return chan;
    }

    @VisibleForTesting
    protected void setupQueue() throws IOException {
//        Util.declareQueueDefaults(this.channel, this.queueName);
    }

    public void open(Configuration config) throws Exception {
        super.open(config);

        try {
            this.connection = this.setupConnection();
            this.channel = this.setupChannel(this.connection);
            if (this.channel == null) {
                throw new RuntimeException("None of RabbitMQ channels are available");
            }

            //  this.channel.exchangeDeclare(queueName, "fanout", true);
//             this.consumer = new QueueingConsumer(this.channel);

//            this.setupQueue();
            Map<String, Object> arguments = new HashMap<String, Object>(16);
            //死信队列配置  ----------------
            arguments.put("x-dead-letter-exchange", "dead-letter-exchange");
            arguments.put("x-dead-letter-routing-key",queueName);
            this.channel.queueDeclare(queueName, true, false, false, arguments);

            this.consumer = new QueueingConsumer(this.channel);
            RuntimeContext runtimeContext = this.getRuntimeContext();
            if (runtimeContext instanceof StreamingRuntimeContext && ((StreamingRuntimeContext)runtimeContext).isCheckpointingEnabled()) {
                this.autoAck = false;
                this.channel.txSelect();
            } else {
                this.autoAck = true;
            }

            LOG.debug("Starting RabbitMQ source with autoAck status: " + this.autoAck);
            this.channel.basicConsume(this.queueName, this.autoAck, this.consumer);
        } catch (IOException var3) {
            IOUtils.closeAllQuietly(new AutoCloseable[]{this.channel, this.connection});
            throw new RuntimeException("Cannot create RMQ connection with " + this.queueName + " at " + this.rmqConnectionConfig.getHost(), var3);
        }

        this.deliveryDeserializer.open(RuntimeContextInitializationContextAdapters.deserializationAdapter(this.getRuntimeContext(), (metricGroup) -> {
            return metricGroup.addGroup("user");
        }));
        this.running = true;
    }

    public void close() throws Exception {
        super.close();
        Object exception = null;

        try {
            if (this.consumer != null && this.channel != null) {
                this.channel.basicCancel(this.consumer.getConsumerTag());
            }
        } catch (IOException var4) {
            exception = new RuntimeException("Error while cancelling RMQ consumer on " + this.queueName + " at " + this.rmqConnectionConfig.getHost(), var4);
        }

        try {
            IOUtils.closeAll(new AutoCloseable[]{this.channel, this.connection});
        } catch (IOException var3) {
            exception = (Exception)ExceptionUtils.firstOrSuppressed(new RuntimeException("Error while closing RMQ source with " + this.queueName + " at " + this.rmqConnectionConfig.getHost(), var3), (Throwable)exception);
        }

        if (exception != null) {
            throw (Exception)exception;
        }
    }

    private void processMessage(Delivery delivery, YTDRMQSource<OUT>.RMQCollectorImpl collector) throws IOException {
        BasicProperties properties = delivery.getProperties();
        byte[] body = delivery.getBody();
        Envelope envelope = delivery.getEnvelope();
        collector.setFallBackIdentifiers(properties.getCorrelationId(), envelope.getDeliveryTag());
        this.deliveryDeserializer.deserialize(envelope, properties, body, collector);
    }

    public void run(SourceContext<OUT> ctx) throws Exception {
        YTDRMQSource<OUT>.RMQCollectorImpl collector = new YTDRMQSource.RMQCollectorImpl(ctx);
        long timeout = this.rmqConnectionConfig.getDeliveryTimeout();

        while(this.running) {
            Delivery delivery = this.consumer.nextDelivery(timeout);
            synchronized(ctx.getCheckpointLock()) {
                if (delivery != null) {
                    this.processMessage(delivery, collector);
                }

                if (collector.isEndOfStreamSignalled()) {
                    this.running = false;
                    return;
                }
            }
        }

    }

    public void cancel() {
        this.running = false;
    }

    protected void acknowledgeSessionIDs(List<Long> sessionIds) {
        try {
            Iterator var2 = sessionIds.iterator();

            while(var2.hasNext()) {
                long id = (Long)var2.next();
                this.channel.basicAck(id, false);
            }

            this.channel.txCommit();
        } catch (IOException var5) {
            throw new RuntimeException("Messages could not be acknowledged during checkpoint creation.", var5);
        }
    }

    public TypeInformation<OUT> getProducedType() {
        return this.deliveryDeserializer.getProducedType();
    }

    private class RMQCollectorImpl implements RMQCollector<OUT> {
        private final SourceContext<OUT> ctx;
        private boolean endOfStreamSignalled;
        private String correlationId;
        private long deliveryTag;
        private boolean customIdentifiersSet;

        private RMQCollectorImpl(SourceContext<OUT> ctx) {
            this.endOfStreamSignalled = false;
            this.customIdentifiersSet = false;
            this.ctx = ctx;
        }

        public void collect(OUT record) {
            if (!this.customIdentifiersSet) {
                boolean newMessage = this.setMessageIdentifiers(this.correlationId, this.deliveryTag);
                if (!newMessage) {
                    return;
                }
            }

            if (this.isEndOfStream(record)) {
                this.endOfStreamSignalled = true;
            } else {
                this.ctx.collect(record);
            }
        }

        public void setFallBackIdentifiers(String correlationId, long deliveryTag) {
            this.correlationId = correlationId;
            this.deliveryTag = deliveryTag;
            this.customIdentifiersSet = false;
        }

        public boolean setMessageIdentifiers(String correlationId, long deliveryTag) {
            if (this.customIdentifiersSet) {
                throw new IllegalStateException("You can set only a single set of identifiers for a block of messages.");
            } else {
                this.customIdentifiersSet = true;
                if (!YTDRMQSource.this.autoAck) {
                    if (YTDRMQSource.this.usesCorrelationId) {
                        Preconditions.checkNotNull(correlationId, "RabbitMQ source was instantiated with usesCorrelationId set to true yet we couldn't extract the correlation id from it!");
                        if (!YTDRMQSource.this.addId(correlationId)) {
                            try {
                                YTDRMQSource.this.channel.basicReject(deliveryTag, false);
                                return false;
                            } catch (IOException var5) {
                                throw new RuntimeException("Message could not be acknowledged with basicReject.", var5);
                            }
                        }
                    }

                    YTDRMQSource.this.sessionIds.add(deliveryTag);
                }

                return true;
            }
        }

        boolean isEndOfStream(OUT record) {
            return this.endOfStreamSignalled || YTDRMQSource.this.deliveryDeserializer.isEndOfStream(record);
        }

        public boolean isEndOfStreamSignalled() {
            return this.endOfStreamSignalled;
        }

        public void close() {
        }
    }
}
