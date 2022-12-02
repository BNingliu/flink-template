package com.ytd.template.util.mq;//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.utility.Utility;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

class QueueingConsumer extends DefaultConsumer {
    private final BlockingQueue<Delivery> queue;
    private volatile ShutdownSignalException shutdown;
    private volatile ConsumerCancelledException cancelled;
    private static final Delivery POISON = new Delivery((Envelope)null, (BasicProperties)null, (byte[])null);

    public QueueingConsumer(Channel channel) {
        this(channel, 2147483647);
    }

    public QueueingConsumer(Channel channel, int capacity) {
        super(channel);
        this.queue = new LinkedBlockingQueue(capacity);
    }

    private void checkShutdown() {
        if (this.shutdown != null) {
            throw (ShutdownSignalException)Utility.fixStackTrace(this.shutdown);
        }
    }

    private Delivery handle(Delivery delivery) {
        if (delivery == POISON || delivery == null && (this.shutdown != null || this.cancelled != null)) {
            if (delivery == POISON) {
                this.queue.add(POISON);
                if (this.shutdown == null && this.cancelled == null) {
                    throw new IllegalStateException("POISON in queue, but null shutdown and null cancelled. This should never happen, please report as a BUG");
                }
            }

            if (null != this.shutdown) {
                throw (ShutdownSignalException)Utility.fixStackTrace(this.shutdown);
            }

            if (null != this.cancelled) {
                throw (ConsumerCancelledException)Utility.fixStackTrace(this.cancelled);
            }
        }

        return delivery;
    }

    public Delivery nextDelivery() throws InterruptedException, ShutdownSignalException, ConsumerCancelledException {
        return this.handle((Delivery)this.queue.take());
    }

    public Delivery nextDelivery(long timeout) throws InterruptedException, ShutdownSignalException, ConsumerCancelledException {
        return this.nextDelivery(timeout, TimeUnit.MILLISECONDS);
    }

    public Delivery nextDelivery(long timeout, TimeUnit unit) throws InterruptedException, ShutdownSignalException, ConsumerCancelledException {
        return this.handle((Delivery)this.queue.poll(timeout, unit));
    }

    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        this.shutdown = sig;
        this.queue.add(POISON);
    }

    public void handleCancel(String consumerTag) throws IOException {
        this.cancelled = new ConsumerCancelledException();
        this.queue.add(POISON);
    }

    public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
        this.checkShutdown();
        this.queue.add(new Delivery(envelope, properties, body));
    }
}
