package com.streamingdata.streaming.api;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class StreamMessageConsumer {
    private static final Logger logger = LoggerFactory.getLogger(StreamMessageConsumer.class);
    private final String topic;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final List<ChannelHandlerContext> subscriberList = new ArrayList<>();
    private MessageProcessor messageProcessor;
    private Thread messageProcessingThread;

    StreamMessageConsumer(final String topic, final String groupId, final ChannelHandlerContext ctx) {
        this.topic = topic;
        subscriberList.add(ctx);

        messageProcessor = new MessageProcessor(topic, groupId);
        messageProcessingThread = new Thread(messageProcessor);

    }


    void process() throws Exception {
        logger.debug("Starting Message Consumer Thread for topic:{} and this subscriberKey {}", this.topic);
        messageProcessingThread.start();
    }


    void stop() {
        try {
            messageProcessor.interrupt();
        } catch (Exception ex) {
            //gulp
            logger.debug("Caught exception interrupting kafka consumer thread!");
        }
    }


    int removeSubscriber(final ChannelHandlerContext channelHandlerContext) {

        try {
            readWriteLock.readLock().lock();
            if (subscriberList.contains(channelHandlerContext)) {
                readWriteLock.readLock().unlock();
                try {
                    readWriteLock.writeLock().lock();
                    subscriberList.remove(channelHandlerContext);

                    if (0 == subscriberList.size()) {
                        stop();
                    }
                } finally {
                    readWriteLock.writeLock().unlock();
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }

        return subscriberList.size();
    }

    void addSubscriber(final ChannelHandlerContext channelHandlerContext) throws Exception {


        logger.debug("\n\t Registering: " + channelHandlerContext.channel().remoteAddress().toString());

        //the channel is not already registered add them to the collection
        readWriteLock.readLock().lock();
        if (!subscriberList.contains(channelHandlerContext)) {
            readWriteLock.readLock().unlock();
            readWriteLock.writeLock().lock();
            try {
                subscriberList.add(channelHandlerContext);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } else {
            readWriteLock.readLock().unlock();
        }
    }

    final class MessageProcessor extends Thread {

        private volatile boolean done = false;
        private final KafkaConsumer<String, String> consumer;
      //  private StreamMessageHandler streamMessageHandler;
        private final String topic;

        MessageProcessor(final String topic, final String groupId) {
            this.topic = topic;
           // streamMessageHandler = new StreamMessageHandler(topic);
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", groupId);
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList("meetup-topn-rsvps"));

        }

        @Override
        public void interrupt() {
            done = true;
            if (consumer != null) {

                logger.debug("MessageProcessor --> Shutting down consumer");
                consumer.unsubscribe();
                consumer.close();
            }
        }


        @Override
        public synchronized void run() {

            try {
                while (!done) {

                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {


                        logger.debug("Processing message for topic: {}", topic);
                        final List<ChannelHandlerContext> activeSubscriberList = new ArrayList<>();
                        readWriteLock.readLock().lock();
                        try {
                            if (0 == subscriberList.size()) {
                                logger.debug("No Subscribers for message! {}", topic);
                                logger.debug("Message: {}", record.value());
                                return;
                            }

                            subscriberList.forEach(channelHandlerContext -> {

                                if (!channelHandlerContext.channel().isOpen()) {
                                    logger.warn("Closing channel that is no longer connected. Remote Address: " + channelHandlerContext.channel().remoteAddress().toString());
                                    ChannelStreamManager.removeSubscriber(channelHandlerContext);

                                }
                                else if (channelHandlerContext.channel().isWritable()) {
                                    channelHandlerContext.channel().writeAndFlush(new TextWebSocketFrame(record.value()));

                                } else {
                                    logger.debug("Channel: " + channelHandlerContext.channel().remoteAddress().toString() + "Topic: " + topic + " is NOT WRITABLE!!");
                                }

                            });

                            //activeSubscriberList = subscriberList.subList(0,subscriberList.size());
                        } finally {
                            readWriteLock.readLock().unlock();
                        }

                        //streamMessageHandler.processMessage(activeSubscriberList, record.value());


                    }


                }


            } catch (Exception ex) {
                logger.error("Exception caught in Kafka Thread!!!");
                logger.error(ex.getMessage(), ex);
            }
        }

    }
}

