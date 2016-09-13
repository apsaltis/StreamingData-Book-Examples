package com.streamingdata.streaming.api;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

class StreamMessageConsumer {
    private static final Logger logger = LoggerFactory.getLogger(StreamMessageConsumer.class);
    private final String topic;
    private MessageProcessor messageProcessor;
    private Thread messageProcessingThread;
    private final ChannelHandlerContext channelHandlerContext;

    StreamMessageConsumer(final String topic, final String groupId, final ChannelHandlerContext ctx) {
        this.topic = topic;
        this.channelHandlerContext = ctx;

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

    final class MessageProcessor extends Thread {

        private volatile boolean done = false;
        private final KafkaConsumer<String, String> consumer;
        private final String topic;

        MessageProcessor(final String topic, final String groupId) {
            this.topic = topic;
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", groupId);
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList("meetup-topn-rsvps"));

        }

        @Override
        public void interrupt() {
            done = true;
            close();
        }


        @Override
        public synchronized void run() {
            while (!done) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {

                        logger.debug("Processing message for topic: {}", topic);

                        if (!channelHandlerContext.channel().isOpen()) {
                            logger.warn("Closing channel that is no longer connected. Remote Address: " + channelHandlerContext.channel().remoteAddress().toString());
                            close();

                        } else if (channelHandlerContext.channel().isWritable()) {
                            channelHandlerContext.channel().writeAndFlush(new TextWebSocketFrame(record.value()));

                        } else {
                            logger.debug("Channel: " + channelHandlerContext.channel().remoteAddress().toString() + "Topic: " + topic + " is NOT WRITABLE!!");
                        }

                    }
                } catch (IllegalStateException ex) {
                    //this may get thrown if we are about to poll and at the same moment our thread is closed.
                    //could be a good case to use a lock instead.
                }
            }
        }

        private void close() {
            if (consumer != null) {

                logger.debug("MessageProcessor --> Shutting down consumer");
                consumer.unsubscribe();
                consumer.close();
            }
        }

    }
}

