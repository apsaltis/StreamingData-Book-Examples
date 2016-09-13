package com.streamingdata.streaming.api;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO -- Add documentation for what the class does.
 */
public final class ChannelStreamManager {

    private static final Logger logger = LoggerFactory.getLogger(ChannelStreamManager.class);
   // private static final Object channelSubscriberLock = new Object();
    //private static final Object2ObjectOpenHashMap<String, StreamMessageConsumer> topicMessageConsumerMap = new Object2ObjectOpenHashMap<>();
    private static final ConcurrentHashMap<String,StreamMessageConsumer> topicMessageConsumerMap = new ConcurrentHashMap<>();
    private static final AttributeKey<String> STREAM_NAME = AttributeKey.valueOf("stream_name");
    private static final AttributeKey<String> GROUP_ID = AttributeKey.valueOf("groupId");


     static void stop() {
        for (Map.Entry<String, StreamMessageConsumer> topicMessageConsumer : topicMessageConsumerMap.entrySet()) {
            topicMessageConsumer.getValue().stop();
        }
    }

     static void removeSubscriber(final ChannelHandlerContext channelHandlerContext) {

         final String streamName = channelHandlerContext.channel().attr(STREAM_NAME).get();
         final String groupId = channelHandlerContext.channel().attr(GROUP_ID).get();
         final String subscriberKey = makeSubscriberKey(streamName, groupId);
        try {

                int remaining = topicMessageConsumerMap.get(subscriberKey).removeSubscriber(channelHandlerContext);
                if (0 == remaining) {
                    topicMessageConsumerMap.remove(subscriberKey);
                }



        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }
    }

     static void addSubscriber(final ChannelHandlerContext ctx) throws Exception {

        final String streamName = ctx.channel().attr(STREAM_NAME).get();
        final String groupId = ctx.channel().attr(GROUP_ID).get();
        final String subscriberKey = makeSubscriberKey(streamName, groupId);


            if(logger.isDebugEnabled()) {
                logger.debug("\t Registering Channel: Remote host: " + ctx.channel().remoteAddress().toString());
                logger.debug("\t Stream Name: " + streamName);
                logger.debug("\t Group ID: " + groupId);
                logger.debug("\t Subscriber Key: " + subscriberKey);
            }

            //the channel is not already registered so spin up a connection to Kafka
            if (!topicMessageConsumerMap.containsKey(subscriberKey)) {
                //process up the SDP Consumer
                StreamMessageConsumer streamMessageConsumer = new StreamMessageConsumer(streamName, groupId,ctx);
                topicMessageConsumerMap.put(subscriberKey, streamMessageConsumer);
                streamMessageConsumer.process();
            } else {
                topicMessageConsumerMap.get(subscriberKey).addSubscriber(ctx);
            }


    }

    private static String makeSubscriberKey(final String topicName, final String groupId) {
        return groupId + "_" + topicName;
    }

}
