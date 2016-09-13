package com.streamingdata.streaming.api;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


final class ChannelStreamManager {

    private static final Logger logger = LoggerFactory.getLogger(ChannelStreamManager.class);
    private static final ConcurrentHashMap<String, StreamMessageConsumer> topicMessageConsumerMap = new ConcurrentHashMap<>();
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
        final String mapKey = makeMapKey(streamName, groupId);


        try {

            int remaining = topicMessageConsumerMap.get(mapKey).removeSubscriber(channelHandlerContext);
            if (0 == remaining) {
                topicMessageConsumerMap.remove(mapKey);
            }


        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }
    }

    static void addSubscriber(final ChannelHandlerContext ctx) throws Exception {

        final String streamName = ctx.channel().attr(STREAM_NAME).get();
        final String groupId = ctx.channel().attr(GROUP_ID).get();
        final String mapKey = makeMapKey(streamName, groupId);


        //the channel is not already registered so spin up a connection to Kafka
        if (!topicMessageConsumerMap.containsKey(mapKey)) {
            StreamMessageConsumer streamMessageConsumer = new StreamMessageConsumer(streamName, groupId, ctx);
            topicMessageConsumerMap.put(mapKey, streamMessageConsumer);
            streamMessageConsumer.process();
        } else {
            topicMessageConsumerMap.get(mapKey).addSubscriber(ctx);
        }


    }

    private static String makeMapKey(final String topicName, final String groupId) {
        return groupId + "_" + topicName;
    }

}
