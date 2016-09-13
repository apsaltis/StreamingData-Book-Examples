package com.streamingdata.streaming.api;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

final public class StreamMessageHandler {

//    private static final Logger logger = LoggerFactory.getLogger(StreamMessageHandler.class);
//    private final String topic;
//
//    public StreamMessageHandler(final String topic) {
//        this.topic = topic;
//    }
//
//    void processMessage(ChannelHandlerContext channelHandlerContext, final String jsonMessage) throws Exception {
//
//        try {
//                        sendMessageToListener(channelHandlerContext,jsonMessage);
//
//        } catch (Throwable t) {
//            logger.error(t.getMessage(), t);
//
//        }
//
//    }
//
//
//    private void sendMessageToListener(final String jsonMessage, ChannelHandlerContext channelHandlerContext, Channel channel) {
//        if (null != channel) {
//
//            if (!channel.isOpen()) {
//                logger.warn("Closing channel that is no longer connected. Remote Address: " + channel.remoteAddress().toString());
//                ChannelStreamManager.removeSubscriber(channelHandlerContext);
//
//            }
//            if (channel.isWritable()) {
//                channel.writeAndFlush(new TextWebSocketFrame(jsonMessage));
//
//            } else {
//                logger.debug("Channel: " + channel.remoteAddress().toString() + "Topic: " + topic + " is NOT WRITABLE!!");
//            }
//        }
//    }


}
