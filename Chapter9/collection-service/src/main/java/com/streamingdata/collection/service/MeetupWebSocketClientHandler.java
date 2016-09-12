package com.streamingdata.collection.service;
import io.netty.channel.*;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.CharsetUtil;

import java.util.UUID;

class MeetupWebSocketClientHandler extends SimpleChannelInboundHandler<Object> {

    private final WebSocketClientHandshaker handshaker;
    private ChannelPromise handshakeFuture;
    private final RSVPProducer rsvpProducer;

    MeetupWebSocketClientHandler(WebSocketClientHandshaker handshaker, RSVPProducer rsvpProducer) {
        this.handshaker = handshaker;
        this.rsvpProducer = rsvpProducer;
    }

    ChannelFuture handshakeFuture() {
        return handshakeFuture;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        handshakeFuture = ctx.newPromise();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        handshaker.handshake(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        System.out.println("WebSocket Client disconnected!");
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        Channel channel = ctx.channel();
        if (!handshaker.isHandshakeComplete()) {
            //we are now connected.
            handshaker.finishHandshake(channel, (FullHttpResponse) msg);
            handshakeFuture.setSuccess();
            System.out.println("WebSocket Client connected and ready to consume RSVPs!");
            return;
        }

        if (msg instanceof FullHttpResponse) {
            //if we get this, it is an error and we should throw an exception
            FullHttpResponse response = (FullHttpResponse) msg;
            throw new IllegalStateException(
                    "Unexpected FullHttpResponse (getStatus=" + response.status() +
                            ", content=" + response.content().toString(CharsetUtil.UTF_8) + ')');
        }

        WebSocketFrame frame = (WebSocketFrame) msg;
        if (frame instanceof TextWebSocketFrame) {
            TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;

            //this is the message we want -- at this point we can take the data and send it to
            //to the next tier. First we need to read the bytes from the ByteBuf
            final String messageKey = UUID.randomUUID().toString();
            final byte[] messagePayload = new byte[textFrame.content().readableBytes()];
            textFrame.content().readBytes(messagePayload);

            HybridMessageLogger.addEvent(messageKey,messagePayload);
            rsvpProducer.sendMessage(messageKey,messagePayload);

        } else if (frame instanceof CloseWebSocketFrame) {
            //we are being asked to close.
            channel.close();
            rsvpProducer.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause);
        }
        ctx.close();
    }
}
