package com.streamingdata.streaming.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderNames.HOST;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


class MeetupTopNSocketServerHandler extends SimpleChannelInboundHandler<Object> {


    private WebSocketServerHandshaker handshaker;
    private static final Logger logger = Logger.getLogger(MeetupTopNSocketServerHandler.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String WEBSOCKET_PATH = "/streaming";
    private static String defaultHtmlPage;
    private static final ConcurrentHashMap<ChannelHandlerContext, StreamMessageConsumer> channelToConsumer = new ConcurrentHashMap<>();


    private static final FastThreadLocal<DateFormat> FORMAT = new FastThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss z");
        }
    };


    MeetupTopNSocketServerHandler() throws IOException {
        defaultHtmlPage = new String(Files.readAllBytes(Paths.get("index.html")));

    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.WRITER_IDLE) {
                logger.debug("Sending PingWebSocketFrame to --> [ " + ctx.channel().remoteAddress().toString() + "]");
                ctx.writeAndFlush(new PingWebSocketFrame());
            }
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpRequest) {
            handleHttpRequest(ctx, (FullHttpRequest) msg);
        } else if (msg instanceof WebSocketFrame) {
            handleWebSocketFrame(ctx, (WebSocketFrame) msg);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }


    private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest req) {


        // Handle a bad request.
        if (!req.decoderResult().isSuccess()) {
            sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST));
            return;
        }

        // Allow only GET methods.
        if (req.method() != GET) {
            sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, FORBIDDEN));
            return;
        }

        final String uriString = req.uri();
        // Send the demo page and favicon.ico
        if ("/".equals(uriString)) {
            ByteBuf content = HtmlPage();
            FullHttpResponse res = new DefaultFullHttpResponse(HTTP_1_1, OK, content);

            res.headers().set(CONTENT_TYPE, "text/html; charset=UTF-8");
            HttpUtil.setContentLength(res, content.readableBytes());

            sendHttpResponse(ctx, req, res);

        } else if ("/favicon.ico".equals(uriString)) {
            FullHttpResponse res = new DefaultFullHttpResponse(HTTP_1_1, NOT_FOUND);
            sendHttpResponse(ctx, req, res);

        } else if (uriString.startsWith(WEBSOCKET_PATH)) {

            // Handshake
            WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(
                    getWebSocketLocation(req), null, true);
            handshaker = wsFactory.newHandshaker(req);
            if (handshaker == null) {
                WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
            }
        } else {
            sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST));
        }

    }

    private ByteBuf HtmlPage() {

        try {
            return Unpooled.copiedBuffer(defaultHtmlPage, CharsetUtil.US_ASCII);
        } catch (Exception ex) {
            logger.error("Failed to read file");
            logger.error(ex.getMessage(), ex);
            return Unpooled.copiedBuffer("", CharsetUtil.US_ASCII);
        }

    }


    private void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {

        // Check for closing frame
        if (frame instanceof CloseWebSocketFrame) {
            logger.debug("CloseWebSocketFrame Request from [ " + ctx.channel().remoteAddress().toString() + "]");
            handshaker.close(ctx.channel(), (CloseWebSocketFrame) frame.retain());
            if (channelToConsumer.containsKey(ctx)) {
                channelToConsumer.get(ctx).stop();
            }
            ctx.channel().close();
            return;
        }
        if (frame instanceof PingWebSocketFrame) {
            logger.debug("PingWebSocketFrame Request from [ " + ctx.channel().remoteAddress().toString() + "]");
            ctx.channel().write(new PongWebSocketFrame(frame.content().retain()));
            return;
        }
        if (frame instanceof PongWebSocketFrame) {
            //most likely we sent the ping....
            logger.debug("PongWebSocketFrame Request from [ " + ctx.channel().remoteAddress().toString() + "]");
            return;
        }
        if (!(frame instanceof TextWebSocketFrame)) {
            throw new UnsupportedOperationException(String.format("%s frame types not supported", frame.getClass()
                    .getName()));
        }

        handleWebSocketRequest(ctx, frame);

    }


    private void handleWebSocketRequest(ChannelHandlerContext ctx, WebSocketFrame frame) {

        try {
            // Ignoring the input.....
            // You would access it this way:
            //final String jsonRequest = ((TextWebSocketFrame) frame).text();

            logger.debug("WebSocket Request from [ " + ctx.channel().remoteAddress().toString() + "]");


            //the channel is not already registered so spin up a connection to Kafka
            if (!channelToConsumer.containsKey(ctx)) {
                StreamMessageConsumer streamMessageConsumer = new StreamMessageConsumer("meetup-topn-rsvps",UUID.randomUUID().toString(), ctx);
                channelToConsumer.put(ctx, streamMessageConsumer);
                streamMessageConsumer.process();
            }
            TextWebSocketFrame returnframe = new TextWebSocketFrame(mapper.writeValueAsString("{response:success}"));
            ctx.channel().write(returnframe);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            ctx.channel().write((new TextWebSocketFrame("{\"type\":\"error\",\"response\":\"" + e.getMessage() + "\"}"))).addListener(ChannelFutureListener.CLOSE);
        }

    }


    private static void sendHttpResponse(ChannelHandlerContext ctx, FullHttpRequest req, FullHttpResponse response) {

        HttpHeaders headers = response.headers();
        headers.set(HttpHeaderNames.SERVER, "Streaming API Service");
        headers.set(HttpHeaderNames.DATE, FORMAT.get().format(new Date()));
        headers.set(HttpHeaderNames.CONTENT_LENGTH, Integer.toString(response.content().readableBytes()));

        // Send the response and close the connection if necessary.
        ctx.write(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) throws Exception {


        logger.error(e.getMessage(), e);

        //lets try and clean them up.
        try {
            Channel channel = ctx.channel();

            if (null != channel) {
                if (!channel.isOpen()) {
                    if (channelToConsumer.containsKey(ctx)) {
                        channelToConsumer.get(ctx).stop();
                    }

                }
                if (channel.isWritable() && channel.isActive()) {
                    TextWebSocketFrame frame = new TextWebSocketFrame("{\"type\":\"error\",\"response\":\"" + e.getMessage() + "\"}");
                    // Close the connection as soon as the error message is sent.
                    ctx.channel().writeAndFlush(frame);
                }
            }
        } catch (Exception ex) {
            //gulp, not much we can do --
            logger.error(ex.getMessage(), ex);
        }

    }


    private static String getWebSocketLocation(FullHttpRequest req) {
        String location = req.headers().get(HOST) + WEBSOCKET_PATH;
        return "ws://" + location;

    }

}
