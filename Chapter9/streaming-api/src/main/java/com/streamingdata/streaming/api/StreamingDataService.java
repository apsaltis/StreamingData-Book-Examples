package com.streamingdata.streaming.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;


public final class StreamingDataService {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int PORT = 8080;
    private static final String bindAddress = "127.0.0.1";

    public static void main(String[] args) throws Exception {
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);


        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                try {
                    System.out.println("Shutdown started...");
                    bossGroup.shutdownGracefully();
                    workerGroup.shutdownGracefully();
                    ChannelStreamManager.stop();
                    System.out.println("Shutdown finished, waiting for main thread to exit...");
                    mainThread.join();
                } catch (final Exception ex) {
                    ex.printStackTrace();
                }
            }
        });

        try {


            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.DEBUG))
                    .childHandler(new WebSocketServerInitializer());

            Channel ch = bootstrap.bind(bindAddress, PORT).sync().channel();

            System.out.println("Open your web browser and navigate to http://" + bindAddress + ":" + PORT + '/');

            ch.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            ChannelStreamManager.stop();
        }

    }


}

