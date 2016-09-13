package com.streamingdata.streaming.api;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;


public final class StreamingDataService {

    private static final int PORT = 8080;
    private static final String bindAddress = "127.0.0.1";

    public static void main(String[] args) throws Exception {

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
                    .childHandler(new  ChannelInitializer<SocketChannel>() {
                                      @Override
                                      public void initChannel(SocketChannel ch) throws Exception {
                                          ChannelPipeline pipeline = ch.pipeline();
                                          pipeline.addLast(new HttpServerCodec());
                                          pipeline.addLast(new HttpObjectAggregator(65536));
                                          pipeline.addLast(new WebSocketServerCompressionHandler());
                                          pipeline.addLast(new WebSocketServerProtocolHandler("/streaming", null, true));
                                          pipeline.addLast(new MeetupTopNSocketServerHandler());

                                      }
                                  });

            Channel channel = bootstrap.bind(bindAddress, PORT).sync().channel();

            System.out.println("Open your web browser and navigate to http://" + bindAddress + ":" + PORT + '/');

            channel.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

    }


}

