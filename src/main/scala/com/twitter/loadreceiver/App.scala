package com.twitter.loadreceiver

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http.HttpObjectAggregator
import io.netty.handler.codec.http.HttpRequestDecoder
import io.netty.handler.codec.http.HttpResponseEncoder
import io.netty.handler.stream.ChunkedWriteHandler
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundMessageHandlerAdapter
import io.netty.handler.codec.http.DefaultFullHttpResponse
import io.netty.handler.codec.http.FullHttpRequest
import io.netty.util.CharsetUtil
import io.netty.handler.codec.http.HttpHeaders.Names._
import io.netty.handler.codec.http.HttpResponseStatus._
import io.netty.handler.codec.http.HttpVersion._

class HttpStaticFileServerHandler extends ChannelInboundMessageHandlerAdapter[FullHttpRequest] {

  override def messageReceived(ctx: ChannelHandlerContext, request: FullHttpRequest) {
    if (!request.getDecoderResult().isSuccess()) {
      val response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.copiedBuffer("failed to decode" + "\r\n", CharsetUtil.UTF_8))
      response.headers().set(CONTENT_TYPE, "text/plain charset=UTF-8")
      ctx.write(response).addListener(ChannelFutureListener.CLOSE)
      return
    } else {
      val response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.copiedBuffer("hello world" + "\r\n", CharsetUtil.UTF_8))
      response.headers().set(CONTENT_TYPE, "text/plain charset=UTF-8")
      ctx.write(response).addListener(ChannelFutureListener.CLOSE)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
  }
}


class HttpStaticFileServerInitializer extends ChannelInitializer[SocketChannel] {
  override def initChannel(ch: SocketChannel) {
    val pipeline = ch.pipeline()

    pipeline.addLast("decoder", new HttpRequestDecoder())
    pipeline.addLast("aggregator", new HttpObjectAggregator(65536))
    pipeline.addLast("encoder", new HttpResponseEncoder())
    pipeline.addLast("chunkedWriter", new ChunkedWriteHandler())

    pipeline.addLast("handler", new HttpStaticFileServerHandler())
  }
}

object App {
  def main(args: Array[String]) = {
    println("Starting")
    val bossGroup = new NioEventLoopGroup()
    val workerGroup = new NioEventLoopGroup()
    try {
      val b = new ServerBootstrap()
      b.group(bossGroup, workerGroup).channel(classOf[NioServerSocketChannel])
              .childHandler(new HttpStaticFileServerInitializer())

      b.bind(8080).sync().channel().closeFuture().sync()
    } finally {
      bossGroup.shutdown()
      workerGroup.shutdown()
    }
    println("end")
  }
}
