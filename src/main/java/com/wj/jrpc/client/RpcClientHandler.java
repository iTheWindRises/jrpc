package com.wj.jrpc.client;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.net.SocketAddress;

/**
 * @program jrpc
 * @description:
 * @author: zhangwj
 * @create: 2021/4/21 10:03 下午
 */
public class RpcClientHandler extends SimpleChannelInboundHandler<Object> {

    private Channel channel;

    private SocketAddress remotePeer;

    public SocketAddress getRemotePeer() {
        return remotePeer;
    }

    /**
     * 当通道激活触发此方法
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        remotePeer = channel.remoteAddress();
    }

    /**
     * 注册后触发此方法
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        channel = ctx.channel();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {

    }

    /**
     * 关闭连接
     */
    public void close() {
        channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }

}
