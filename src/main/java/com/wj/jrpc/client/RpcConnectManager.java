package com.wj.jrpc.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @program jrpc
 * @description: 连接管理器
 * @author: zhangwj
 * @create: 2021/4/21 9:12 下午
 */
@Slf4j
public class RpcConnectManager {

    private static volatile RpcConnectManager RPC_CONNECT_MANAGER = new RpcConnectManager();

    private Map<SocketAddress, RpcClientHandler> connectHandlerMap = new ConcurrentHashMap<>();

    // 连接成功的处理器列表
    private CopyOnWriteArrayList<RpcClientHandler> connectedHandlerList = new CopyOnWriteArrayList<>();

    private ReentrantLock lock = new ReentrantLock();

    private Condition connectCondition = lock.newCondition();

    private EventLoopGroup eventLoopGroup = new NioEventLoopGroup(2);

    private ThreadPoolExecutor threadPoolExecutor =
            new ThreadPoolExecutor(16, 16, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1024));

    private RpcConnectManager(){}

    public static RpcConnectManager getInstance() {
        return RPC_CONNECT_MANAGER;
    }

    public void connect(final String ...serverAddress) {
        List<String> address = List.of(serverAddress);
        updateConnectedServer(address);
    }

    /**
     * 更新缓存信息并异步发起连接
     * @param allServerAddress
     */
    public void updateConnectedServer(List<String> allServerAddress) {
        if (allServerAddress == null || allServerAddress.isEmpty()) {
            log.error("没有可解析地址");
            clearConnected();
        }
        Set<InetSocketAddress> serverNodeSet = new HashSet<>();
        for (int i = 0; i < allServerAddress.size(); i++) {
            String[] ipPort = allServerAddress.get(i).split(":");
            if (ipPort.length != 2) {
                continue;
            }
            InetSocketAddress remotePeer = new InetSocketAddress(ipPort[0], Integer.parseInt(ipPort[1]));
            serverNodeSet.add(remotePeer);
        }

        // 2.调用建立连接方法 发起连接
        for (InetSocketAddress inetSocketAddress : serverNodeSet) {
            if (!connectHandlerMap.containsKey(inetSocketAddress)) {
                connectAsync(inetSocketAddress);
            }
        }

        // 3.删除旧的连接
        for (int i = 0; i < connectedHandlerList.size(); i++) {
            RpcClientHandler rpcClientHandler = connectedHandlerList.get(i);
            SocketAddress remotePeer = rpcClientHandler.getRemotePeer();
            if (!serverNodeSet.contains(remotePeer)) {
                log.info("删除旧连接" + remotePeer);
                RpcClientHandler handler = connectHandlerMap.get(remotePeer);
                if (handler != null) {
                    handler.close();
                    connectHandlerMap.remove(remotePeer);
                }
                connectedHandlerList.remove(remotePeer);
            }
        }
    }

    /**
     * 异步发起连接
     * @param remotePeer
     */
    private void connectAsync(InetSocketAddress remotePeer) {
        threadPoolExecutor.execute(() -> {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new RpcClientInitializer());

            connect(bootstrap, remotePeer);
        });
    }

    private void connect(Bootstrap bootstrap, InetSocketAddress remotePeer) {
        ChannelFuture channelFuture = bootstrap.connect(remotePeer);
        // 2.连接失败时候添加监听, 清除资源后发起重连操作
        channelFuture.channel().closeFuture().addListener((ChannelFuture future) -> {
            log.info("远程地址channel关闭: " + remotePeer);
            future.channel().eventLoop().schedule(()-> {
                log.warn("连接失败" + remotePeer);
                clearConnected();
                // TODO 死循环
                connect(bootstrap, remotePeer);
            }, 3, TimeUnit.SECONDS);
        });
        // 3.连接成功的时候添加监听, 把连接放入缓存
        channelFuture.addListener((ChannelFuture future) -> {
            if (future.isSuccess()) {
                log.info("连接成功" + remotePeer);
                RpcClientHandler handler = future.channel().pipeline().get(RpcClientHandler.class);
                addHandler(handler);
            }
        });
    }

    /**
     * 添加到缓存中
     * @param handler
     */
    private void addHandler(RpcClientHandler handler) {
        connectedHandlerList.add(handler);
        connectHandlerMap.put(handler.getRemotePeer(), handler);
        signalAvailableHandler();
    }

    /**
     * 关闭连接
     */
    private void clearConnected() {
        for (RpcClientHandler rpcClientHandler : connectedHandlerList) {
            // 通过handler找到remotePeer
            SocketAddress remotePeer = rpcClientHandler.getRemotePeer();
            RpcClientHandler handler = connectHandlerMap.get(remotePeer);
            if (handler != null) {
                handler.close();
                connectHandlerMap.remove(remotePeer);
            }

        }
        connectedHandlerList.clear();
    }

    /**
     * 唤醒另一端线程 告知新连接接入
     */
    private void signalAvailableHandler() {
        lock.lock();
        try {
            connectCondition.signalAll();
        }finally {
            lock.unlock();
        }
    }
}
