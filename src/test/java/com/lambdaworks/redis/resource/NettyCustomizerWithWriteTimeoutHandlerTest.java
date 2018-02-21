package com.lambdaworks.redis.resource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lambdaworks.redis.AbstractRedisClientTest;
import com.lambdaworks.redis.ClientOptions;
import com.lambdaworks.redis.ClientOptions.DisconnectedBehavior;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.SocketOptions;
import com.lambdaworks.redis.api.sync.RedisCommands;
import com.lambdaworks.redis.protocol.CommandEncoder;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.WriteTimeoutException;
import io.netty.handler.timeout.WriteTimeoutHandler;

public class NettyCustomizerWithWriteTimeoutHandlerTest extends AbstractRedisClientTest {
    private static final Logger log = LoggerFactory.getLogger(NettyCustomizerWithWriteTimeoutHandlerTest.class);

    private static class MockUnstableNetworkHandler extends ChannelOutboundHandlerAdapter {
        private final AtomicBoolean stability = new AtomicBoolean(true);

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            if (stability.get()) {
                ctx.write(msg, promise);
            } else {
                log.info("We lost the request.");
            }
        }

        public void set(boolean stability) {
            this.stability.set(stability);
        }
    }

    @Test
    public void test() {
        MockUnstableNetworkHandler networkHandler = new MockUnstableNetworkHandler();

        ClientResources res = DefaultClientResources
                .builder()
                .nettyCustomizer(new NettyCustomizer() {
                    @Override
                    public void afterBootstrapInitialized(Bootstrap bootstrap) {
                    }

                    @Override
                    public void afterChannelInitialized(Channel channel) {
                        ChannelPipeline pipeline = channel.pipeline();
                        String handlerName = find(pipeline, CommandEncoder.class);
                        pipeline.addAfter(handlerName, "WriteTimeoutHandler",
                                          new WriteTimeoutHandler(100, TimeUnit.MILLISECONDS));
                        pipeline.addFirst(networkHandler);
                    }
                })
                .build();
        SocketOptions socketOptions = SocketOptions.builder()
                                                   .connectTimeout(1000, TimeUnit.MILLISECONDS)
                                                   .keepAlive(true)
                                                   .tcpNoDelay(true)
                                                   .build();

        ClientOptions clientOptions = ClientOptions.builder()
                                                   .socketOptions(socketOptions)
                                                   .pingBeforeActivateConnection(false)
                                                   .autoReconnect(false)
                                                   .cancelCommandsOnReconnectFailure(true)
                                                   .suspendReconnectOnProtocolFailure(true)
                                                   .requestQueueSize(100)
                                                   .disconnectedBehavior(DisconnectedBehavior.REJECT_COMMANDS)
                                                   .build();

        RedisClient client = RedisClient.create(res, RedisURI.Builder.redis(host, port).build());
        client.setOptions(clientOptions);
        client.setDefaultTimeout(1, TimeUnit.SECONDS);
        RedisCommands<String, String> redisCommands = client.connect().sync();

        redisCommands.set("a", "1");
        assertThat(redisCommands.get("a")).isEqualTo("1");
        networkHandler.set(true);
        redisCommands.get("a");
        networkHandler.set(false);
        assertThatThrownBy(() -> redisCommands.get("a")).hasCauseInstanceOf(WriteTimeoutException.class);
    }

    private static String find(ChannelPipeline pipeline, Class<? extends ChannelHandler> type) {
        for (Map.Entry<String, ChannelHandler> e : pipeline) {
            if (type.isInstance(e.getValue())) {
                return e.getKey();
            }
        }
        throw new RuntimeException("cannot find");
    }
}
