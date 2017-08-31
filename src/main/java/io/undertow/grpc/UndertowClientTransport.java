/*
 * Copyright 2017 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package io.undertow.grpc;

import com.google.common.base.Preconditions;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.internal.ClientStream;
import io.grpc.internal.ConnectionClientTransport;
import io.grpc.internal.FailingClientStream;
import io.grpc.internal.GrpcUtil;
import io.grpc.internal.KeepAliveManager;
import io.grpc.internal.LogId;
import io.grpc.internal.SharedResourceHolder;
import io.grpc.internal.StatsTraceContext;
import io.grpc.internal.WritableBuffer;
import io.grpc.internal.WritableBufferAllocator;
import io.undertow.UndertowOptions;
import io.undertow.client.ClientCallback;
import io.undertow.client.ClientConnection;
import io.undertow.client.UndertowClient;
import io.undertow.connector.ByteBufferPool;
import io.undertow.server.DefaultByteBufferPool;
import org.xnio.IoUtils;
import org.xnio.OptionMap;
import org.xnio.XnioWorker;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static io.grpc.internal.GrpcUtil.TIMER_SERVICE;

/**
 * @author Stuart Douglas
 */
class UndertowClientTransport implements ConnectionClientTransport {

    private final LogId logId = LogId.allocate(getClass().getName());

    private ClientConnection clientConnection;

    private ByteBufferPool bufferPool;
    private final WritableBufferAllocator allocator = new WritableBufferAllocator() {
        @Override
        public WritableBuffer allocate(int capacityHint) {
            return new UndertowWritableBuffer(clientConnection.getBufferPool().allocate());
        }
    };

    private final InetSocketAddress address;
    private final String defaultAuthority;
    private final String userAgent;
    private Listener listener;
    private final XnioWorker executor;
    private final int maxMessageSize;
    /**
     * Indicates the transport is in go-away state: no new streams will be processed, but existing
     * streams may continue.
     */
    @GuardedBy("lock")
    private Status goAwayStatus;
    private final Object lock = new Object();


    private final SSLContext sslContext;
    private ScheduledExecutorService scheduler;
    private KeepAliveManager keepAliveManager;
    private boolean enableKeepAlive;
    private long keepAliveTimeNanos;
    private long keepAliveTimeoutNanos;
    private boolean keepAliveWithoutCalls;
    private int streamCount;

    UndertowClientTransport(InetSocketAddress address, String authority, @Nullable String userAgent,
                            XnioWorker worker, @Nullable SSLContext sslContext,
                            int maxMessageSize) {
        this.address = Preconditions.checkNotNull(address, "address");
        this.defaultAuthority = authority;
        this.maxMessageSize = maxMessageSize;
        this.executor = Preconditions.checkNotNull(worker, "worker");
        this.sslContext = sslContext;
        this.userAgent = GrpcUtil.getGrpcUserAgent("undertow", userAgent);
    }

    /**
     * Enable keepalive with custom delay and timeout.
     */
    void enableKeepAlive(boolean enable, long keepAliveTimeNanos,
                         long keepAliveTimeoutNanos, boolean keepAliveWithoutCalls) {
        enableKeepAlive = enable;
        this.keepAliveTimeNanos = keepAliveTimeNanos;
        this.keepAliveTimeoutNanos = keepAliveTimeoutNanos;
        this.keepAliveWithoutCalls = keepAliveWithoutCalls;
    }

    @Override
    public void ping(final PingCallback callback, Executor executor) {
        long time = System.nanoTime();
        clientConnection.sendPing(new ClientConnection.PingListener() {
            @Override
            public void acknowledged() {
                executor.execute(() -> callback.onSuccess(System.nanoTime() - time));
            }

            @Override
            public void failed(IOException e) {
                executor.execute(() -> callback.onFailure(e));
            }
        }, -1, null);
    }

    @Override
    public ClientStream newStream(final MethodDescriptor<?, ?> method,
                                  final Metadata headers, CallOptions callOptions) {
        if (goAwayStatus != null) {
            return new FailingClientStream(goAwayStatus);
        }
        Preconditions.checkNotNull(method, "method");
        Preconditions.checkNotNull(headers, "headers");
        StatsTraceContext statsTraceCtx = StatsTraceContext.newClientContext(callOptions, headers);
        UndertowClientStream undertowClientStream = new UndertowClientStream(clientConnection, allocator, method, headers, callOptions, getAttributes(), userAgent, maxMessageSize, statsTraceCtx, this);
        undertowClientStream.setAuthority(defaultAuthority);
        return undertowClientStream;
    }

    @Override
    public ClientStream newStream(final MethodDescriptor<?, ?> method, final Metadata
            headers) {
        return newStream(method, headers, CallOptions.DEFAULT);
    }

    @Override
    public Runnable start(Listener listener) {
        this.listener = Preconditions.checkNotNull(listener, "listener");
        this.bufferPool = new DefaultByteBufferPool(true, 2048);//TODO: configurable
        if (enableKeepAlive) {
            //TODO: this should just use the IO thread
            scheduler = SharedResourceHolder.get(TIMER_SERVICE);
            keepAliveManager = new KeepAliveManager(
                    new KeepAliveManager.ClientKeepAlivePinger(this), scheduler, keepAliveTimeNanos, keepAliveTimeoutNanos,
                    keepAliveWithoutCalls);
            keepAliveManager.onTransportStarted();
        }
        try {
            UndertowClient.getInstance().connect(new ClientCallback<ClientConnection>() {
                @Override
                public void completed(ClientConnection result) {
                    clientConnection = result;
                    listener.transportReady();

                }

                @Override
                public void failed(IOException e) {
                    listener.transportShutdown(Status.fromThrowable(e));
                }
            }, new URI(sslContext == null ? "http" : "https", null, address.getHostString(), address.getPort(), "/", null, null), executor, bufferPool, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        return () -> {
        };
    }

    @Override
    public String toString() {
        return getLogId() + "(" + address + ")";
    }

    @Override
    public LogId getLogId() {
        return logId;
    }

    @Override
    public void shutdown() {
        synchronized (lock) {
            if (goAwayStatus != null) {
                return;
            }
            goAwayStatus = Status.UNAVAILABLE.withDescription("Transport stopped");
            listener.transportShutdown(goAwayStatus);
            if (streamCount == 0) {
                IoUtils.safeClose(clientConnection);
                listener.transportTerminated();
            }
        }
    }

    @Override
    public void shutdownNow(Status reason) {
        shutdown();
        IoUtils.safeClose(clientConnection);
        listener.transportTerminated();
    }

    @Override
    public Attributes getAttributes() {
        // TODO fix this
        return Attributes.EMPTY;
    }

    void streamCreated() {
        synchronized (lock) {
            if (streamCount++ == 0) {
                listener.transportInUse(true);
            }
        }
    }

    void streamDestroyed() {
        synchronized (lock) {
            if (--streamCount == 0) {
                listener.transportInUse(false);
                if (goAwayStatus != null) {
                    IoUtils.safeClose(clientConnection);
                }
            }
        }

    }
}
