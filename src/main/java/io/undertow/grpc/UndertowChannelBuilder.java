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
import io.grpc.Internal;
import io.grpc.NameResolver;
import io.grpc.internal.AbstractManagedChannelImplBuilder;
import io.grpc.internal.AtomicBackoff;
import io.grpc.internal.ClientTransportFactory;
import io.grpc.internal.ConnectionClientTransport;
import io.grpc.internal.GrpcUtil;
import io.grpc.internal.KeepAliveManager;
import org.xnio.XnioWorker;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

import static io.grpc.internal.GrpcUtil.DEFAULT_KEEPALIVE_TIMEOUT_NANOS;
import static io.grpc.internal.GrpcUtil.KEEPALIVE_TIME_NANOS_DISABLED;

/**
 * @author Stuart Douglas
 */
public class UndertowChannelBuilder extends AbstractManagedChannelImplBuilder<UndertowChannelBuilder> {

    private static final long AS_LARGE_AS_INFINITE = TimeUnit.DAYS.toNanos(1000L);

    /**
     * Creates a new builder for the given server host and port.
     */
    public static UndertowChannelBuilder forAddress(String host, int port) {
        return new UndertowChannelBuilder(host, port);
    }

    /**
     * Creates a new builder for the given target that will be resolved by
     * {@link io.grpc.NameResolver}.
     */
    public static UndertowChannelBuilder forTarget(String target) {
        return new UndertowChannelBuilder(target);
    }

    private XnioWorker worker;

    private SSLContext sslContext;
    private NegotiationType negotiationType = NegotiationType.TLS;
    private long keepAliveTimeNanos = KEEPALIVE_TIME_NANOS_DISABLED;
    private long keepAliveTimeoutNanos = DEFAULT_KEEPALIVE_TIMEOUT_NANOS;
    private boolean keepAliveWithoutCalls;

    protected UndertowChannelBuilder(String host, int port) {
        this(GrpcUtil.authorityFromHostAndPort(host, port));
    }

    private UndertowChannelBuilder(String target) {
        super(target);
    }

    /**
     * Set the worker that will be used for this connection
     * <p>
     * <p>The channel does not take ownership of the given worker. It is the caller' responsibility
     * to shutdown the worker when appropriate.
     */
    public final UndertowChannelBuilder worker(@Nullable XnioWorker transportExecutor) {
        this.worker = transportExecutor;
        return this;
    }

    /**
     * Sets the negotiation type for the HTTP/2 connection.
     * <p>
     * <p>If TLS is enabled a default {@link SSLContext} is created using <code>SSLContext.getInstance("TLSv1.2")</code>
     * <p>
     * <p>Default: <code>TLS</code>
     */
    public final UndertowChannelBuilder negotiationType(NegotiationType type) {
        negotiationType = Preconditions.checkNotNull(type, "type");
        return this;
    }

    /**
     * Sets the time without read activity before sending a keepalive ping. An unreasonably small
     * value might be increased, and {@code Long.MAX_VALUE} nano seconds or an unreasonably large
     * value will disable keepalive. Defaults to infinite.
     * <p>
     * <p>Clients must receive permission from the service owner before enabling this option.
     * Keepalives can increase the load on services and are commonly "invisible" making it hard to
     * notice when they are causing excessive load. Clients are strongly encouraged to use only as
     * small of a value as necessary.
     */
    public UndertowChannelBuilder keepAliveTime(long keepAliveTime, TimeUnit timeUnit) {
        Preconditions.checkArgument(keepAliveTime > 0L, "keepalive time must be positive");
        keepAliveTimeNanos = timeUnit.toNanos(keepAliveTime);
        keepAliveTimeNanos = KeepAliveManager.clampKeepAliveTimeInNanos(keepAliveTimeNanos);
        if (keepAliveTimeNanos >= AS_LARGE_AS_INFINITE) {
            // Bump keepalive time to infinite. This disables keepalive.
            keepAliveTimeNanos = KEEPALIVE_TIME_NANOS_DISABLED;
        }
        return this;
    }

    /**
     * Sets the time waiting for read activity after sending a keepalive ping. If the time expires
     * without any read activity on the connection, the connection is considered dead. An unreasonably
     * small value might be increased. Defaults to 20 seconds.
     * <p>
     * <p>This value should be at least multiple times the RTT to allow for lost packets.
     */
    public UndertowChannelBuilder keepAliveTimeout(long keepAliveTimeout, TimeUnit timeUnit) {
        Preconditions.checkArgument(keepAliveTimeout > 0L, "keepalive timeout must be positive");
        keepAliveTimeoutNanos = timeUnit.toNanos(keepAliveTimeout);
        keepAliveTimeoutNanos = KeepAliveManager.clampKeepAliveTimeoutInNanos(keepAliveTimeoutNanos);
        return this;
    }

    /**
     * Sets whether keepalive will be performed when there are no outstanding RPC on a connection.
     * Defaults to {@code false}.
     * <p>
     * <p>Clients must receive permission from the service owner before enabling this option.
     * Keepalives on unused connections can easily accidentally consume a considerable amount of
     * bandwidth and CPU.
     *
     * @see #keepAliveTime(long, TimeUnit)
     */
    public UndertowChannelBuilder keepAliveWithoutCalls(boolean enable) {
        keepAliveWithoutCalls = enable;
        return this;
    }

    /**
     * Override the default {@link SSLContext} and enable {@link NegotiationType#TLS}
     * negotiation.
     * <p>
     * <p>By default, when TLS is enabled, <code>SSLSocketFactory.getDefault()</code> will be used.
     * <p>
     * <p>{@link NegotiationType#TLS} will be applied by calling this method.
     */
    public final UndertowChannelBuilder sslContext(SSLContext factory) {
        this.sslContext = factory;
        negotiationType(NegotiationType.TLS);
        return this;
    }

    @Override
    public final UndertowChannelBuilder usePlaintext(boolean skipNegotiation) {
        if (skipNegotiation) {
            negotiationType(NegotiationType.PRIOR_KNOWLEDGE);
        } else {
            negotiationType(NegotiationType.UPGRADDE);
        }
        return this;
    }

    @Override
    @Internal
    protected final ClientTransportFactory buildTransportFactory() {
        boolean enableKeepAlive = keepAliveTimeNanos != KEEPALIVE_TIME_NANOS_DISABLED;
        return new UndertowTransportFactory(worker,
                createSslContext(), maxInboundMessageSize(), enableKeepAlive,
                keepAliveTimeNanos, keepAliveTimeoutNanos, keepAliveWithoutCalls);
    }

    private SSLContext createSslContext() {
        if (negotiationType == NegotiationType.TLS) {
            if (sslContext != null) {
                return sslContext;
            }
            try {
                return SSLContext.getInstance("TLSv1.2");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    @Override
    protected Attributes getNameResolverParams() {
        int defaultPort;
        switch (negotiationType) {
            case PRIOR_KNOWLEDGE:
            case UPGRADDE:
                defaultPort = GrpcUtil.DEFAULT_PORT_PLAINTEXT;
                break;
            case TLS:
                defaultPort = GrpcUtil.DEFAULT_PORT_SSL;
                break;
            default:
                throw new AssertionError(negotiationType + " not handled");
        }
        return Attributes.newBuilder()
                .set(NameResolver.Factory.PARAMS_DEFAULT_PORT, defaultPort).build();
    }

    /**
     * Creates OkHttp transports. Exposed for internal use, as it should be private.
     */
    @Internal
    static final class UndertowTransportFactory implements ClientTransportFactory {

        private final XnioWorker worker;
        @Nullable
        private final SSLContext sslContext;
        private final int maxMessageSize;
        private final boolean enableKeepAlive;
        private final AtomicBackoff keepAliveTimeNanos;
        private final long keepAliveTimeoutNanos;
        private final boolean keepAliveWithoutCalls;
        private boolean closed;

        private UndertowTransportFactory(XnioWorker worker,
                                         @Nullable SSLContext sslContext,
                                         int maxMessageSize,
                                         boolean enableKeepAlive,
                                         long keepAliveTimeNanos,
                                         long keepAliveTimeoutNanos,
                                         boolean keepAliveWithoutCalls) {
            this.sslContext = sslContext;
            this.maxMessageSize = maxMessageSize;
            this.enableKeepAlive = enableKeepAlive;
            this.keepAliveTimeNanos = new AtomicBackoff("keepalive time nanos", keepAliveTimeNanos);
            this.keepAliveTimeoutNanos = keepAliveTimeoutNanos;
            this.keepAliveWithoutCalls = keepAliveWithoutCalls;

            if (worker == null) {
                // The executor was unspecified, using the default executor.
                this.worker = XnioWorker.getContextManager().get();
            } else {
                this.worker = worker;
            }
        }

        @Override
        public ConnectionClientTransport newClientTransport(
                SocketAddress addr, String authority, @Nullable String userAgent) {
            if (closed) {
                throw new IllegalStateException("The transport factory is closed.");
            }
            InetSocketAddress proxyAddress = null;
            String proxy = System.getenv("GRPC_PROXY_EXP");
            if (proxy != null) {
                String[] parts = proxy.split(":", 2);
                int port = 80;
                if (parts.length > 1) {
                    port = Integer.parseInt(parts[1]);
                }
                proxyAddress = new InetSocketAddress(parts[0], port);
            }
            final AtomicBackoff.State keepAliveTimeNanosState = keepAliveTimeNanos.getState();
            Runnable tooManyPingsRunnable = keepAliveTimeNanosState::backoff;
            InetSocketAddress inetSocketAddr = (InetSocketAddress) addr;
            UndertowClientTransport transport = new UndertowClientTransport(inetSocketAddr, authority,
                    userAgent, worker, sslContext, maxMessageSize
            );
            if (enableKeepAlive) {
                transport.enableKeepAlive(
                        true, keepAliveTimeNanosState.get(), keepAliveTimeoutNanos, keepAliveWithoutCalls);
            }
            return transport;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
        }
    }

    public enum NegotiationType {
        TLS,
        PRIOR_KNOWLEDGE,
        UPGRADDE
    }
}
