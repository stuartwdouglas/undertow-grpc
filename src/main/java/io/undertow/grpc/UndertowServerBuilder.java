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

import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.ServerStreamTracer;
import io.grpc.Status;
import io.grpc.internal.AbstractServerImplBuilder;
import io.grpc.internal.AbstractServerStream;
import io.grpc.internal.GrpcUtil;
import io.grpc.internal.InternalServer;
import io.grpc.internal.LogId;
import io.grpc.internal.ServerListener;
import io.grpc.internal.ServerTransport;
import io.grpc.internal.ServerTransportListener;
import io.grpc.internal.StatsTraceContext;
import io.grpc.internal.WritableBuffer;
import io.grpc.internal.WritableBufferAllocator;
import io.undertow.connector.PooledByteBuffer;
import io.undertow.io.IoCallback;
import io.undertow.io.Sender;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.protocol.http.HttpAttachments;
import io.undertow.util.AttachmentKey;
import io.undertow.util.HeaderMap;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import org.xnio.ChannelListener;
import org.xnio.IoUtils;
import org.xnio.channels.StreamSourceChannel;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Stuart Douglas
 */
public class UndertowServerBuilder extends AbstractServerImplBuilder<UndertowServerBuilder> {

    private volatile ServerTransportListener serverListener;

    private static final AttachmentKey<Attributes> ATTRIBUTES_ATTACHMENT_KEY = AttachmentKey.create(Attributes.class);

    private final HttpHandler handler = new HttpHandler() {
        public void handleRequest(final HttpServerExchange exchange) throws Exception {

            exchange.dispatch(exchange.getIoThread(), new Runnable() {
                public void run() {

                    final Attributes existing = exchange.getConnection().getAttachment(ATTRIBUTES_ATTACHMENT_KEY);
                    if (existing == null) {
                        Attributes attributes = serverListener.transportReady(Attributes.EMPTY);
                        exchange.getConnection().putAttachment(ATTRIBUTES_ATTACHMENT_KEY, attributes);
                    }
                    Metadata headers = new Metadata();
                    for (HeaderValues header : exchange.getRequestHeaders()) {
                        headers.put(Metadata.Key.of(header.getHeaderName().toString(), Metadata.ASCII_STRING_MARSHALLER), header.getFirst());
                    }
                    serverListener.streamCreated(new UndertowServerStream(exchange), exchange.getRequestPath().substring(1), headers);

                }
            });

        }
    };


    protected InternalServer buildTransportServer(List<ServerStreamTracer.Factory> streamTracerFactories) {
        return new InternalServer() {
            public void start(ServerListener listener) throws IOException {
                serverListener = listener.transportCreated(new ServerTransport() {
                    public void shutdown() {
                        //ignore
                    }

                    public void shutdownNow(Status reason) {
                        //ignore
                    }

                    public LogId getLogId() {
                        return LogId.allocate(UndertowServerBuilder.class.getName());
                    }
                });
            }

            public void shutdown() {

            }

            public int getPort() {
                return 0;
            }
        };
    }

    public UndertowServerBuilder useTransportSecurity(File certChain, File privateKey) {
        //ignore this for now, the builder does not actually set up the transport, just creates a handler
        return this;
    }

    static class UndertowServerStream extends AbstractServerStream {
        private final HttpServerExchange exchange;

        private List<PooledByteBuffer> queuedData;
        private boolean ready = true;

        private final Sender sender;

        private boolean closed;

        private int requestedMessageCount = 0;
        private final StreamSourceChannel requestChannel;

        private final TransportState transportState = new TransportState(Integer.MAX_VALUE, statsTraceContext()) {


            @Override
            protected void deframeFailed(Throwable cause) {
                //TODO: proper handling
                IoUtils.safeClose(exchange.getConnection());
            }

            public void bytesRead(int numBytes) {

            }

            @Override
            public void messageRead(InputStream is) {
                super.messageRead(is);
                requestedMessageCount--;
                if (requestedMessageCount == 0) {
                    requestChannel.suspendReads();
                }
            }


        };

        private final Sink sink = new Sink() {
            public void writeHeaders(Metadata headers) {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, GrpcUtil.CONTENT_TYPE_GRPC);
                for (String key : headers.keys()) {
                    //TODO: is there a better way to do this? Seems like a super slow way to do iteration
                    Iterable<String> all = headers.getAll(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
                    if (all != null) {
                        for (String val : all) {
                            exchange.getResponseHeaders().add(new HttpString(key), val);
                        }
                    }
                }
            }

            public void writeFrame(@Nullable final WritableBuffer frame, boolean flush) {
                if (frame == null) {
                    if (flush) {
                        doSend();
                    }
                    return;
                }
                UndertowBuffer buf = (UndertowBuffer) frame;
                if (queuedData == null) {
                    queuedData = new ArrayList<PooledByteBuffer>();
                }
                PooledByteBuffer buffer = buf.getBuffer();
                buffer.getBuffer().flip();
                queuedData.add(buffer);
                doSend();
            }

            private void doSend() {
                if (ready) {
                    final List<PooledByteBuffer> buffers;
                    final ByteBuffer[] data;
                    if (queuedData != null) {
                        buffers = queuedData;
                        queuedData = null;
                        data = new ByteBuffer[buffers.size()];
                        for (int i = 0; i < data.length; ++i) {
                            data[i] = buffers.get(i).getBuffer();
                        }
                    } else {
                        buffers = Collections.emptyList();
                        data = new ByteBuffer[0];
                    }
                    ready = false;
                    sender.send(data, new IoCallback() {
                        public void onComplete(HttpServerExchange exchange, Sender sender) {
                            for (PooledByteBuffer i : buffers) {
                                i.close();
                            }
                            ready = true;
                            if (closed) {
                                sender.close();

                                if (queuedData != null) {
                                    for (PooledByteBuffer i : queuedData) {
                                        i.close();
                                    }
                                    queuedData = null;
                                }
                            } else if (queuedData != null) {
                                doSend();
                            }
                        }

                        public void onException(HttpServerExchange exchange, Sender sender, IOException exception) {
                            for (PooledByteBuffer i : buffers) {
                                i.close();
                            }
                            if (queuedData != null) {
                                for (PooledByteBuffer i : queuedData) {
                                    i.close();
                                }
                                queuedData = null;
                            }

                            //TODO: not sure what to do here, the connection is pretty much hosed
                        }
                    });
                }

            }


            public void writeTrailers(Metadata trailers, boolean headersSent) {
                HeaderMap map;
                if (exchange.isResponseStarted()) {
                    map = new HeaderMap();
                    exchange.putAttachment(HttpAttachments.RESPONSE_TRAILERS, map);
                } else {
                    map = exchange.getResponseHeaders();
                }
                if (trailers != null) {
                    for (String key : trailers.keys()) {
                        Iterable<String> all = trailers.getAll(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
                        if (all != null) {
                            for (String val : all) {
                                map.add(new HttpString(key), val);
                            }
                        }
                    }
                }
                closed = true;
                if (ready) {
                    sender.close();
                }
            }

            public void request(int numMessages) {
                requestedMessageCount += numMessages;
                requestChannel.resumeReads();
                transportState.requestMessagesFromDeframer(numMessages);
            }

            public void cancel(Status status) {
                IoUtils.safeClose(exchange.getConnection());
            }
        };

        UndertowServerStream(final HttpServerExchange exchange) {
            super(new WritableBufferAllocator() {
                public WritableBuffer allocate(int capacityHint) {
                    return new UndertowBuffer(exchange.getConnection().getByteBufferPool().allocate());
                }
            }, StatsTraceContext.NOOP);
            this.exchange = exchange;
            this.sender = exchange.getResponseSender();
            requestChannel = exchange.getRequestChannel();
            requestChannel.getReadSetter().set(new ChannelListener<StreamSourceChannel>() {
                public void handleEvent(StreamSourceChannel channel) {
                    PooledByteBuffer pooled = exchange.getConnection().getByteBufferPool().allocate();
                    try {
                        int res = channel.read(pooled.getBuffer());
                        if(res == -1) {
                            pooled.getBuffer().flip();
                            transportState.inboundDataReceived(new UndertowReadableBuffer(pooled), true);
                        } else if(res == 0) {
                            pooled.close();
                        } else {
                            pooled.getBuffer().flip();
                            transportState.inboundDataReceived(new UndertowReadableBuffer(pooled), false);
                        }
                    } catch (Exception e) {
                        pooled.close();
                        //TODO: error handling
                        transportState.inboundDataReceived(null, true);
                    }

                }
            });
        }

        public Attributes getAttributes() {
            return exchange.getConnection().getAttachment(ATTRIBUTES_ATTACHMENT_KEY);
        }

        @Nullable
        public String getAuthority() {
            return exchange.getHostName();
        }

        protected TransportState transportState() {
            return transportState;
        }

        protected Sink abstractServerStreamSink() {
            return sink;
        }
    }

    public HttpHandler getHandler() {
        return handler;
    }
}
