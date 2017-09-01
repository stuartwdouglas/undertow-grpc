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
import io.grpc.Status;
import io.grpc.internal.AbstractServerStream;
import io.grpc.internal.StatsTraceContext;
import io.grpc.internal.WritableBuffer;
import io.undertow.connector.PooledByteBuffer;
import io.undertow.io.IoCallback;
import io.undertow.io.Sender;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.protocol.http.HttpAttachments;
import io.undertow.util.HeaderMap;
import org.xnio.IoUtils;
import org.xnio.channels.StreamSourceChannel;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Stuart Douglas
 */
class UndertowServerStream extends AbstractServerStream {
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
            UndertowGrpcUtil.metadataToHeaderMap(exchange.getResponseHeaders(), headers);
        }

        public void writeFrame(@Nullable final WritableBuffer frame, boolean flush) {
            if (frame == null) {
                if (flush) {
                    doSend();
                }
                return;
            }
            UndertowWritableBuffer buf = (UndertowWritableBuffer) frame;
            if (queuedData == null) {
                queuedData = new ArrayList<>();
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
            if (trailers != null) {
                HeaderMap map = new HeaderMap();
                exchange.putAttachment(HttpAttachments.RESPONSE_TRAILERS, map);
                UndertowGrpcUtil.metadataToHeaderMap(map, trailers);
            }
            closed = true;
            if (ready) {
                sender.close();
            }
        }

        public void request(int numMessages) {
            exchange.getIoThread().execute(() -> {
                requestedMessageCount += numMessages;
                requestChannel.resumeReads();
                transportState.requestMessagesFromDeframer(numMessages);
            });
        }

        public void cancel(Status status) {
            IoUtils.safeClose(exchange.getConnection());
        }
    };

    UndertowServerStream(final HttpServerExchange exchange) {
        super(capacityHint -> new UndertowWritableBuffer(exchange.getConnection().getByteBufferPool().allocate()), StatsTraceContext.NOOP);
        this.exchange = exchange;
        this.sender = exchange.getResponseSender();
        requestChannel = exchange.getRequestChannel();
        requestChannel.getReadSetter().set(new ReadChannelListener(exchange.getConnection().getByteBufferPool(), (transportState::inboundDataReceived), (e) -> transportState.transportReportStatus(Status.fromThrowable(e))));
    }

    public Attributes getAttributes() {
        return exchange.getConnection().getAttachment(UndertowServerBuilder.ATTRIBUTES_ATTACHMENT_KEY);
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
