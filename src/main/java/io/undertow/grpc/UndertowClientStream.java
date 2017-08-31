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
import io.grpc.CallOptions;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.internal.AbstractClientStream;
import io.grpc.internal.GrpcUtil;
import io.grpc.internal.Http2ClientStreamTransportState;
import io.grpc.internal.ReadableBuffer;
import io.grpc.internal.StatsTraceContext;
import io.grpc.internal.WritableBuffer;
import io.grpc.internal.WritableBufferAllocator;
import io.undertow.client.ClientCallback;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientExchange;
import io.undertow.client.ClientRequest;
import io.undertow.server.protocol.http.HttpAttachments;
import io.undertow.util.HeaderMap;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.xnio.ChannelExceptionHandler;
import org.xnio.ChannelListener;
import org.xnio.ChannelListeners;
import org.xnio.IoUtils;
import org.xnio.channels.StreamSinkChannel;
import org.xnio.channels.StreamSourceChannel;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.concurrent.LinkedBlockingQueue;

import static io.undertow.grpc.UndertowGrpcUtil.headerMapToMetadata;

/**
 * @author Stuart Douglas
 */
class UndertowClientStream extends AbstractClientStream {

    private static final Metadata.Key<String> STATUS = Metadata.Key.of(":status", Metadata.ASCII_STRING_MARSHALLER);

    private final ClientConnection clientConnection;
    private final CallOptions callOptions;
    private final MethodDescriptor<?, ?> method;
    private final Attributes attributes;
    private final String userAgent;
    private final UndertowClientTransport undertowClientTransport;


    private String authority;

    private ClientExchange clientExchange;

    private final ClientTransportState transportState;

    private int requestedMessages = 0;

    private final LinkedBlockingQueue<WriteData> dataQueue = new LinkedBlockingQueue<>();
    private boolean requestDone;
    private boolean responseDone;

    private StreamSourceChannel responseChannel;
    private final Sink sink = new Sink() {

        public void writeHeaders(Metadata metadata, @Nullable byte[] payload) {
            ClientRequest request = new ClientRequest();
            if (payload == null) {
                request.setMethod(Methods.POST);
                request.setPath("/" + method.getFullMethodName());
            } else {
                request.setMethod(Methods.GET);
                request.setPath("/" + method.getFullMethodName() + Base64.getUrlEncoder().encodeToString(payload));
            }
            request.getRequestHeaders().put(Headers.HOST, authority);
            request.getRequestHeaders().put(Headers.TRANSFER_ENCODING, Headers.CHUNKED.toString()); //even though this is HTTP/2 the client follows HTTP/1
            request.getRequestHeaders().put(Headers.CONTENT_TYPE, UndertowGrpcUtil.APPLICATION_GRPC);

            metadata.discardAll(GrpcUtil.USER_AGENT_KEY);
            metadata.put(GrpcUtil.USER_AGENT_KEY, userAgent);
            UndertowGrpcUtil.metadataToHeaderMap(request.getRequestHeaders(), metadata);
            clientConnection.sendRequest(request, new ClientCallback<ClientExchange>() {
                @Override
                public void completed(ClientExchange result) {
                    undertowClientTransport.streamCreated();
                    clientExchange = result;
                    result.getRequestChannel().getWriteSetter().set(new WriteListener());
                    result.setResponseListener(new ClientCallback<ClientExchange>() {
                        @Override
                        public void completed(ClientExchange result) {
                            clientExchange = result;
                            Metadata headers = headerMapToMetadata(result.getResponse().getResponseHeaders());
                            headers.put(STATUS, Integer.toString(result.getResponse().getResponseCode()));
                            transportState.transportHeadersReceived(headers);
                            responseChannel = clientExchange.getResponseChannel();
                            responseChannel.getReadSetter().set(new ReadChannelListener(clientConnection.getBufferPool(), ((b, endOfStream) -> {
                                if (endOfStream) {
                                    responseDone();
                                    HeaderMap trailers = result.getAttachment(HttpAttachments.REQUEST_TRAILERS);
                                    transportState.transportTrailersReceived(headerMapToMetadata(trailers));
                                }
                                transportState.transportDataReceived(b, endOfStream);
                            }), (e) -> {
                                requestDone();
                                responseDone();
                                transportState.transportReportStatus(Status.fromThrowable(e), false, new Metadata());
                            }));
                            if(requestedMessages > 0) {
                                responseChannel.resumeReads();
                            }
                        }

                        @Override
                        public void failed(IOException e) {
                            handleException(e);
                            requestDone();
                            responseDone();
                        }
                    });
                }

                @Override
                public void failed(IOException e) {
                    handleException(e);
                }
            });
        }

        public void writeFrame(@Nullable WritableBuffer frame, boolean endOfStream, boolean flush) {
            UndertowWritableBuffer writableBuffer = (UndertowWritableBuffer) frame;
            writableBuffer.getBuffer().getBuffer().flip();
            dataQueue.add(new WriteData(writableBuffer, endOfStream));
            clientExchange.getRequestChannel().resumeWrites();
        }

        public void request(int numMessages) {
            requestedMessages += numMessages;
            transportState.requestMessagesFromDeframer(numMessages);
            if (responseChannel != null) {
                responseChannel.resumeReads();
            }
        }

        public void cancel(Status status) {

        }
    };

    private void handleException(IOException e) {
        transportState.transportReportStatus(Status.fromThrowable(e), false, new Metadata());
    }

    UndertowClientStream(final ClientConnection clientConnection, final WritableBufferAllocator allocator, MethodDescriptor<?, ?> method, final Metadata headers, CallOptions callOptions, Attributes attributes, String userAgent, int maxMessageSize, StatsTraceContext statsTraceCtx, UndertowClientTransport undertowClientTransport) {
        super(allocator, statsTraceCtx, headers, method.isSafe());
        this.userAgent = userAgent;
        this.transportState = new ClientTransportState(maxMessageSize, statsTraceCtx);
        this.clientConnection = clientConnection;
        this.method = method;
        this.callOptions = callOptions;
        this.attributes = attributes;
        this.undertowClientTransport = undertowClientTransport;
        //TODO: deadline
    }


    protected TransportState transportState() {
        return transportState;
    }

    protected Sink abstractClientStreamSink() {
        return sink;
    }

    public void setAuthority(String authority) {
        this.authority = authority;
    }

    public Attributes getAttributes() {
        return attributes;
    }

    void requestDone() {
        if(requestDone) {
            return;
        }
        requestDone = true;
        if(responseDone) {
            undertowClientTransport.streamDestroyed();
        }
    }
    void responseDone() {
        if(responseDone) {
            return;
        }
        responseDone = true;
        if(requestDone) {
            undertowClientTransport.streamDestroyed();
        }
    }

    private class ClientTransportState extends Http2ClientStreamTransportState {
        protected ClientTransportState(int maxMessageSize, StatsTraceContext statsTraceCtx) {
            super(maxMessageSize, statsTraceCtx);
        }

        @Override
        protected void deframeFailed(Throwable cause) {
            http2ProcessingFailed(Status.fromThrowable(cause), new Metadata());
        }

        @Override
        public void bytesRead(int numBytes) {

        }

        @Override
        public void messageRead(InputStream is) {
            super.messageRead(is);
            requestedMessages--;
            if (requestedMessages == 0) {
                clientExchange.getResponseChannel().suspendReads();
            }
        }

        @Override
        protected void http2ProcessingFailed(Status status, Metadata trailers) {
            transportReportStatus(status, false, trailers);
            if (clientExchange != null) {
                IoUtils.safeClose(clientExchange.getResponseChannel(), clientExchange.getRequestChannel());
            }
        }

        @Override
        protected void transportHeadersReceived(Metadata headers) {
            super.transportHeadersReceived(headers);
        }

        @Override
        protected void transportDataReceived(ReadableBuffer frame, boolean endOfStream) {
            super.transportDataReceived(frame, endOfStream);
        }

        @Override
        protected void transportTrailersReceived(Metadata trailers) {
            super.transportTrailersReceived(trailers);
        }
    }

    private class WriteListener implements ChannelListener<StreamSinkChannel> {

        @Override
        public void handleEvent(StreamSinkChannel streamSinkChannel) {
            try {
                for (; ; ) {
                    WriteData data = dataQueue.peek();
                    if (data == null) {
                        streamSinkChannel.suspendWrites();
                        return;
                    }

                    if (data.frame != null) {
                        ByteBuffer buffer = data.frame.getBuffer().getBuffer();
                        if (buffer.hasRemaining()) {
                            int res;
                            if (data.endOfStream) {
                                res = streamSinkChannel.writeFinal(buffer);
                            } else {
                                res = streamSinkChannel.write(buffer);
                            }
                            if (res == 0) {
                                return;
                            }
                            if (!buffer.hasRemaining()) {
                                dataQueue.poll();
                            }
                        } else {
                            dataQueue.poll(); //remove this one from the queue
                        }
                    }
                    if (data.endOfStream) {
                        streamSinkChannel.shutdownWrites();
                        if (!streamSinkChannel.flush()) {
                            streamSinkChannel.getWriteSetter().set(ChannelListeners.flushingChannelListener((ChannelListener<StreamSinkChannel>) sc -> requestDone(), (ChannelExceptionHandler<StreamSinkChannel>) (sc, e) -> {
                                UndertowClientStream.this.handleException(e);
                                IoUtils.safeClose(sc);
                                requestDone();
                                responseDone();
                            }));
                        } else {
                            requestDone();
                        }

                    }
                }
            } catch (IOException e) {
                handleException(e);
                IoUtils.safeClose(streamSinkChannel);
            }
        }
    }

    private static class WriteData {
        final UndertowWritableBuffer frame;
        final boolean endOfStream;

        private WriteData(UndertowWritableBuffer frame, boolean endOfStream) {
            this.frame = frame;
            this.endOfStream = endOfStream;
        }
    }


}
