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
import io.grpc.internal.InternalServer;
import io.grpc.internal.LogId;
import io.grpc.internal.ServerListener;
import io.grpc.internal.ServerTransport;
import io.grpc.internal.ServerTransportListener;
import io.undertow.server.HandlerWrapper;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.AttachmentKey;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import io.undertow.util.Protocols;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author Stuart Douglas
 */
public class UndertowServerBuilder extends AbstractServerImplBuilder<UndertowServerBuilder> {

    private volatile ServerTransportListener serverListener;

    static final AttachmentKey<Attributes> ATTRIBUTES_ATTACHMENT_KEY = AttachmentKey.create(Attributes.class);

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

    public HandlerWrapper getHandlerWrapper() {
        return new UndertowGRPCHandlerWrapper();
    }

    private class UndertowGRPCHandler implements HttpHandler {

        private final HttpHandler next;

        private UndertowGRPCHandler(HttpHandler next) {
            this.next = next;
        }

        public void handleRequest(final HttpServerExchange exchange) throws Exception {

            String contentType = exchange.getRequestHeaders().getFirst(Headers.CONTENT_TYPE);
            if(!exchange.getProtocol().equals(Protocols.HTTP_2_0) || contentType == null || !contentType.startsWith("application/grpc")) {
                next.handleRequest(exchange);
                return;
            }

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
    }


    private class UndertowGRPCHandlerWrapper implements HandlerWrapper {

        public HttpHandler wrap(HttpHandler handler) {
            return new UndertowGRPCHandler(handler);
        }
    }
}
