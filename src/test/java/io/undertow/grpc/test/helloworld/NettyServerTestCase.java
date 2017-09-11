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

package io.undertow.grpc.test.helloworld;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class NettyServerTestCase {


    public static final int PORT = 7878;

    @Test
    public void testServer() throws IOException, InterruptedException {

        Server server = ServerBuilder.forPort(PORT)
                .addService(new TestGreeter())
                .build()
                .start();
        HelloWorldClient client = new HelloWorldClient("localhost", PORT);
        try {
            String user = "world";
            String result = client.greet(user);

            Assert.assertEquals("hello world", result);
        } finally {
            client.shutdown();
            server.shutdownNow();
        }


    }

    public static class TestGreeter extends GreeterGrpc.GreeterImplBase {
        @Override
        public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            responseObserver.onNext(HelloReply.newBuilder().setMessage("hello " + request.getName()).build());
            responseObserver.onCompleted();
        }
    }
}
