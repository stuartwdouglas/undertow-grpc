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

import io.undertow.connector.ByteBufferPool;
import io.undertow.connector.PooledByteBuffer;
import org.xnio.ChannelListener;
import org.xnio.channels.StreamSourceChannel;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author Stuart Douglas
 */
class ReadChannelListener implements ChannelListener<StreamSourceChannel> {
    private final ByteBufferPool pool;
    private final BiConsumer<UndertowReadableBuffer, Boolean> dataReceiver;
    private final Consumer<Exception> errorHandler;

    ReadChannelListener(ByteBufferPool pool, BiConsumer<UndertowReadableBuffer, Boolean> dataReceiver, Consumer<Exception> errorHandler) {
        this.pool = pool;
        this.dataReceiver = dataReceiver;
        this.errorHandler = errorHandler;
    }

    @Override
    public void handleEvent(StreamSourceChannel channel) {
        for(;;) {
            PooledByteBuffer pooled = pool.allocate();
            try {
                int res = channel.read(pooled.getBuffer());
                if (res == -1) {
                    pooled.getBuffer().flip();
                    dataReceiver.accept(new UndertowReadableBuffer(pooled), true);
                    return;
                } else if (res == 0) {
                    pooled.close();
                    return;
                } else {
                    pooled.getBuffer().flip();
                    dataReceiver.accept(new UndertowReadableBuffer(pooled), false);
                }
            } catch (Exception e) {
                pooled.close();
                //TODO: error handling
                errorHandler.accept(e);
                return;
            }
        }
    }
}
