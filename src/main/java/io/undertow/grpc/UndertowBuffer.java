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

import io.grpc.internal.WritableBuffer;
import io.undertow.connector.PooledByteBuffer;

/**
 * @author Stuart Douglas
 */
public class UndertowBuffer implements WritableBuffer {

    private final PooledByteBuffer buffer;

    public UndertowBuffer(PooledByteBuffer buffer) {
        this.buffer = buffer;
    }

    public void write(byte[] src, int srcIndex, int length) {
        buffer.getBuffer().put(src, srcIndex, length);
    }

    public void write(byte b) {
        buffer.getBuffer().put(b);
    }

    public int writableBytes() {
        return buffer.getBuffer().remaining();
    }

    public int readableBytes() {
        return buffer.getBuffer().position();
    }

    public void release() {
        buffer.close();
    }

    public PooledByteBuffer getBuffer() {
        return buffer;
    }
}
