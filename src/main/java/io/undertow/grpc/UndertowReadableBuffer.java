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

import io.grpc.internal.AbstractReadableBuffer;
import io.grpc.internal.ReadableBuffer;
import io.grpc.internal.ReadableBuffers;
import io.undertow.connector.PooledByteBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * @author Stuart Douglas
 */
class UndertowReadableBuffer extends AbstractReadableBuffer {

    private final PooledByteBuffer buffer;

    public UndertowReadableBuffer(PooledByteBuffer buffer) {
        this.buffer = buffer;
    }

    public int readableBytes() {
        return buffer.getBuffer().remaining();
    }

    public int readUnsignedByte() {
        return buffer.getBuffer().get() & 0xFF;
    }

    public void skipBytes(int length) {
        buffer.getBuffer().position(buffer.getBuffer().position() + length);
    }

    public void readBytes(byte[] dest, int destOffset, int length) {
        buffer.getBuffer().get(dest, destOffset, length);
    }

    public void readBytes(ByteBuffer dest) {
        while (dest.hasRemaining() && buffer.getBuffer().hasRemaining()) {
            dest.put(buffer.getBuffer().get());
        }
    }

    public void readBytes(OutputStream dest, int length) throws IOException {
        for (int i = 0; i < length; ++i) {
            dest.write(buffer.getBuffer().get());
        }
    }

    public ReadableBuffer readBytes(int length) {
        byte[] data = new byte[length];
        buffer.getBuffer().get(data);
        return ReadableBuffers.wrap(data);
    }

    public boolean hasArray() {
        return buffer.getBuffer().hasArray();
    }

    public byte[] array() {
        return buffer.getBuffer().array();
    }

    public int arrayOffset() {
        return buffer.getBuffer().arrayOffset();
    }

    public void close() {
        buffer.close();
    }
}
