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

import io.grpc.InternalMetadata;
import io.grpc.Metadata;
import io.undertow.util.HeaderMap;
import io.undertow.util.HeaderValues;
import io.undertow.util.HttpString;

import java.nio.charset.StandardCharsets;

/**
 * @author Stuart Douglas
 */
class UndertowGrpcUtil {

    public static final String APPLICATION_GRPC = "application/grpc";

    private UndertowGrpcUtil() {
    }

    static void metadataToHeaderMap(HeaderMap map, Metadata metadata) {
        byte[][] data = InternalMetadata.serialize(metadata);
        for (int i = 0; i < data.length; i += 2) {
            HttpString header = new HttpString(data[i]);
            String value = new String(data[i + 1]);
            map.add(header, value);
        }
    }

    static Metadata headerMapToMetadata(HeaderMap map) {
        byte[][] data = new byte[map.size() * 2][];
        int c = 0;
        for (HeaderValues header : map) {
            data[c] = new byte[header.getHeaderName().length()];
            header.getHeaderName().copyTo(data[c], 0);
            data[c + 1] = header.getFirst().getBytes(StandardCharsets.US_ASCII);
            c += 2;
        }
        return InternalMetadata.newMetadata(data);
    }
}
