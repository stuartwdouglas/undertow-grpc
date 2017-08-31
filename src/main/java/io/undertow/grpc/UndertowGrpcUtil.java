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

import io.grpc.Metadata;
import io.undertow.util.HeaderMap;
import io.undertow.util.HeaderValues;
import io.undertow.util.HttpString;

/**
 * @author Stuart Douglas
 */
class UndertowGrpcUtil {

    public static final String APPLICATION_GRPC = "application/grpc";

    private UndertowGrpcUtil() {
    }

    static void metadataToHeaderMap(HeaderMap map, Metadata metadata) {
        for (String key : metadata.keys()) {
            Iterable<String> all = metadata.getAll(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
            if (all != null) {
                for (String val : all) {
                    map.add(new HttpString(key), val);
                }
            }
        }
    }

    static Metadata headerMapToMetadata(HeaderMap map) {
        Metadata metadata = new Metadata();
        for (HeaderValues header : map) {
            Metadata.Key<String> key = Metadata.Key.of(header.getHeaderName().toString(), Metadata.ASCII_STRING_MARSHALLER);
            for (String val : header) {
                metadata.put(key, val);
            }
        }
        return metadata;
    }
}
