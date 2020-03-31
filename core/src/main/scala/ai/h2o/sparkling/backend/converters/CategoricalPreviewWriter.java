/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.h2o.sparkling.backend.converters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import water.parser.ParseWriter;
import water.parser.PreviewParseWriter;
import water.util.IcedHashMap;

class CategoricalPreviewWriter extends PreviewParseWriter {
    public CategoricalPreviewWriter() {
        super();
    }

    public CategoricalPreviewWriter(int numberOfColumns) {
        super(numberOfColumns);
    }

    static final int MAX_PREVIEW_RECORDS = 100;

    static private Kryo getKryoSerializer() {
        Kryo kryo = new Kryo();
        kryo.register(CategoricalPreviewWriter.class);
        kryo.register(IcedHashMap.class);
        kryo.register(ParseWriter.ParseErr.class);
        kryo.register(ParseWriter.ParseErr[].class);
        kryo.register(ParseWriter.UnsupportedTypeOverride.class);
        kryo.register(ParseWriter.UnsupportedTypeOverride[].class);
        return kryo;
    }

    public static byte[] serialize(CategoricalPreviewWriter instance) {
        Kryo serializer = getKryoSerializer();
        try (Output output = new Output(4096, Integer.MIN_VALUE - 8)) {
            serializer.writeObject(output, instance);
            return output.toBytes();
        }
    }

    public static CategoricalPreviewWriter deserialize(byte[] bytes) {
        Kryo serializer = getKryoSerializer();
        try (Input input = new Input(bytes)) {
            return serializer.readObject(input, CategoricalPreviewWriter.class);
        }
    }
}
