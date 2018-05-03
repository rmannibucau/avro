/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;

import org.apache.avro.util.internal.JsonUtils;

import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ResolvingDecoder;

/** Utilities for reading and writing arbitrary Json data in Avro format. */
public class Json {
  private Json() {}                               // singleton: no public ctor

  /** The schema for Json data. */
  public static final Schema SCHEMA;
  static {
    try {
      InputStream in = Json.class.getResourceAsStream("/org/apache/avro/data/Json.avsc");
      try {
        SCHEMA = Schema.parse(in);
      } finally {
        in.close();
      }
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  /**
   * {@link DatumWriter} for arbitrary Json data.
   * @deprecated use {@link ObjectWriter}
   */
  @Deprecated
  public static class Writer implements DatumWriter<JsonValue> {

    @Override public void setSchema(Schema schema) {
      if (!SCHEMA.equals(schema))
        throw new RuntimeException("Not the Json schema: "+schema);
    }

    @Override
    public void write(JsonValue datum, Encoder out) throws IOException {
      Json.write(datum, out);
    }
  }

  /**
   * {@link DatumReader} for arbitrary Json data.
   * @deprecated use {@link ObjectReader}
   */
  @Deprecated
  public static class Reader implements DatumReader<JsonValue> {
    private Schema written;
    private ResolvingDecoder resolver;

    @Override public void setSchema(Schema schema) {
      this.written = SCHEMA.equals(written) ? null : schema;
    }

    @Override
    public JsonValue read(JsonValue reuse, Decoder in) throws IOException {
      if (written == null)                        // same schema
        return Json.read(in);

      // use a resolver to adapt alternate version of Json schema
      if (resolver == null)
        resolver = DecoderFactory.get().resolvingDecoder(written, SCHEMA, null);
      resolver.configure(in);
      JsonValue result = Json.read(resolver);
      resolver.drain();
      return result;
    }
  }

  /** {@link DatumWriter} for arbitrary Json data using the object model described
   *  in {@link org.apache.avro.JsonProperties}. */
  public static class ObjectWriter implements DatumWriter<Object> {

    @Override public void setSchema(Schema schema) {
      if (!SCHEMA.equals(schema))
        throw new RuntimeException("Not the Json schema: "+schema);
    }

    @Override
    public void write(Object datum, Encoder out) throws IOException {
      Json.writeObject(datum, out);
    }
  }

  /** {@link DatumReader} for arbitrary Json data using the object model described
   *  in {@link org.apache.avro.JsonProperties}. */
  public static class ObjectReader implements DatumReader<Object> {
    private Schema written;
    private ResolvingDecoder resolver;

    @Override public void setSchema(Schema schema) {
      this.written = SCHEMA.equals(written) ? null : schema;
    }

    @Override
    public Object read(Object reuse, Decoder in) throws IOException {
      if (written == null)                        // same schema
        return Json.readObject(in);

      // use a resolver to adapt alternate version of Json schema
      if (resolver == null)
        resolver = DecoderFactory.get().resolvingDecoder(written, SCHEMA, null);
      resolver.configure(in);
      Object result = Json.readObject(resolver);
      resolver.drain();
      return result;
    }
  }

  /**
   * Parses a JSON string and converts it to the object model described in
   * {@link org.apache.avro.JsonProperties}.
   */
  public static Object parseJson(String s) {
    return JsonUtils.toObject(JsonUtils.getJsonB().fromJson(s, JsonValue.class));
  }

  /**
   * Converts an instance of the object model described in
   * {@link org.apache.avro.JsonProperties} to a JSON string.
   */
  public static String toString(Object datum) {
    return JsonUtils.toJsonValue(datum).toString();
  }

  /** Note: this enum must be kept aligned with the union in Json.avsc. */
  private enum JsonType { LONG, DOUBLE, STRING, BOOLEAN, NULL, ARRAY, OBJECT }

  /**
   * Write Json data as Avro data.
   * @deprecated internal method
   */
  @Deprecated
  public static void write(JsonValue node, Encoder out) throws IOException {
    switch(node.getValueType()) {
    case NUMBER:
      JsonNumber number = JsonNumber.class.cast(node);
      if (number.isIntegral()) { // todo: check it is what we want
        out.writeIndex(JsonType.LONG.ordinal());
        out.writeLong(number.longValue());
      } else {
        out.writeIndex(JsonType.DOUBLE.ordinal());
        out.writeDouble(number.doubleValue());
      }
    break;
    case STRING:
      out.writeIndex(JsonType.STRING.ordinal());
      out.writeString(JsonString.class.cast(node).getString());
      break;
    case TRUE:
      out.writeIndex(JsonType.BOOLEAN.ordinal());
      out.writeBoolean(true);
      break;
    case FALSE:
      out.writeIndex(JsonType.BOOLEAN.ordinal());
      out.writeBoolean(false);
      break;
    case NULL:
      out.writeIndex(JsonType.NULL.ordinal());
      out.writeNull();
      break;
    case ARRAY:
      out.writeIndex(JsonType.ARRAY.ordinal());
      out.writeArrayStart();
      JsonArray array = node.asJsonArray();
      out.setItemCount(array.size());
      for (JsonValue element : array) {
        out.startItem();
        write(element, out);
      }
      out.writeArrayEnd();
      break;
      case OBJECT:
      out.writeIndex(JsonType.OBJECT.ordinal());
      out.writeMapStart();
      JsonObject object = node.asJsonObject();
      out.setItemCount(object.size());
      Iterator<String> i = object.keySet().iterator();
      while (i.hasNext()) {
        out.startItem();
        String name = i.next();
        out.writeString(name);
        write(object.get(name), out);
      }
      out.writeMapEnd();
      break;
    default:
      throw new AvroRuntimeException(node.getValueType()+" unexpected: "+node);
    }
  }

  /**
   * Read Json data from Avro data.
   * @deprecated internal method
   */
  @Deprecated
  public static JsonValue read(Decoder in) throws IOException {
    switch (JsonType.values()[in.readIndex()]) {
    case LONG:
      return JsonUtils.getProvider().createValue(in.readLong());
    case DOUBLE:
      return JsonUtils.getProvider().createValue(in.readDouble());
    case STRING:
      return JsonUtils.getProvider().createValue(in.readString());
    case BOOLEAN:
      return in.readBoolean() ? JsonValue.TRUE : JsonValue.FALSE;
    case NULL:
      in.readNull();
      return JsonValue.NULL;
    case ARRAY:
      JsonArrayBuilder array = JsonUtils.getBuilder().createArrayBuilder();
      for (long l = in.readArrayStart(); l > 0; l = in.arrayNext())
        for (long i = 0; i < l; i++)
          array.add(read(in));
      return array.build();
    case OBJECT:
      JsonObjectBuilder object = JsonUtils.getBuilder().createObjectBuilder();
      for (long l = in.readMapStart(); l > 0; l = in.mapNext())
        for (long i = 0; i < l; i++)
          object.add(in.readString(), read(in));
      return object.build();
    default:
      throw new AvroRuntimeException("Unexpected Json node type");
    }
  }

  private static void writeObject(Object datum, Encoder out) throws IOException {
    write(JsonUtils.toJsonValue(datum), out);
  }

  private static Object readObject(Decoder in) throws IOException {
    return JsonUtils.toObject(read(in));
  }

}
