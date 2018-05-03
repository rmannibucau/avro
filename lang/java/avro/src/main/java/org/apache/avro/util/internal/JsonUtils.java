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
package org.apache.avro.util.internal;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.json.JsonBuilderFactory;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.JsonbConfig;
import javax.json.bind.JsonbException;
import javax.json.spi.JsonProvider;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonGeneratorFactory;
import javax.json.stream.JsonParserFactory;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;

public class JsonUtils {
  static final String BYTES_CHARSET = "ISO-8859-1";
  private static final JsonProvider JSON = JsonProvider.provider();
  private static final JsonParserFactory PARSER = JSON.createParserFactory(emptyMap());
  private static final JsonBuilderFactory BUILDER = JSON.createBuilderFactory(emptyMap());
  private static final JsonGeneratorFactory FACTORY = JSON.createGeneratorFactory(singletonMap("org.apache.johnzon.supports-comments", "true"));
  private static final JsonGeneratorFactory PRETTY_FACTORY = JSON.createGeneratorFactory(new HashMap<String, String>() {{
    put("org.apache.johnzon.supports-comments", "true");
    put(JsonGenerator.PRETTY_PRINTING, "true");
  }});
  private static final Jsonb COMMENT_FRIENDLY_JSONB = JsonbBuilder.create(new JsonbConfig()
    .setProperty("johnzon.cdi.activated", false)
    .setProperty("org.apache.johnzon.supports-comments", true));
  private static final Jsonb JSONB = JsonbBuilder.create(new JsonbConfig().setProperty("johnzon.cdi.activated", false));

  private JsonUtils() {
  }

  public static JsonBuilderFactory getBuilder() {
    return BUILDER;
  }

  public static JsonParserFactory getParser() {
    return PARSER;
  }

  public static JsonProvider getProvider() {
    return JSON;
  }

  public static Jsonb getJsonB() {
    return JSONB;
  }

  public static JsonGeneratorFactory getDefaultFactory() {
    return FACTORY;
  }

  public static JsonGeneratorFactory getPrettyFactory() {
    return PRETTY_FACTORY;
  }

  public static Jsonb getCommentFriendlyJsonb() {
    return COMMENT_FRIENDLY_JSONB;
  }

  public static JsonValue toJsonValue(Object datum) {
    if (datum == null) {
      return null;
    }
    try {
      return JSONB.fromJson(JSONB.toJson(datum), JsonValue.class);
    } catch (final JsonbException e) {
      throw new AvroRuntimeException(e);
    }
  }

  @SuppressWarnings(value="unchecked")
  static void toJson(Object datum, JsonGenerator generator) throws IOException {
    if (datum == JsonProperties.NULL_VALUE) { // null
      generator.writeNull();
    } else if (datum instanceof Map) { // record, map
      generator.writeStartObject();
      for (Map.Entry<Object,Object> entry : ((Map<Object,Object>) datum).entrySet()) {
        generator.write(entry.getKey().toString());
        toJson(entry.getValue(), generator);
      }
      generator.writeEnd();
    } else if (datum instanceof Collection) { // array
      generator.writeStartArray();
      for (Object element : (Collection<?>) datum) {
        toJson(element, generator);
      }
      generator.writeEnd();
    } else if (datum instanceof byte[]) { // bytes, fixed
      generator.write(new String((byte[]) datum, BYTES_CHARSET));
    } else if (datum instanceof CharSequence || datum instanceof Enum<?>) { // string, enum
      generator.write(datum.toString());
    } else if (datum instanceof Double) { // double
      generator.write((Double) datum);
    } else if (datum instanceof Float) { // float
      generator.write((Float) datum);
    } else if (datum instanceof Long) { // long
      generator.write((Long) datum);
    } else if (datum instanceof Integer) { // int
      generator.write((Integer) datum);
    } else if (datum instanceof Boolean) { // boolean
      generator.write((Boolean) datum);
    } else {
      throw new AvroRuntimeException("Unknown datum class: " + datum.getClass());
    }
  }

  public static Object toObject(JsonValue jsonNode) {
    return toObject(jsonNode, null);
  }

  public static Object toObject(JsonValue jsonNode, Schema schema) {
    if (schema != null && schema.getType().equals(Schema.Type.UNION)) {
      return toObject(jsonNode, schema.getTypes().get(0));
    }
    if (jsonNode == null) {
      return null;
    } else if (jsonNode.getValueType() == JsonValue.ValueType.NULL) {
      return JsonProperties.NULL_VALUE;
    } else if (jsonNode.getValueType() == JsonValue.ValueType.TRUE || jsonNode.getValueType() == JsonValue.ValueType.FALSE) {
      return JsonValue.TRUE.equals(jsonNode);
    } else if (jsonNode.getValueType() == JsonValue.ValueType.NUMBER) {
      final JsonNumber number = JsonNumber.class.cast(jsonNode);
      if (schema == null || schema.getType().equals(Schema.Type.INT)) {
        return number.intValue();
      } else if (schema.getType().equals(Schema.Type.LONG)) {
        return number.longValue();
      } else if (schema.getType().equals(Schema.Type.DOUBLE)) {
        return number.doubleValue();
      } else if (schema.getType().equals(Schema.Type.FLOAT)) {
        return (float) number.doubleValue();
      }
    } else if (jsonNode.getValueType() == JsonValue.ValueType.STRING) {
        final JsonString string = JsonString.class.cast(jsonNode);
      if (schema == null || schema.getType().equals(Schema.Type.STRING)
              || schema.getType().equals(Schema.Type.ENUM)) {
        return string.getString();
      } else if (schema.getType().equals(Schema.Type.BYTES)
              || schema.getType().equals(Schema.Type.FIXED)) {
        try {
          return string.getString().getBytes(BYTES_CHARSET);
        } catch (final UnsupportedEncodingException e) {
          throw new AvroRuntimeException(e);
        }
      }
    } else if (jsonNode.getValueType() == JsonValue.ValueType.ARRAY) {
      final List l = new ArrayList();
      for (final JsonValue node : jsonNode.asJsonArray()) {
        l.add(toObject(node, schema == null ? null : schema.getElementType()));
      }
      return l;
    } else if (jsonNode.getValueType() == JsonValue.ValueType.OBJECT) {
      final Map m = new LinkedHashMap();
        final JsonObject object = jsonNode.asJsonObject();
        for (final String key : object.keySet()) {
          Schema s = null;
          if (schema == null) {
            s = null;
          } else if (schema.getType().equals(Schema.Type.MAP)) {
            s = schema.getValueType();
          } else if (schema.getType().equals(Schema.Type.RECORD)) {
            s = schema.getField(key).schema();
          }
          Object value = toObject(object.get(key), s);
          m.put(key, value);
        }
      return m;
    }
    return null;
  }
}
