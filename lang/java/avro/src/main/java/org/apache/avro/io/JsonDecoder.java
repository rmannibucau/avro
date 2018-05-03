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
package org.apache.avro.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import javax.json.JsonNumber;
import javax.json.stream.JsonParser;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.JsonGrammarGenerator;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;
import org.apache.avro.util.internal.JsonUtils;

/** A {@link Decoder} for Avro's JSON data encoding.
 * </p>
 * Construct using {@link DecoderFactory}.
 * </p>
 * JsonDecoder is not thread-safe.
 * */
public class JsonDecoder extends ParsingDecoder
  implements Parser.ActionHandler {
  private JsonParser in;
  Stack<ReorderBuffer> reorderBuffers = new Stack<>();
  ReorderBuffer currentReorderBuffer;

  private JsonParser.Event last;

  private static class ReorderBuffer {
    public Map<String, List<JsonElement>> savedFields = new HashMap<>();
    public JsonParser origParser = null;
  }

  static final String CHARSET = "ISO-8859-1";

  private JsonDecoder(Symbol root, InputStream in) throws IOException {
    super(root);
    configure(in);
  }

  private JsonDecoder(Symbol root, String in) throws IOException {
    super(root);
    configure(in);
  }

  JsonDecoder(Schema schema, InputStream in) throws IOException {
    this(getSymbol(schema), in);
  }

  JsonDecoder(Schema schema, String in) throws IOException {
    this(getSymbol(schema), in);
  }

  private static Symbol getSymbol(Schema schema) {
    if (null == schema) {
      throw new NullPointerException("Schema cannot be null!");
    }
    return new JsonGrammarGenerator().generate(schema);
  }

  /**
   * Reconfigures this JsonDecoder to use the InputStream provided.
   * <p/>
   * If the InputStream provided is null, a NullPointerException is thrown.
   * <p/>
   * Otherwise, this JsonDecoder will reset its state and then
   * reconfigure its input.
   * @param in
   *   The InputStream to read from. Cannot be null.
   * @throws IOException
   * @return this JsonDecoder
   */
  public JsonDecoder configure(InputStream in) throws IOException {
    if (null == in) {
      throw new NullPointerException("InputStream to read from cannot be null!");
    }
    parser.reset();
    reorderBuffers.clear();
    currentReorderBuffer = null;
    this.in = JsonUtils.getParser().createParser(in);
    this.in.next();
    return this;
  }

  /**
   * Reconfigures this JsonDecoder to use the String provided for input.
   * <p/>
   * If the String provided is null, a NullPointerException is thrown.
   * <p/>
   * Otherwise, this JsonDecoder will reset its state and then
   * reconfigure its input.
   * @param in
   *   The String to read from. Cannot be null.
   * @throws IOException
   * @return this JsonDecoder
   */
  public JsonDecoder configure(String in) throws IOException {
    if (null == in) {
      throw new NullPointerException("String to read from cannot be null!");
    }
    parser.reset();
    reorderBuffers.clear();
    currentReorderBuffer = null;
    this.in = JsonUtils.getParser().createParser(new StringReader(in));
    last = this.in.next();
    return this;
  }

  private void advance(Symbol symbol) throws IOException {
    this.parser.processTrailingImplicitActions();
    if (last == null && this.parser.depth() == 1)
      throw new EOFException();
    parser.advance(symbol);
  }

  @Override
  public void readNull() throws IOException {
    advance(Symbol.NULL);
    if (last == JsonParser.Event.VALUE_NULL) {
      last = in.next();
    } else {
      throw error("null");
    }
  }

  @Override
  public boolean readBoolean() throws IOException {
    advance(Symbol.BOOLEAN);
    JsonParser.Event t = last;
    if (t == JsonParser.Event.VALUE_TRUE || t == JsonParser.Event.VALUE_FALSE) {
      last = in.next();
      return t == JsonParser.Event.VALUE_TRUE;
    } else {
      throw error("boolean");
    }
  }

  @Override
  public int readInt() throws IOException {
    advance(Symbol.INT);
    if (last == JsonParser.Event.VALUE_NUMBER) {
      int result = in.getInt();
      last = in.next();
      return result;
    } else {
      throw error("int");
    }
  }

  @Override
  public long readLong() throws IOException {
    advance(Symbol.LONG);
    if (last == JsonParser.Event.VALUE_NUMBER) {
      long result = in.getLong();
      last = in.next();
      return result;
    } else {
      throw error("long");
    }
  }

  @Override
  public float readFloat() throws IOException {
    advance(Symbol.FLOAT);
    if (last == JsonParser.Event.VALUE_NUMBER) {
      float result = (float) JsonNumber.class.cast(in.getValue()).doubleValue();
      last = in.next();
      return result;
    } else {
      throw error("float");
    }
  }

  @Override
  public double readDouble() throws IOException {
    advance(Symbol.DOUBLE);
    if (last == JsonParser.Event.VALUE_NUMBER) {
      double result = JsonNumber.class.cast(in.getValue()).doubleValue();
      last = in.next();
      return result;
    } else {
      throw error("double");
    }
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    return new Utf8(readString());
  }

  @Override
  public String readString() throws IOException {
    advance(Symbol.STRING);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER);
      if (last != JsonParser.Event.KEY_NAME) {
        throw error("map-key");
      }
    } else {
      if (last != JsonParser.Event.VALUE_STRING) {
        throw error("string");
      }
    }
    String result = in.getString();
    last = in.next();
    return result;
  }

  @Override
  public void skipString() throws IOException {
    advance(Symbol.STRING);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER);
      if (last != JsonParser.Event.KEY_NAME) {
        throw error("map-key");
      }
    } else {
      if (last != JsonParser.Event.VALUE_STRING) {
        throw error("string");
      }
    }
    last = in.next();
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    advance(Symbol.BYTES);
    if (last == JsonParser.Event.VALUE_STRING) {
      byte[] result = readByteArray();
      last = in.next();
      return ByteBuffer.wrap(result);
    } else {
      throw error("bytes");
    }
  }

  private byte[] readByteArray() throws IOException {
    byte[] result = in.getString().getBytes(CHARSET);
    return result;
  }

  @Override
  public void skipBytes() throws IOException {
    advance(Symbol.BYTES);
    if (last == JsonParser.Event.VALUE_STRING) {
      last = in.next();
    } else {
      throw error("bytes");
    }
  }

  private void checkFixed(int size) throws IOException {
    advance(Symbol.FIXED);
    Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
    if (size != top.size) {
      throw new AvroTypeException(
        "Incorrect length for fixed binary: expected " +
        top.size + " but received " + size + " bytes.");
    }
  }

  @Override
  public void readFixed(byte[] bytes, int start, int len) throws IOException {
    checkFixed(len);
    if (last == JsonParser.Event.VALUE_STRING) {
      byte[] result = readByteArray();
      last = in.next();
      if (result.length != len) {
        throw new AvroTypeException("Expected fixed length " + len
            + ", but got" + result.length);
      }
      System.arraycopy(result, 0, bytes, start, len);
    } else {
      throw error("fixed");
    }
  }

  @Override
  public void skipFixed(int length) throws IOException {
    checkFixed(length);
    doSkipFixed(length);
  }

  private void doSkipFixed(int length) throws IOException {
    if (last == JsonParser.Event.VALUE_STRING) {
      byte[] result = readByteArray();
      last = in.next();
      if (result.length != length) {
        throw new AvroTypeException("Expected fixed length " + length
            + ", but got" + result.length);
      }
    } else {
      throw error("fixed");
    }
  }

  @Override
  protected void skipFixed() throws IOException {
    advance(Symbol.FIXED);
    Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
    doSkipFixed(top.size);
  }

  @Override
  public int readEnum() throws IOException {
    advance(Symbol.ENUM);
    Symbol.EnumLabelsAction top = (Symbol.EnumLabelsAction) parser.popSymbol();
    if (last == JsonParser.Event.VALUE_STRING) {
      // in.getString();
      int n = top.findLabel(in.getString());
      if (n >= 0) {
        last = in.next();
        return n;
      }
      throw new AvroTypeException("Unknown symbol in enum " + in.getString());
    } else {
      throw error("fixed");
    }
  }

  @Override
  public long readArrayStart() throws IOException {
    advance(Symbol.ARRAY_START);
    if (last == JsonParser.Event.START_ARRAY) {
      last = in.next();
      return doArrayNext();
    } else {
      throw error("array-start");
    }
  }

  @Override
  public long arrayNext() throws IOException {
    advance(Symbol.ITEM_END);
    return doArrayNext();
  }

  private long doArrayNext() throws IOException {
    if (last == JsonParser.Event.END_ARRAY) {
      parser.advance(Symbol.ARRAY_END);
      last = in.next();
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  public long skipArray() throws IOException {
    advance(Symbol.ARRAY_START);
    if (last == JsonParser.Event.START_ARRAY) {
      // in.skipChildren();
      last = in.next();
      advance(Symbol.ARRAY_END);
    } else {
      throw error("array-start");
    }
    return 0;
  }

  @Override
  public long readMapStart() throws IOException {
    advance(Symbol.MAP_START);
    if (last == JsonParser.Event.START_OBJECT) {
      last = in.next();
      return doMapNext();
    } else {
      throw error("map-start");
    }
  }

  @Override
  public long mapNext() throws IOException {
    advance(Symbol.ITEM_END);
    return doMapNext();
  }

  private long doMapNext() throws IOException {
    if (last == JsonParser.Event.END_OBJECT) {
      last = in.next();
      advance(Symbol.MAP_END);
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  public long skipMap() throws IOException {
    advance(Symbol.MAP_START);
    if (last == JsonParser.Event.START_OBJECT) {
      // in.skipChildren();
      last = in.next();
      advance(Symbol.MAP_END);
    } else {
      throw error("map-start");
    }
    return 0;
  }

  @Override
  public int readIndex() throws IOException {
    advance(Symbol.UNION);
    Symbol.Alternative a = (Symbol.Alternative) parser.popSymbol();

    String label;
    if (last == JsonParser.Event.VALUE_NULL) {
      label = "null";
    } else if (last == JsonParser.Event.START_OBJECT &&
            (last = in.next()) == JsonParser.Event.KEY_NAME) {
      label = in.getString();
      last = in.next();
      parser.pushSymbol(Symbol.UNION_END);
    } else {
      throw error("start-union");
    }
    int n = a.findLabel(label);
    if (n < 0)
      throw new AvroTypeException("Unknown union branch " + label);
    parser.pushSymbol(a.getSymbol(n));
    return n;
  }

  @Override
  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    if (top instanceof Symbol.FieldAdjustAction) {
        Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction) top;
        String name = fa.fname;
      if (currentReorderBuffer != null) {
        List<JsonElement> node = currentReorderBuffer.savedFields.get(name);
        if (node != null) {
          currentReorderBuffer.savedFields.remove(name);
          currentReorderBuffer.origParser = in;
          in = makeParser(node);
          return null;
        }
      }
      if (last == JsonParser.Event.KEY_NAME) {
        do {
          String fn = in.getString();
          last = in.next();
          if (name.equals(fn)) {
            return null;
          } else {
            if (currentReorderBuffer == null) {
              currentReorderBuffer = new ReorderBuffer();
            }
            currentReorderBuffer.savedFields.put(fn, getValueAsTree(in));
          }
        } while (last == JsonParser.Event.KEY_NAME);
        throw new AvroTypeException("Expected field name not found: " + fa.fname);
      }
    } else if (top == Symbol.FIELD_END) {
      if (currentReorderBuffer != null && currentReorderBuffer.origParser != null) {
        in = currentReorderBuffer.origParser;
        currentReorderBuffer.origParser = null;
      }
    } else if (top == Symbol.RECORD_START) {
      if (last == JsonParser.Event.START_OBJECT) {
        last = in.next();
        reorderBuffers.push(currentReorderBuffer);
        currentReorderBuffer = null;
      } else {
        throw error("record-start");
      }
    } else if (top == Symbol.RECORD_END || top == Symbol.UNION_END) {
      if (last == JsonParser.Event.END_OBJECT) {
        last = in.next();
        if (top == Symbol.RECORD_END) {
          if (currentReorderBuffer != null && !currentReorderBuffer.savedFields.isEmpty()) {
            throw error("Unknown fields: " + currentReorderBuffer.savedFields.keySet());
          }
          currentReorderBuffer = reorderBuffers.pop();
        }
      } else {
        throw error(top == Symbol.RECORD_END ? "record-end" : "union-end");
      }
    } else {
      throw new AvroTypeException("Unknown action symbol " + top);
    }
    return null;
  }

  private static class JsonElement {
    public final JsonParser.Event token;
    public final String value;
    public JsonElement(JsonParser.Event t, String value) {
      this.token = t;
      this.value = value;
    }

    public JsonElement(JsonParser.Event t) {
      this(t, null);
    }
  }

  private List<JsonElement> getValueAsTree(JsonParser in) throws IOException {
    int level = 0;
    List<JsonElement> result = new ArrayList<>();
    do {
      JsonParser.Event t = last;
      switch (t) {
      case START_OBJECT:
      case START_ARRAY:
        level++;
        result.add(new JsonElement(t));
        break;
      case END_OBJECT:
      case END_ARRAY:
        level--;
        result.add(new JsonElement(t));
        break;
      case KEY_NAME:
      case VALUE_STRING:
      case VALUE_NUMBER:
      case VALUE_TRUE:
      case VALUE_FALSE:
      case VALUE_NULL:
        result.add(new JsonElement(t, in.getString()));
        break;
      }
      last = in.next();
    } while (level != 0);
    result.add(new JsonElement(null));
    return result;
  }

  private JsonParser makeParser(final List<JsonElement> elements) throws IOException {
    // todo:
    return null;
    /*
    return new JsonParser() {
      int pos = 0;

      @Override
      public ObjectCodec getCodec() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void setCodec(ObjectCodec c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public JsonParser.Event next() throws IOException {
        pos++;
        return elements.get(pos).token;
      }

      @Override
      public JsonParser skipChildren() throws IOException {
        JsonParser.Event tkn = elements.get(pos).token;
        int level = (tkn == JsonParser.Event.START_ARRAY || tkn == JsonParser.Event.END_ARRAY) ? 1 : 0;
        while (level > 0) {
          switch(elements.get(++pos).token) {
          case START_ARRAY:
          case START_OBJECT:
            level++;
            break;
          case END_ARRAY:
          case END_OBJECT:
            level--;
            break;
          }
        }
        return this;
      }

      @Override
      public boolean isClosed() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getCurrentName() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public JsonStreamContext getParsingContext() {
        throw new UnsupportedOperationException();
      }

      @Override
      public JsonLocation getTokenLocation() {
        throw new UnsupportedOperationException();
      }

      @Override
      public JsonLocation getCurrentLocation() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getString() throws IOException {
        return elements.get(pos).value;
      }

      @Override
      public char[] getStringCharacters() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getStringLength() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getStringOffset() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public Number getNumberValue() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public NumberType getNumberType() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getIntValue() throws IOException {
        return Integer.parseInt(getString());
      }

      @Override
      public long getLongValue() throws IOException {
        return Long.parseLong(getString());
      }

      @Override
      public BigInteger getBigIntegerValue() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public float getFloatValue() throws IOException {
        return Float.parseFloat(getString());
      }

      @Override
      public double getDoubleValue() throws IOException {
        return Double.parseDouble(getString());
      }

      @Override
      public BigDecimal getDecimalValue() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public byte[] getBinaryValue(Base64Variant b64variant)
        throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public JsonParser.Event getCurrentToken() {
        return elements.get(pos).token;
      }
    };
    */
  }

  private AvroTypeException error(String type) {
    return new AvroTypeException("Expected " + type +
        ". Got " + last);
  }

}

