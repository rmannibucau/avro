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
package org.apache.avro;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.stream.JsonGenerator;

import org.apache.avro.Schema.Field;
import org.apache.avro.util.internal.JsonUtils;

/** A set of messages forming an application protocol.
 * <p> A protocol consists of:
 * <ul>
 * <li>a <i>name</i> for the protocol;
 * <li>an optional <i>namespace</i>, further qualifying the name;
 * <li>a list of <i>types</i>, or named {@link Schema schemas};
 * <li>a list of <i>errors</i>, or named {@link Schema schemas} for exceptions;
 * <li>a list of named <i>messages</i>, each of which specifies,
 *   <ul>
 *   <li><i>request</i>, the parameter schemas;
 *   <li>one of either;
 *     <ul><li>one-way</li></ul>
 *   or
 *     <ul>
 *       <li><i>response</i>, the response schema;
 *       <li><i>errors</i>, an optional list of potential error schema names.
 *     </ul>
 *   </ul>
 * </ul>
 */
public class Protocol extends JsonProperties {
  /** The version of the protocol specification implemented here. */
  public static final long VERSION = 1;

  // Support properties for both Protocol and Message objects
  private static final Set<String> MESSAGE_RESERVED = new HashSet<>();
  static {
    Collections.addAll(MESSAGE_RESERVED,
                       "doc", "response","request", "errors", "one-way");
  }

  private static final Set<String> FIELD_RESERVED = new HashSet<>();
  static {
    Collections.addAll(FIELD_RESERVED,
                       "name", "type", "doc", "default", "aliases");
  }

  /** A protocol message. */
  public class Message extends JsonProperties {
    private String name;
    private String doc;
    private Schema request;

    /** Construct a message. */
    private Message(String name, String doc,
                    Map<String,?> propMap, Schema request) {
      super(MESSAGE_RESERVED);
      this.name = name;
      this.doc = doc;
      this.request = request;

      if (propMap != null)                        // copy props
        for (Map.Entry<String,?> prop : propMap.entrySet()) {
          Object value = prop.getValue();
          this.addProp(prop.getKey(),
                       value instanceof String
                       ? JsonUtils.getProvider().createValue((String)value)
                       : (JsonValue) value);
        }
    }

    /** The name of this message. */
    public String getName() { return name; }
    /** The parameters of this message. */
    public Schema getRequest() { return request; }
    /** The returned data. */
    public Schema getResponse() { return Schema.create(Schema.Type.NULL); }
    /** Errors that might be thrown. */
    public Schema getErrors() {
      return Schema.createUnion(new ArrayList<>());
    }

    /** Returns true if this is a one-way message, with no response or errors.*/
    public boolean isOneWay() { return true; }

    public String toString() {
      try {
        StringWriter writer = new StringWriter();
        JsonGenerator gen = Schema.FACTORY.createGenerator(writer);
        toJson(gen);
        gen.close();
        return writer.toString();
      } catch (IOException e) {
        throw new AvroRuntimeException(e);
      }
    }
    void toJson(JsonGenerator gen) throws IOException {
      gen.writeStartObject();
      if (doc != null) gen.write("doc", doc);
      writeProps(gen);                           // write out properties
      gen.write("request");
      request.fieldsToJson(types, gen);

      toJson1(gen);
      gen.writeEnd();
    }

    void toJson1(JsonGenerator gen) throws IOException {
      gen.write("response", "null");
      gen.write("one-way", true);
    }

    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof Message)) return false;
      Message that = (Message)o;
      return this.name.equals(that.name)
        && this.request.equals(that.request)
        && props.equals(that.props);
    }

    public int hashCode() {
      return name.hashCode() + request.hashCode() + props.hashCode();
    }

    public String getDoc() { return doc; }

  }

  private class TwoWayMessage extends Message {
    private Schema response;
    private Schema errors;

    /** Construct a message. */
    private TwoWayMessage(String name, String doc, Map<String,?> propMap,
                          Schema request, Schema response, Schema errors) {
      super(name, doc, propMap, request);
      this.response = response;
      this.errors = errors;
    }

    @Override public Schema getResponse() { return response; }
    @Override public Schema getErrors() { return errors; }
    @Override public boolean isOneWay() { return false; }

    @Override public boolean equals(Object o) {
      if (!super.equals(o)) return false;
      if (!(o instanceof TwoWayMessage)) return false;
      TwoWayMessage that = (TwoWayMessage)o;
      return this.response.equals(that.response)
        && this.errors.equals(that.errors);
    }

    @Override public int hashCode() {
      return super.hashCode() + response.hashCode() + errors.hashCode();
    }

    @Override void toJson1(JsonGenerator gen) throws IOException {
      gen.write("response");
      response.toJson(types, gen);

      List<Schema> errs = errors.getTypes();  // elide system error
      if (errs.size() > 1) {
        Schema union = Schema.createUnion(errs.subList(1, errs.size()));
        gen.write("errors");
        union.toJson(types, gen);
      }
    }

  }

  private String name;
  private String namespace;
  private String doc;

  private Schema.Names types = new Schema.Names();
  private Map<String,Message> messages = new LinkedHashMap<>();
  private byte[] md5;

  /** An error that can be thrown by any message. */
  public static final Schema SYSTEM_ERROR = Schema.create(Schema.Type.STRING);

  /** Union type for generating system errors. */
  public static final Schema SYSTEM_ERRORS;
  static {
    List<Schema> errors = new ArrayList<>();
    errors.add(SYSTEM_ERROR);
    SYSTEM_ERRORS = Schema.createUnion(errors);
  }

  private static final Set<String> PROTOCOL_RESERVED = new HashSet<>();
  static {
    Collections.addAll(PROTOCOL_RESERVED,
       "namespace", "protocol", "doc",
       "messages","types", "errors");
  }

  private Protocol() {
    super(PROTOCOL_RESERVED);
  }

  public Protocol(String name, String doc, String namespace) {
    super(PROTOCOL_RESERVED);
    this.name = name;
    this.doc = doc;
    this.namespace = namespace;
  }
  public Protocol(String name, String namespace) {
    this(name, null, namespace);
  }

  /** The name of this protocol. */
  public String getName() { return name; }

  /** The namespace of this protocol.  Qualifies its name. */
  public String getNamespace() { return namespace; }

  /** Doc string for this protocol. */
  public String getDoc() { return doc; }

  /** The types of this protocol. */
  public Collection<Schema> getTypes() { return types.values(); }

  /** Returns the named type. */
  public Schema getType(String name) { return types.get(name); }

  /** Set the types of this protocol. */
  public void setTypes(Collection<Schema> newTypes) {
    types = new Schema.Names();
    for (Schema s : newTypes)
      types.add(s);
  }

  /** The messages of this protocol. */
  public Map<String,Message> getMessages() { return messages; }

  /** Create a one-way message. */
  @Deprecated
  public Message createMessage(String name, String doc, Schema request) {
    return createMessage(name, doc, new LinkedHashMap<String,String>(),request);
  }
  /** Create a one-way message. */
  public <T> Message createMessage(String name, String doc,
                                   Map<String,T> propMap, Schema request) {
    return new Message(name, doc, propMap, request);
  }

  /** Create a two-way message. */
  @Deprecated
  public Message createMessage(String name, String doc, Schema request,
                               Schema response, Schema errors) {
    return createMessage(name, doc, new LinkedHashMap<String,String>(),
                         request, response, errors);
  }
  /** Create a two-way message. */
  public <T> Message createMessage(String name, String doc,
                                   Map<String,T> propMap, Schema request,
                                   Schema response, Schema errors) {
    return new TwoWayMessage(name, doc, propMap, request, response, errors);
  }

  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Protocol)) return false;
    Protocol that = (Protocol)o;
    return this.name.equals(that.name)
      && this.namespace.equals(that.namespace)
      && this.types.equals(that.types)
      && this.messages.equals(that.messages)
      && this.props.equals(that.props);
  }

  public int hashCode() {
    return name.hashCode() + namespace.hashCode()
      + types.hashCode() + messages.hashCode() + props.hashCode();
  }

  /** Render this as <a href="http://json.org/">JSON</a>.*/
  @Override
  public String toString() { return toString(false); }

  /** Render this as <a href="http://json.org/">JSON</a>.
   * @param pretty if true, pretty-print JSON.
   */
  public String toString(boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator gen = (pretty ? Schema.PRETTY_FACTORY : Schema.FACTORY).createGenerator(writer);
      toJson(gen);
      gen.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }
  void toJson(JsonGenerator gen) throws IOException {
    types.space(namespace);

    gen.writeStartObject();
    gen.write("protocol", name);
    gen.write("namespace", namespace);

    if (doc != null) gen.write("doc", doc);
    writeProps(gen);
    gen.writeStartArray("types");
    Schema.Names resolved = new Schema.Names(namespace);
    for (Schema type : types.values())
      if (!resolved.contains(type))
        type.toJson(resolved, gen);
    gen.writeEnd();

    gen.writeStartObject("messages");
    for (Map.Entry<String,Message> e : messages.entrySet()) {
      gen.write(e.getKey());
      e.getValue().toJson(gen);
    }
    gen.writeEnd();
    gen.writeEnd();
  }

  /** Return the MD5 hash of the text of this protocol. */
  public byte[] getMD5() {
    if (md5 == null)
      try {
        md5 = MessageDigest.getInstance("MD5")
          .digest(this.toString().getBytes("UTF-8"));
      } catch (Exception e) {
        throw new AvroRuntimeException(e);
      }
    return md5;
  }

  /** Read a protocol from a Json file. */
  public static Protocol parse(File file) throws IOException {
    return parse(new FileInputStream(file));
  }

  /** Read a protocol from a Json stream. */
  public static Protocol parse(InputStream stream) throws IOException {
    try {
      return parse(Schema.MAPPER.fromJson(stream, JsonValue.class));
    } finally {
      stream.close();
    }
  }

  /** Read a protocol from one or more json strings */
  public static Protocol parse(String string, String... more) {
    StringBuilder b = new StringBuilder(string);
    for (String part : more)
      b.append(part);
    return parse(b.toString());
  }

  /** Read a protocol from a Json string. */
  public static Protocol parse(String string) {
    return parse(Schema.MAPPER.fromJson(string, JsonValue.class));
  }

  private static Protocol parse(JsonValue json) {
    Protocol protocol = new Protocol();
    protocol.parseNamespace(json);
    protocol.parseName(json);
    protocol.parseTypes(json);
    protocol.parseMessages(json);
    protocol.parseDoc(json);
    protocol.parseProps(json);
    return protocol;
  }

  private void parseNamespace(JsonValue json) {
    JsonValue nameNode = json.asJsonObject().get("namespace");
    if (nameNode == null) return;                 // no namespace defined
    this.namespace = JsonString.class.cast(nameNode).getString();
    types.space(this.namespace);
  }

  private void parseDoc(JsonValue json) {
    this.doc = parseDocNode(json);
  }

  private String parseDocNode(JsonValue json) {
    JsonValue nameNode = json.asJsonObject().get("doc");
    if (nameNode == null) return null;                 // no doc defined
    return JsonString.class.cast(nameNode).getString();
  }

  private void parseName(JsonValue json) {
    JsonValue nameNode = json.asJsonObject().get("protocol");
    if (nameNode == null)
      throw new SchemaParseException("No protocol name specified: "+json);
    this.name = JsonString.class.cast(nameNode).getString();
  }

  private void parseTypes(JsonValue json) {
    JsonValue defs = json.asJsonObject().get("types");
    if (defs == null) return;                    // no types defined
    if (defs.getValueType() != JsonValue.ValueType.ARRAY)
      throw new SchemaParseException("Types not an array: "+defs);
    for (JsonValue type : defs.asJsonArray()) {
      if (type.getValueType() != JsonValue.ValueType.OBJECT)
        throw new SchemaParseException("Type not an object: "+type);
      Schema.parse(type, types);
    }
  }

  private void parseProps(JsonValue json) {
    JsonObject jsonObject = json.asJsonObject();
    for (Iterator<String> i = jsonObject.keySet().iterator(); i.hasNext();) {
      String p = i.next();                        // add non-reserved as props
      if (!PROTOCOL_RESERVED.contains(p))
        this.addProp(p, jsonObject.get(p));
    }
  }

  private void parseMessages(JsonValue json) {
    JsonObject jsonObject = json.asJsonObject();
    JsonValue defs = jsonObject.get("messages");
    if (defs == null) return;                    // no messages defined
    for (Iterator<String> i = jsonObject.keySet().iterator(); i.hasNext();) {
      String prop = i.next();
      this.messages.put(prop, parseMessage(prop, jsonObject.get(prop)));
    }
  }

  private Message parseMessage(String messageName, JsonValue json) {
    String doc = parseDocNode(json);

    JsonObject jsonObject = json.asJsonObject();
    Map<String,JsonValue> mProps = new LinkedHashMap<>();
    for (Iterator<String> i = jsonObject.keySet().iterator(); i.hasNext();) {
      String p = i.next();                        // add non-reserved as props
      if (!MESSAGE_RESERVED.contains(p))
        mProps.put(p, jsonObject.get(p));
    }

    JsonValue requestNode = jsonObject.get("request");
    if (requestNode == null || requestNode.getValueType() != JsonValue.ValueType.ARRAY)
      throw new SchemaParseException("No request specified: "+json);
    List<Field> fields = new ArrayList<>();
    for (JsonValue field : requestNode.asJsonArray()) {
      JsonObject object = field.asJsonObject();
      JsonValue fieldNameNode = object.get("name");
      if (fieldNameNode == null)
        throw new SchemaParseException("No param name: "+field);
      JsonValue fieldTypeNode = object.get("type");
      if (fieldTypeNode == null)
        throw new SchemaParseException("No param type: "+field);
      String name = JsonString.class.cast(fieldNameNode).getString();
      String fieldDoc = null;
      JsonValue fieldDocNode = object.get("doc");
      if (fieldDocNode != null)
        fieldDoc = JsonString.class.cast(fieldDocNode).getString();
      Field newField = new Field(name, Schema.parse(fieldTypeNode,types),
                                 fieldDoc, object.get("default"));
      Set<String> aliases = Schema.parseAliases(field);
      if (aliases != null) {                      // add aliases
        for (String alias : aliases)
          newField.addAlias(alias);
      }

      Iterator<String> i = object.keySet().iterator();
      while (i.hasNext()) {                       // add properties
        String prop = i.next();
        if (!FIELD_RESERVED.contains(prop))      // ignore reserved
          newField.addProp(prop, object.get(prop));
      }
      fields.add(newField);
    }
    Schema request = Schema.createRecord(fields);

    boolean oneWay = false;
    JsonObject object = json.asJsonObject();
    JsonValue oneWayNode = object.get("one-way");
    if (oneWayNode != null) {
      if (oneWayNode.getValueType() != JsonValue.ValueType.TRUE && oneWayNode.getValueType() != JsonValue.ValueType.FALSE)
        throw new SchemaParseException("one-way must be boolean: "+json);
      oneWay = JsonValue.TRUE.equals(oneWayNode);
    }

    JsonValue responseNode = object.get("response");
    if (!oneWay && responseNode == null)
      throw new SchemaParseException("No response specified: "+json);

    JsonValue decls = object.get("errors");

    if (oneWay) {
      if (decls != null)
        throw new SchemaParseException("one-way can't have errors: "+json);
      if (responseNode != null
          && Schema.parse(responseNode, types).getType() != Schema.Type.NULL)
        throw new SchemaParseException("One way response must be null: "+json);
      return new Message(messageName, doc, mProps, request);
    }

    Schema response = Schema.parse(responseNode, types);

    List<Schema> errs = new ArrayList<>();
    errs.add(SYSTEM_ERROR);                       // every method can throw
    if (decls != null) {
      if (decls.getValueType() != JsonValue.ValueType.ARRAY)
        throw new SchemaParseException("Errors not an array: "+json);
      for (JsonValue decl : decls.asJsonArray()) {
        String name = JsonString.class.cast(decl).getString();
        Schema schema = this.types.get(name);
        if (schema == null)
          throw new SchemaParseException("Undefined error: "+name);
        if (!schema.isError())
          throw new SchemaParseException("Not an error: "+name);
        errs.add(schema);
      }
    }

    return new TwoWayMessage(messageName, doc, mProps, request, response,
                             Schema.createUnion(errs));
  }

  public static void main(String[] args) throws Exception {
    System.out.println(Protocol.parse(new File(args[0])));
  }

}

