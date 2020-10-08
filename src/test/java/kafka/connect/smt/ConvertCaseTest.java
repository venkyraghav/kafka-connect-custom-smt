/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ConvertCaseTest {

    private ConvertCase<SinkRecord> xformValue = new ConvertCase.Value<>();
    private ConvertCase<SinkRecord> xformKey = new ConvertCase.Key<>();

    @After
    public void teardown() {
        xformValue.close();
        xformKey.close();
    }

    @Test
    public void tombstoneSchemaless() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "uppercase");

        xformValue.configure(props);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, null, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        assertNull(transformedRecord.value());
        assertNull(transformedRecord.valueSchema());
    }

    @Test
    public void tombstoneWithSchema() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "snakeunderscore2camel");

        xformValue.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("first_name", Schema.STRING_SCHEMA)
                .field("address_number", Schema.INT32_SCHEMA)
                .field("living", Schema.BOOLEAN_SCHEMA)
                .field("salary", Schema.FLOAT32_SCHEMA)
                .build();

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, null, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        assertNull(transformedRecord.value());
        assertEquals(schema, transformedRecord.valueSchema());
    }

    @Test
    public void schemalessUppercase() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "uppercase");

        xformValue.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("first_name", "whatever");
        value.put("address_number", 123);
        value.put("living", Boolean.TRUE);
        value.put("salary", 100.32);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(4, updatedValue.size());
        assertEquals("whatever", updatedValue.get("FIRST_NAME"));
        assertEquals(123, updatedValue.get("ADDRESS_NUMBER"));
        assertEquals(Boolean.TRUE, updatedValue.get("LIVING"));
        assertEquals(100.32, updatedValue.get("SALARY"));
    }

    @Test
    public void schemalessLowercase() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "lowercase");

        xformValue.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("first_name", "whatever");
        value.put("address_number", 123);
        value.put("living", Boolean.TRUE);
        value.put("SALARY", 100.32);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(4, updatedValue.size());
        assertEquals("whatever", updatedValue.get("first_name"));
        assertEquals(123, updatedValue.get("address_number"));
        assertEquals(Boolean.TRUE, updatedValue.get("living"));
        assertEquals(100.32, updatedValue.get("salary"));
    }

    @Test
    public void schemalessSnakeUnderscore2Camel() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "snakeunderscore2camel");

        xformValue.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("first_name", "whatever");
        value.put("address_number", 123);
        value.put("living", Boolean.TRUE);
        value.put("salary", 100.32);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(4, updatedValue.size());
        assertEquals("whatever", updatedValue.get("firstName"));
        assertEquals(123, updatedValue.get("addressNumber"));
        assertEquals(Boolean.TRUE, updatedValue.get("living"));
        assertEquals(100.32, updatedValue.get("salary"));

    }

    @Test
    public void schemalessSnakeHyphen2Camel() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "snakehyphen2camel");

        xformValue.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("first-name", "whatever");
        value.put("address-number", 123);
        value.put("living", Boolean.TRUE);
        value.put("salary", 100.32);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(4, updatedValue.size());
        assertEquals("whatever", updatedValue.get("firstName"));
        assertEquals(123, updatedValue.get("addressNumber"));
        assertEquals(Boolean.TRUE, updatedValue.get("living"));
        assertEquals(100.32, updatedValue.get("salary"));
    }

    @Test
    public void schemalessCamel2SnakeUnderscore() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "camel2snakeunderscore");

        xformValue.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("firstName", "whatever");
        value.put("addressNumber", 123);
        value.put("living", Boolean.TRUE);
        value.put("salary", 100.32);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(4, updatedValue.size());
        assertEquals("whatever", updatedValue.get("first_name"));
        assertEquals(123, updatedValue.get("address_number"));
        assertEquals(Boolean.TRUE, updatedValue.get("living"));
        assertEquals(100.32, updatedValue.get("salary"));

    }

    @Test
    public void schemalessCamel2SnakeHyphen() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "camel2snakehyphen");

        xformValue.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("firstName", "whatever");
        value.put("addressNumber", 123);
        value.put("living", Boolean.TRUE);
        value.put("salary", 100.32);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(4, updatedValue.size());
        assertEquals("whatever", updatedValue.get("first-name"));
        assertEquals(123, updatedValue.get("address-number"));
        assertEquals(Boolean.TRUE, updatedValue.get("living"));
        assertEquals(100.32, updatedValue.get("salary"));
    }

    @Test
    public void schemalessNestedSnakeUnderscore2Camel() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "snakeunderscore2camel");

        xformValue.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("first_name", "whatever");
        value.put("address_number", new Integer(123));
        value.put("living", Boolean.TRUE);
        value.put("salary", new Float(100.32));

        final Map<String, Object> inner = new HashMap<>();
        value.put("inner_map", inner);
        inner.put("display_name", "Test Display");

        final Map<String, Object> innerMost = new HashMap<>();
        inner.put("inner_most", innerMost);
        innerMost.put("media_type", "TestMediaType");

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(5, updatedValue.size());
        assertEquals("whatever", updatedValue.get("firstName"));
        assertEquals(Integer.valueOf(123), updatedValue.get("addressNumber"));
        assertEquals(Boolean.TRUE, updatedValue.get("living"));
        assertEquals(Float.valueOf(100.32f), updatedValue.get("salary"));

        HashMap updatedValueInner = (HashMap) updatedValue.get("innerMap");
        assertNotNull(updatedValueInner);
        assertEquals("Test Display", updatedValueInner.get("displayName"));

        HashMap updatedValueInnerMost = (HashMap) updatedValueInner.get("innerMost");
        assertNotNull(updatedValueInnerMost);
        assertEquals("TestMediaType", updatedValueInnerMost.get("mediaType"));
    }

    @Test
    public void schemaUppercase() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "uppercase");

        xformValue.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("first_name", Schema.STRING_SCHEMA)
                .field("address_number", Schema.INT32_SCHEMA)
                .field("living", Schema.BOOLEAN_SCHEMA)
                .field("salary", Schema.FLOAT32_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("first_name", "whatever");
        value.put("address_number", 123);
        value.put("living", Boolean.TRUE);
        value.put("salary", 100.32f);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(4, updatedValue.schema().fields().size());
        assertEquals("whatever", updatedValue.getString("FIRST_NAME"));
        assertEquals(Integer.valueOf(123), updatedValue.getInt32("ADDRESS_NUMBER"));
        assertEquals(Boolean.TRUE, updatedValue.getBoolean("LIVING"));
        assertEquals(Float.valueOf(100.32f), updatedValue.getFloat32("SALARY"));
    }

    @Test
    public void schemaLowercase() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "lowercase");

        xformValue.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("FIRST_NAME", Schema.STRING_SCHEMA)
                .field("ADDRESS_NUMBER", Schema.INT32_SCHEMA)
                .field("living", Schema.BOOLEAN_SCHEMA)
                .field("SALARY", Schema.FLOAT32_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("FIRST_NAME", "whatever");
        value.put("ADDRESS_NUMBER", 123);
        value.put("living", Boolean.TRUE);
        value.put("SALARY", 100.32f);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(4, updatedValue.schema().fields().size());
        assertEquals("whatever", updatedValue.getString("first_name"));
        assertEquals(Integer.valueOf(123), updatedValue.getInt32("address_number"));
        assertEquals(Boolean.TRUE, updatedValue.getBoolean("living"));
        assertEquals(Float.valueOf(100.32f), updatedValue.getFloat32("salary"));
    }

    @Test
    public void schemaSnakeUnderscore2Camel() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "snakeunderscore2camel");

        xformValue.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("first_name", Schema.STRING_SCHEMA)
                .field("address_number", Schema.INT32_SCHEMA)
                .field("living", Schema.BOOLEAN_SCHEMA)
                .field("salary", Schema.FLOAT32_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("first_name", "whatever");
        value.put("address_number", 123);
        value.put("living", Boolean.TRUE);
        value.put("salary", 100.32f);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(4, updatedValue.schema().fields().size());
        assertEquals("whatever", updatedValue.getString("firstName"));
        assertEquals(Integer.valueOf(123), updatedValue.getInt32("addressNumber"));
        assertEquals(Boolean.TRUE, updatedValue.getBoolean("living"));
        assertEquals(Float.valueOf(100.32f), updatedValue.getFloat32("salary"));
    }

    @Test
    public void schemaSnakeHyphen2Camel() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "snakehyphen2camel");

        xformValue.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("first-name", Schema.STRING_SCHEMA)
                .field("address-number", Schema.INT32_SCHEMA)
                .field("living", Schema.BOOLEAN_SCHEMA)
                .field("salary", Schema.FLOAT32_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("first-name", "whatever");
        value.put("address-number", 123);
        value.put("living", Boolean.TRUE);
        value.put("salary", 100.32f);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(4, updatedValue.schema().fields().size());
        assertEquals("whatever", updatedValue.getString("firstName"));
        assertEquals(Integer.valueOf(123), updatedValue.getInt32("addressNumber"));
        assertEquals(Boolean.TRUE, updatedValue.getBoolean("living"));
        assertEquals(Float.valueOf(100.32f), updatedValue.getFloat32("salary"));
    }

    @Test
    public void schemaCamel2SnakeUnderscore() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "camel2snakeunderscore");

        xformValue.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("firstName", Schema.STRING_SCHEMA)
                .field("addressNumber", Schema.INT32_SCHEMA)
                .field("living", Schema.BOOLEAN_SCHEMA)
                .field("salary", Schema.FLOAT32_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("firstName", "whatever");
        value.put("addressNumber", 123);
        value.put("living", Boolean.TRUE);
        value.put("salary", 100.32f);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(4, updatedValue.schema().fields().size());
        assertEquals("whatever", updatedValue.getString("first_name"));
        assertEquals(Integer.valueOf(123), updatedValue.getInt32("address_number"));
        assertEquals(Boolean.TRUE, updatedValue.getBoolean("living"));
        assertEquals(Float.valueOf(100.32f), updatedValue.getFloat32("salary"));
    }

    @Test
    public void schemaCamel2SnakeHyphen() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "camel2snakehyphen");

        xformValue.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("firstName", Schema.STRING_SCHEMA)
                .field("addressNumber", Schema.INT32_SCHEMA)
                .field("living", Schema.BOOLEAN_SCHEMA)
                .field("salary", Schema.FLOAT32_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("firstName", "whatever");
        value.put("addressNumber", 123);
        value.put("living", Boolean.TRUE);
        value.put("salary", 100.32f);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(4, updatedValue.schema().fields().size());
        assertEquals("whatever", updatedValue.getString("first-name"));
        assertEquals(Integer.valueOf(123), updatedValue.getInt32("address-number"));
        assertEquals(Boolean.TRUE, updatedValue.getBoolean("living"));
        assertEquals(Float.valueOf(100.32f), updatedValue.getFloat32("salary"));
    }

    @Test
    public void schemaCamel2SnakeHyphenKey() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "camel2snakehyphen");

        xformKey.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("firstName", Schema.STRING_SCHEMA)
                .field("addressNumber", Schema.INT32_SCHEMA)
                .field("living", Schema.BOOLEAN_SCHEMA)
                .field("salary", Schema.FLOAT32_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("firstName", "whatever");
        value.put("addressNumber", 123);
        value.put("living", Boolean.TRUE);
        value.put("salary", 100.32f);

        final SinkRecord record = new SinkRecord("test", 0, schema, value, null, null, 0);
        final SinkRecord transformedRecord = xformKey.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.key();
        assertEquals(4, updatedValue.schema().fields().size());
        assertEquals("whatever", updatedValue.getString("first-name"));
        assertEquals(Integer.valueOf(123), updatedValue.getInt32("address-number"));
        assertEquals(Boolean.TRUE, updatedValue.getBoolean("living"));
        assertEquals(Float.valueOf(100.32f), updatedValue.getFloat32("salary"));
    }

    @Test
    public void schemaNestedSnakeUnderscore2Camel() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "snakeunderscore2camel");

        xformValue.configure(props);

        final Schema innerMostSchema = SchemaBuilder.struct()
                .field("media_type", Schema.STRING_SCHEMA)
                .build();

        final Schema innerSchema = SchemaBuilder.struct()
                .field("display_name", Schema.STRING_SCHEMA)
                .field("inner_most_schema", innerMostSchema)
                .build();

        final Schema schema = SchemaBuilder.struct()
                .field("first_name", Schema.STRING_SCHEMA)
                .field("address_number", Schema.INT32_SCHEMA)
                .field("living", Schema.BOOLEAN_SCHEMA)
                .field("salary", Schema.FLOAT32_SCHEMA)
                .field("inner_schema", innerSchema)
                .build();

        final Struct innerMostValue = new Struct(innerMostSchema);
        innerMostValue.put("media_type", "TestMediaType");

        final Struct innerValue = new Struct(innerSchema);
        innerValue.put("display_name", "Test Display");
        innerValue.put("inner_most_schema", innerMostValue);

        final Struct value = new Struct(schema);
        value.put("first_name", "whatever");
        value.put("address_number", 123);
        value.put("living", Boolean.TRUE);
        value.put("salary", 100.32f);
        value.put("inner_schema", innerValue);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(5, updatedValue.schema().fields().size());
        assertEquals("whatever", updatedValue.getString("firstName"));
        assertEquals(Integer.valueOf(123), updatedValue.getInt32("addressNumber"));
        assertEquals(Boolean.TRUE, updatedValue.getBoolean("living"));
        assertEquals(Float.valueOf(100.32f), updatedValue.getFloat32("salary"));
        assertNotNull(updatedValue.getStruct("innerSchema"));
        assertEquals("Test Display", updatedValue.getStruct("innerSchema").getString("displayName"));
        assertNotNull(updatedValue.getStruct("innerSchema").getStruct("innerMostSchema"));
        assertEquals("TestMediaType", updatedValue.getStruct("innerSchema").getStruct("innerMostSchema").getString("mediaType"));
    }

    @Test
    public void schemalessWhitelist() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "uppercase");
        props.put("whitelist", "first_name");

        xformValue.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("first_name", "whatever");
        value.put("address_number", Integer.valueOf(123));
        value.put("living", Boolean.TRUE);
        value.put("SALARY", Float.valueOf(100.32f));

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(1, updatedValue.size());
        assertEquals("whatever", updatedValue.get("FIRST_NAME"));
        assertNull(updatedValue.get("ADDRESS_NUMBER"));
        assertNull(updatedValue.get("LIVING"));
        assertNull(updatedValue.get("SALARY"));
    }

    @Test
    public void schemalessBlacklist() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "uppercase");
        props.put("blacklist", "first_name");

        xformValue.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("first_name", "whatever");
        value.put("address_number", 123);
        value.put("living", Boolean.TRUE);
        value.put("SALARY", 100.32);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(3, updatedValue.size());
        assertNull(updatedValue.get("FIRST_NAME"));
        assertEquals(123, updatedValue.get("ADDRESS_NUMBER"));
        assertEquals(Boolean.TRUE, updatedValue.get("LIVING"));
        assertEquals(100.32, updatedValue.get("SALARY"));
    }

    @Test
    public void schemalessNoop() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "uppercase");
        props.put("noop", "first_name");

        xformValue.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("first_name", "whatever");
        value.put("address_number", 123);
        value.put("living", Boolean.TRUE);
        value.put("SALARY", 100.32);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(4, updatedValue.size());
        assertEquals("whatever", updatedValue.get("first_name"));
        assertEquals(123, updatedValue.get("ADDRESS_NUMBER"));
        assertEquals(Boolean.TRUE, updatedValue.get("LIVING"));
        assertEquals(100.32, updatedValue.get("SALARY"));
    }

    @Test
    public void schemalessNoopBlacklist() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "uppercase");
        props.put("noop", "first_name");
        props.put("blacklist", "address_number");

        xformValue.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("first_name", "whatever");
        value.put("address_number", 123);
        value.put("living", Boolean.TRUE);
        value.put("SALARY", 100.32);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(3, updatedValue.size());
        assertEquals("whatever", updatedValue.get("first_name"));
        assertNull(updatedValue.get("ADDRESS_NUMBER"));
        assertEquals(Boolean.TRUE, updatedValue.get("LIVING"));
        assertEquals(100.32, updatedValue.get("SALARY"));
    }

    @Test
    public void schemalessNoopWhitelist() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "uppercase");
        props.put("noop", "first_name");
        props.put("whitelist", "address_number");

        xformValue.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("first_name", "whatever");
        value.put("address_number", 123);
        value.put("living", Boolean.TRUE);
        value.put("SALARY", 100.32);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(2, updatedValue.size());
        assertEquals("whatever", updatedValue.get("first_name"));
        assertEquals(123, updatedValue.get("ADDRESS_NUMBER"));
    }

    @Test
    public void schemalessWhitelistBlacklist() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "uppercase");
        props.put("whitelist", "address_number,first_name");
        props.put("blacklist", "address_number");

        xformValue.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("first_name", "whatever");
        value.put("address_number", 123);
        value.put("living", Boolean.TRUE);
        value.put("SALARY", 100.32);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(1, updatedValue.size());
        assertEquals("whatever", updatedValue.get("FIRST_NAME"));
    }
}