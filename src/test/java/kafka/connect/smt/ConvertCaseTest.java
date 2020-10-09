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

import java.util.*;

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
    public void schemaRepeatedNestedSnakeUnderscore2Camel() {
        final Map<String, String> props = new HashMap<>();
        props.put("convert.from.to", "snakeunderscore2camel");
        xformValue.configure(props);

        final Schema address = SchemaBuilder.struct()
                .field("display_name", Schema.STRING_SCHEMA)
                .field("address", Schema.STRING_SCHEMA)
                .build();

        final Schema addressArray = SchemaBuilder.array(address)
                .build();

        final Schema messageBody = SchemaBuilder.struct()
                .field("body", Schema.STRING_SCHEMA)
                .field("media_type", Schema.STRING_SCHEMA)
                .build();

        final Schema messageBodyArray = SchemaBuilder.array(messageBody)
                .build();

        final Schema attachment = SchemaBuilder.struct()
                .field("filename", Schema.OPTIONAL_STRING_SCHEMA)
                .field("content", Schema.OPTIONAL_STRING_SCHEMA)
                .field("media_type", Schema.OPTIONAL_STRING_SCHEMA)
                .field("disposition", Schema.OPTIONAL_STRING_SCHEMA)
                .field("content_id", Schema.OPTIONAL_STRING_SCHEMA)
                .field("location", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final Schema attachmentArray = SchemaBuilder.array(attachment)
                .build();

        final Schema emailSchema = SchemaBuilder.struct()
                .field("from", address)
                .field("reply_to", addressArray)
                .field("to", addressArray)
                .field("cc", addressArray)
                .field("bcc", addressArray)
                .field("subject", Schema.STRING_SCHEMA)
                .field("message_bodies", messageBodyArray)
                .field("importance", Schema.STRING_SCHEMA)
                .field("attachments", attachmentArray)
                .build();

        final Schema schema = SchemaBuilder.struct()
                .field("email_message", emailSchema)
                .field("user_id", Schema.STRING_SCHEMA)
                .field("testing", Schema.BOOLEAN_SCHEMA)
                .field("requires_signature", Schema.BOOLEAN_SCHEMA)
                .field("correlation_id", Schema.STRING_SCHEMA)
                .build();

/*[
    {
        "emailMessage": {
            "from": {
                "displayName": "Douglas Adams",
                "address": "douglas.adams@panbooks.com"
            },
            "replyTo": [],
            "to": [
                {
                    "display_name": "",
                    "address": "zbeeblebrox@galactic.com"
                }
            ],
            "cc": [],
            "bcc": [],
            "subject": "Testing Protobuf 20201008172027",
            "messageBodies": [
                {
                    "body": "<html><head><title></title></head><body>Test Protobuf</body></html>",
                    "media_type": "text/html"
                },
                {
                    "body": "testing Protobuf",
                    "media_type": "text/plain"
                }
            ],
            "importance": "NORMAL",
            "attachments": []
        },
        "userId": "3453ba7b-6493-4768-a59f-0519a4ab21ba",
        "testing": false,
        "requiresSignature": false,
        "correlationId": "0c84a983-01ba-4972-ab13-207475ce8d04"
    }
] */

        final Struct fromValue = new Struct(address);
        fromValue.put("display_name", "From Display Name");
        fromValue.put("address", "from.display.name@from.example.com");

        final Struct toValue1 = new Struct(address);
        toValue1.put("display_name", "To1 Display Name");
        toValue1.put("address", "to1.display.name@to1.example.com");
        final Struct toValue2 = new Struct(address);
        toValue2.put("display_name", "To2 Display Name");
        toValue2.put("address", "to2.display.name@to2.example.com");
        final List toValueList = new ArrayList<Struct>();
        toValueList.add(toValue1);
        toValueList.add(toValue2);

        final Struct ccValue1 = new Struct(address);
        ccValue1.put("display_name", "CC1 Display Name");
        ccValue1.put("address", "CC1.display.name@CC1.example.com");
        final Struct ccValue2 = new Struct(address);
        ccValue2.put("display_name", "CC2 Display Name");
        ccValue2.put("address", "CC2.display.name@CC2.example.com");
        final List ccValueList = new ArrayList<Struct>();
        ccValueList.add(ccValue1);
        ccValueList.add(ccValue2);

        final Struct messageBodiesValue1 = new Struct(messageBody);
        messageBodiesValue1.put("body", "<html><head><title></title></head><body>Test Protobuf</body></html>");
        messageBodiesValue1.put("media_type", "text/html");
        final Struct messageBodiesValue2 = new Struct(messageBody);
        messageBodiesValue2.put("body", "testing Protobuf");
        messageBodiesValue2.put("media_type", "text/plain");
        final List messageBodiesValueList = new ArrayList<Struct>();
        messageBodiesValueList.add(messageBodiesValue1);
        messageBodiesValueList.add(messageBodiesValue2);

        final Struct emailValue = new Struct(emailSchema);
        emailValue.put("from", fromValue);
        emailValue.put("reply_to", new ArrayList<>());
        emailValue.put("to", toValueList);
        emailValue.put("cc", ccValueList);
        emailValue.put("bcc", new ArrayList<>());
        emailValue.put("subject", "Testing Protobuf 20201008172027");
        emailValue.put("message_bodies", messageBodiesValueList);
        emailValue.put("importance", "NORMAL");
        emailValue.put("attachments", new ArrayList<>());

        final Struct value = new Struct(schema);
        value.put("email_message", emailValue);
        value.put("user_id", "3453ba7b-6493-4768-a59f-0519a4ab21ba");
        value.put("testing", Boolean.FALSE);
        value.put("requires_signature", Boolean.FALSE);
        value.put("correlation_id", "0c84a983-01ba-4972-ab13-207475ce8d04");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(5, updatedValue.schema().fields().size());

        // Check userId, testing, requiresSignature, coorelationId fields
        assertEquals("3453ba7b-6493-4768-a59f-0519a4ab21ba", updatedValue.getString("userId"));
        assertEquals(Boolean.FALSE, updatedValue.getBoolean("testing"));
        assertEquals(Boolean.FALSE, updatedValue.getBoolean("requiresSignature"));
        assertEquals("0c84a983-01ba-4972-ab13-207475ce8d04", updatedValue.getString("correlationId"));

        // Check emailMessage.subject, emailMessage.importance fields
        Struct emailMessage = updatedValue.getStruct("emailMessage");
        assertEquals("Testing Protobuf 20201008172027", emailMessage.getString("subject"));
        assertEquals("NORMAL", emailMessage.getString("importance"));

        // Check emailMessage.from
        Struct from = emailMessage.getStruct("from");
        assertEquals("From Display Name", from.getString("displayName"));
        assertEquals("from.display.name@from.example.com", from.getString("address"));

        // Check emailMessage.to
        List list = emailMessage.getArray("to");
        assertNotNull(list);
        assertEquals(2, list.size());
        list.forEach(e -> {
            Struct struct = (Struct)e;
            if (struct.getString("displayName").equals("To1 Display Name")) {
                assertEquals("to1.display.name@to1.example.com", struct.getString("address"));
            } else if (struct.getString("displayName").equals("To2 Display Name")) {
                assertEquals("to2.display.name@to2.example.com", struct.getString("address"));
            } else {
                fail("Array is missing elements");
            }
        });

        // Check emailMessage.replyTo
        list = emailMessage.getArray("replyTo");
        assertNotNull(list);
        assertEquals(0, list.size());

        // Check emailMessage.cc
        list = emailMessage.getArray("cc");
        assertNotNull(list);
        assertEquals(2, list.size());
        list.forEach(e -> {
            Struct struct = (Struct)e;
            if (struct.getString("displayName").equals("CC1 Display Name")) {
                assertEquals("CC1.display.name@CC1.example.com", struct.getString("address"));
            } else if (struct.getString("displayName").equals("CC2 Display Name")) {
                assertEquals("CC2.display.name@CC2.example.com", struct.getString("address"));
            } else {
                fail("Array is missing elements");
            }
        });

        // Check emailMessage.bcc
        list = emailMessage.getArray("bcc");
        assertNotNull(list);
        assertEquals(0, list.size());

        // check emailMessage.messageBodies
        list = emailMessage.getArray("messageBodies");
        assertNotNull(list);
        assertEquals(2, list.size());
        list.forEach(e -> {
            Struct struct = (Struct)e;
            if (struct.getString("body").equals("<html><head><title></title></head><body>Test Protobuf</body></html>")) {
                assertEquals("text/html", struct.getString("mediaType"));
            } else if (struct.getString("body").equals("testing Protobuf")) {
                assertEquals("text/plain", struct.getString("mediaType"));
            } else {
                fail("Array is missing elements");
            }
        });

        // check emailMessage.attachments
        list = emailMessage.getArray("attachments");
        assertNotNull(list);
        assertEquals(0, list.size());
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