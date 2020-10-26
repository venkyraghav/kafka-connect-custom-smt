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

import com.google.common.base.CaseFormat;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Converts field name cases of KEY and VALUE
 *
 * @param <R>
 */
public abstract class ConvertCase<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(ConvertCase.class);

    public static final String OVERVIEW_DOC = "Converts record field name cases";

    protected enum CaseType {
        UPPERCASE,
        LOWERCASE,
        SNAKEHYPHEN2CAMEL,
        SNAKEUNDERSCORE2CAMEL,
        CAMEL2SNAKEHYPHEN,
        CAMEL2SNAKEUNDERSCORE;

        public static CaseType getEnum(String name) {
            try {
                String name2 = name != null ? name.toUpperCase() : null;
                return valueOf(name2);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        ConfigName.CONVERT_FROM_TO + " has invalid value. "
                                + "Valid values include " + Arrays.asList(CaseType.values()));
            }
        }
    }

    private interface ConfigName {
        String CONVERT_FROM_TO ="convert.from.to";
        String WHITELIST = "whitelist";
        String BLACKLIST = "blacklist";
        String NOOP = "noop";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.CONVERT_FROM_TO,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.HIGH,
                    "Conversion to i.e. uppercase, lowercase, snakehyphen2camel, snakeunderscore2camel, camel2snakehyphen, camel2snakeunderscore"
            )
            .define(ConfigName.WHITELIST,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "White list of record elements"
            )
            .define(ConfigName.BLACKLIST,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Black list (or filter out) of record elements"
            )
            .define(ConfigName.NOOP,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "No operation on record elements"
            );

    private static final String PURPOSE = "convert field names from source case to destination case";

    private String convertFromTo;
    private String whitelist;
    private String blacklist;
    private String noop;
    private Cache<String, Schema> schemaUpdateCache;
    private Map<String, String> reverseRenames;

    protected long schemaUpdateCacheSize() {
        return schemaUpdateCache.size();
    }

    @Override
    public void configure(Map<String, ?> map) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, map);
        convertFromTo = config.getString(ConfigName.CONVERT_FROM_TO);
        whitelist = config.getString(ConfigName.WHITELIST);
        blacklist = config.getString(ConfigName.BLACKLIST);
        noop = config.getString(ConfigName.NOOP);

        switch (CaseType.getEnum(convertFromTo)) {
            case CAMEL2SNAKEHYPHEN:
            case CAMEL2SNAKEUNDERSCORE:
            case SNAKEUNDERSCORE2CAMEL:
            case SNAKEHYPHEN2CAMEL:
            case UPPERCASE:
            case LOWERCASE:
                break;
            default:
                throw new ConfigException("There's no action related to " + ConfigName.CONVERT_FROM_TO + " `" + convertFromTo + "`");
        }

        reverseRenames = new HashMap<>();
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    /*
        Converts field name. If structure field noop, blacklist rule cannot be applied
     */
    private String convertField(String fieldName, boolean override) {
        if (!override) { // if structure blacklist, noop, whitelist doesn't apply
            if (isNoop(fieldName)) { // if noop send fieldname
                return fieldName;
            }
            if (isBlacklist(fieldName)) { // if blacklisted
                return null;
            }
        }

        String fieldName2Use = convertCase(fieldName); // Use converted field name
        if (log.isDebugEnabled()) {log.debug("Converted " + fieldName + " to " + fieldName2Use);}
        return fieldName2Use;
    }

    private boolean isBlacklist(String fieldName) {
        return !(!blacklist.contains(fieldName) && (whitelist.isEmpty() || whitelist.contains(fieldName)));
    }

    private boolean isNoop(String fieldName) {
        return !noop.isEmpty() && noop.contains(fieldName);
    }

    /*
        Reverse lookup for converted field names
     */
    private String reverseConverted(String fieldName) {
        final String mapping = reverseRenames.get(fieldName);
        return mapping == null ? fieldName : mapping;
    }

    /*
        Convert case based on conversion rule
     */
    private String convertCase(String value) {
        String retValue = value;
        switch (CaseType.getEnum(convertFromTo)) {
            case CAMEL2SNAKEUNDERSCORE:
                retValue = value != null ? CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, value) : null;
                break;
            case CAMEL2SNAKEHYPHEN:
                retValue = value != null ? CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_HYPHEN, value) : null;
                break;
            case SNAKEUNDERSCORE2CAMEL:
                retValue = value != null ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, value) : null;
                break;
            case SNAKEHYPHEN2CAMEL:
                retValue = value != null ? CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, value) : null;
                break;
            case UPPERCASE:
                retValue = value != null ? value.toUpperCase() : null;
                break;
            case LOWERCASE:
                retValue = value != null ? value.toLowerCase() : null;
                break;
        }
        return retValue;
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    /*
        Create record without schema applying conversion rule
     */
    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final Map<String, Object> updatedValue = applySchemaless(value);
        return newRecord(record, null, updatedValue);
    }

    /*
        Create new schema less object based on conversion rules
     */
    private Map<String, Object> applySchemaless(Map<String, Object> originalValue) {
        final Map<String, Object> updatedValue = new HashMap<>();

        originalValue.forEach( (k, v) -> {
            boolean structField = v instanceof Map || v instanceof List;

            String fieldName2Use = convertField(k, structField);
            if (structField) {
                if (v instanceof Map) {
                    final Map v1 = applySchemaless((Map) v);
                    updatedValue.put(fieldName2Use, v1);
                } else {
                    List valueList = new ArrayList();
                    ((List) v).forEach( e -> {
                        final Object object = applySchemaless((Map)e);
                        valueList.add(object);
                    });
                    updatedValue.put(fieldName2Use, valueList);
                }
            } else {
                if (fieldName2Use != null) {
                    updatedValue.put(fieldName2Use, v);
                }
            }
        });
        return updatedValue;
    }

    /*
        Create new record based on new schema and conversion rules
     */
    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        final String hashCode = String.valueOf(value.schema().hashCode());
        // System.out.println("Schema " + hashCode);

        Schema updatedSchema = schemaUpdateCache.get(hashCode);
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(hashCode, updatedSchema);
        }

        final Struct updatedValue = applyWithSchema(updatedSchema, value);
        return newRecord(record, updatedSchema, updatedValue);
    }

    /*
        Create updated value based on the converted schema
     */
    private Struct applyWithSchema (Schema schema, Struct originalValue) {
        final Struct value = new Struct(schema);
        final StringBuilder valuePresent = new StringBuilder("");

        schema.fields().forEach( field -> {
            String fieldName = field.name();
            String originalFieldName = reverseConverted(fieldName);

            if (field.schema().type() == Schema.Type.STRUCT) {
                final Struct v1 = applyWithSchema(field.schema(), originalValue.getStruct(originalFieldName));
                if (v1 != null) {
                    valuePresent.append("1");
                    value.put(field.name(), v1);
                }
            } else if (field.schema().type() == Schema.Type.ARRAY) {
                List originalList = originalValue.getArray(originalFieldName);
                List valueList = new ArrayList();
                originalList.forEach(originalElement -> {
                    final Struct v1Value = applyWithSchema(field.schema().valueSchema(), (Struct)originalElement);
                    if (v1Value != null) {
                        valuePresent.append("1");
                        valueList.add(v1Value);
                    }
                });
                value.put(field.name(), valueList);
                valuePresent.append("1");
            } else {
                if (originalValue != null) { // optional field and its children
                    final Object fieldValue = originalValue.get(originalFieldName);
                    if (fieldValue != null) {
                        value.put(field.name(), fieldValue);
                        valuePresent.append("1");
                    }
                }
            }
        });
        if (valuePresent.length() < 1) {
            return null;
        }

        return value;
    }

    /*
        Create a new schema applying the conversion rules
     */
    private Schema makeUpdatedSchema(Schema schema) {
        List<Field> fields;

        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        if (schema.type() == Schema.Type.ARRAY) {
            fields = schema.valueSchema().fields();
        } else {
            fields = schema.fields();
        }

        fields.forEach( field -> {
            final String fieldName = field.name();
            boolean structField = field.schema().type() == Schema.Type.STRUCT || field.schema().type() == Schema.Type.ARRAY;

            String fieldName2Use = convertField(fieldName, structField);
            if (field.schema().type() == Schema.Type.STRUCT || field.schema().type() == Schema.Type.ARRAY) {  // Recurse on struct field
                assert fieldName2Use != null;
                Schema innerSchema = makeUpdatedSchema(field.schema());
                builder.field(fieldName2Use, innerSchema).optional();
                reverseRenames.put(fieldName2Use, fieldName);
            } else {
                if (fieldName2Use != null) {
                    builder.field(fieldName2Use, field.schema());
                    reverseRenames.put(fieldName2Use, fieldName);
                }
            }
        });

        SchemaBuilder builder1 = builder;
        if (schema.type() == Schema.Type.ARRAY) {
            Schema schema1 = builder1.optional().build();
            builder1 = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.array(schema1));
        }
        return builder1.optional().build();
    }
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    /**
     * Use this for converting record Key fields
     *
     * @param <R>
     */
    public static class Key<R extends ConnectRecord<R>> extends ConvertCase<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    /**
     * Use this for converting record Value fields
     *
     * @param <R>
     */
    public static class Value<R extends ConnectRecord<R>> extends ConvertCase<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
