/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kafka.protobuf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.trino.plugin.kafka.schema.confluent.KafkaWithConfluentSchemaRegistryQueryRunner;
import io.trino.spi.type.SqlTimestamp;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.ENUM;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.STRING;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static io.trino.decoder.protobuf.ProtobufRowDecoderFactory.DEFAULT_MESSAGE;
import static io.trino.decoder.protobuf.ProtobufUtils.getFileDescriptor;
import static io.trino.decoder.protobuf.ProtobufUtils.getProtoFile;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static java.lang.Math.PI;
import static java.lang.Math.floorDiv;
import static java.lang.Math.multiplyExact;
import static java.lang.StrictMath.floorMod;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestKafkaProtobufWithSchemaRegistryMinimalFunctionality
        extends AbstractTestQueryFramework
{
    private static final String RECORD_NAME = "schema";
    private static final int MESSAGE_COUNT = 100;

    private TestingKafka testingKafka;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(TestingKafka.createWithSchemaRegistry());
        return KafkaWithConfluentSchemaRegistryQueryRunner.builder(testingKafka).build();
    }

    @Test
    public void testBasicTopic()
            throws Exception
    {
        String topic = "topic-basic-MixedCase";
        assertTopic(
                topic,
                format("SELECT col_1, col_2 FROM %s", toDoubleQuoted(topic)),
                format("SELECT col_1, col_2, col_3 FROM %s", toDoubleQuoted(topic)),
                false,
                producerProperties());
    }

    @Test
    public void testTopicWithKeySubject()
            throws Exception
    {
        String topic = "topic-Key-Subject";
        assertTopic(
                topic,
                format("SELECT key, col_1, col_2 FROM %s", toDoubleQuoted(topic)),
                format("SELECT key, col_1, col_2, col_3 FROM %s", toDoubleQuoted(topic)),
                true,
                producerProperties());
    }

    @Test
    public void testTopicWithRecordNameStrategy()
            throws Exception
    {
        String topic = "topic-Record-Name-Strategy";
        assertTopic(
                topic,
                format("SELECT key, col_1, col_2 FROM \"%1$s&value-subject=%2$s\"", topic, RECORD_NAME),
                format("SELECT key, col_1, col_2, col_3 FROM \"%1$s&value-subject=%2$s\"", topic, RECORD_NAME),
                true,
                ImmutableMap.<String, String>builder()
                        .putAll(producerProperties())
                        .put(VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName())
                        .buildOrThrow());
    }

    @Test
    public void testTopicWithTopicRecordNameStrategy()
            throws Exception
    {
        String topic = "topic-Topic-Record-Name-Strategy";
        assertTopic(
                topic,
                format("SELECT key, col_1, col_2 FROM \"%1$s&value-subject=%1$s-%2$s\"", topic, RECORD_NAME),
                format("SELECT key, col_1, col_2, col_3 FROM \"%1$s&value-subject=%1$s-%2$s\"", topic, RECORD_NAME),
                true,
                ImmutableMap.<String, String>builder()
                        .putAll(producerProperties())
                        .put(VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
                        .buildOrThrow());
    }

    @Test
    public void testBasicTopicForInsert()
            throws Exception
    {
        String topic = "topic-basic-inserts";
        assertTopic(
                topic,
                format("SELECT col_1, col_2 FROM %s", toDoubleQuoted(topic)),
                format("SELECT col_1, col_2, col_3 FROM %s", toDoubleQuoted(topic)),
                false,
                producerProperties());
        assertQueryFails(
                format("INSERT INTO %s (col_1, col_2, col_3) VALUES ('Trino', 14, 1.4)", toDoubleQuoted(topic)),
                "Insert is not supported for schema registry based tables");
    }

    @Test
    public void testUnsupportedNestedDataTypes()
            throws Exception
    {
        String topic = "topic-unsupported-nested";
        assertNotExists(topic);

        UnsupportedNestedTypes.schema message = UnsupportedNestedTypes.schema.newBuilder()
                .setNestedValueOne(UnsupportedNestedTypes.NestedValue.newBuilder().setStringValue("Value1").build())
                .build();

        ImmutableList.Builder<ProducerRecord<DynamicMessage, UnsupportedNestedTypes.schema>> producerRecordBuilder = ImmutableList.builder();
        producerRecordBuilder.add(new ProducerRecord<>(topic, createKeySchema(0, getKeySchema()), message));
        List<ProducerRecord<DynamicMessage, UnsupportedNestedTypes.schema>> messages = producerRecordBuilder.build();
        testingKafka.sendMessages(
                messages.stream(),
                ImmutableMap.of(
                        SCHEMA_REGISTRY_URL_CONFIG, testingKafka.getSchemaRegistryConnectString(),
                        KEY_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName(),
                        VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName()));

        waitUntilTableExists(topic);
        assertQueryFails("SELECT * FROM " + toDoubleQuoted(topic),
                "Cannot parse registry schema for nested object with the same object reference: " +
                        "io.trino.protobuf.NestedValue > " +
                        "io.trino.protobuf.NestedStruct > " +
                        "io.trino.protobuf.NestedStruct.FieldsEntry > " +
                        "io.trino.protobuf.NestedValue");
    }

    @Test
    public void testBuildInProtobufValueType()
            throws Exception
    {
        String topic = "topic-struct-value";
        assertNotExists(topic);

        StructValueType.schema nullValue = StructValueType.schema.newBuilder()
                .setStructValueOne(Value.newBuilder().setNullValueValue(0).build())
                .build();
        StructValueType.schema numberValue = StructValueType.schema.newBuilder()
                .setStructValueOne(Value.newBuilder().setNumberValue(42.42).build())
                .build();
        StructValueType.schema stringValue = StructValueType.schema.newBuilder()
                .setStructValueOne(Value.newBuilder().setStringValue("value string").build())
                .build();
        StructValueType.schema boolValue = StructValueType.schema.newBuilder()
                .setStructValueOne(Value.newBuilder().setBoolValue(false).build())
                .build();

        ImmutableList.Builder<ProducerRecord<DynamicMessage, StructValueType.schema>> producerRecordBuilder = ImmutableList.builder();
        producerRecordBuilder.add(new ProducerRecord<>(topic, createKeySchema(1, getKeySchema()), nullValue));
        producerRecordBuilder.add(new ProducerRecord<>(topic, createKeySchema(2, getKeySchema()), numberValue));
        producerRecordBuilder.add(new ProducerRecord<>(topic, createKeySchema(3, getKeySchema()), stringValue));
        producerRecordBuilder.add(new ProducerRecord<>(topic, createKeySchema(4, getKeySchema()), boolValue));

        List<ProducerRecord<DynamicMessage, StructValueType.schema>> messages = producerRecordBuilder.build();
        testingKafka.sendMessages(
                messages.stream(),
                ImmutableMap.of(
                        SCHEMA_REGISTRY_URL_CONFIG, testingKafka.getSchemaRegistryConnectString(),
                        KEY_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName(),
                        VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName()));

        waitUntilTableExists(topic);
        assertQuery(format("SELECT struct_value_one FROM %s", toDoubleQuoted(topic)),
                "VALUES " +
                        "('null')," +
                        "('42.42')," +
                        "('\"value string\"')," +
                        "('false')");
    }

    @Test
    public void testStructuralDataTypes()
            throws Exception
    {
        String topic = "topic-structural";
        assertNotExists(topic);

        Descriptor descriptor = getDescriptor("structural_datatypes.proto");

        Timestamp timestamp = getTimestamp(sqlTimestampOf(3, LocalDateTime.parse("2020-12-12T15:35:45.923")));
        DynamicMessage message = buildDynamicMessage(
                descriptor,
                ImmutableMap.<String, Object>builder()
                        .put("list", ImmutableList.of("Search"))
                        .put("map", ImmutableList.of(buildDynamicMessage(
                                descriptor.findFieldByName("map").getMessageType(),
                                ImmutableMap.of("key", "Key1", "value", "Value1"))))
                        .put("row", ImmutableMap.<String, Object>builder()
                                .put("string_column", "Trino")
                                .put("integer_column", 1)
                                .put("long_column", 493857959588286460L)
                                .put("double_column", PI)
                                .put("float_column", 3.14f)
                                .put("boolean_column", true)
                                .put("number_column", descriptor.findEnumTypeByName("Number").findValueByName("ONE"))
                                .put("timestamp_column", timestamp)
                                .put("bytes_column", "Trino".getBytes(UTF_8))
                                .buildOrThrow())
                        .buildOrThrow());

        ImmutableList.Builder<ProducerRecord<DynamicMessage, DynamicMessage>> producerRecordBuilder = ImmutableList.builder();
        producerRecordBuilder.add(new ProducerRecord<>(topic, createKeySchema(0, getKeySchema()), message));
        List<ProducerRecord<DynamicMessage, DynamicMessage>> messages = producerRecordBuilder.build();
        assertThatThrownBy(() -> {
            testingKafka.sendMessages(
                    messages.stream(),
                    ImmutableMap.of(
                            SCHEMA_REGISTRY_URL_CONFIG, testingKafka.getSchemaRegistryConnectString(),
                            KEY_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName(),
                            VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName()));
        }).isInstanceOf(NullPointerException.class)
                .hasMessage("Cannot invoke \"com.squareup.wire.schema.internal.parser.ProtoFileElement.getImports()\" because \"protoFileElement\" is null");
    }

    private DynamicMessage buildDynamicMessage(Descriptor descriptor, Map<String, Object> data)
    {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            FieldDescriptor fieldDescriptor = descriptor.findFieldByName(entry.getKey());
            if (entry.getValue() instanceof Map<?, ?>) {
                builder.setField(fieldDescriptor, buildDynamicMessage(fieldDescriptor.getMessageType(), (Map<String, Object>) entry.getValue()));
            }
            else {
                builder.setField(fieldDescriptor, entry.getValue());
            }
        }

        return builder.build();
    }

    protected static Timestamp getTimestamp(SqlTimestamp sqlTimestamp)
    {
        return Timestamp.newBuilder()
                .setSeconds(floorDiv(sqlTimestamp.getEpochMicros(), MICROSECONDS_PER_SECOND))
                .setNanos(floorMod(sqlTimestamp.getEpochMicros(), MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND)
                .build();
    }

    private Map<String, String> producerProperties()
    {
        return ImmutableMap.of(
                SCHEMA_REGISTRY_URL_CONFIG, testingKafka.getSchemaRegistryConnectString(),
                KEY_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName());
    }

    private void assertTopic(String topicName, String initialQuery, String evolvedQuery, boolean isKeyIncluded, Map<String, String> producerConfig)
            throws Exception
    {
        testingKafka.createTopic(topicName);

        assertNotExists(topicName);

        List<ProducerRecord<DynamicMessage, DynamicMessage>> messages = createMessages(topicName, MESSAGE_COUNT, true, getInitialSchema(), getKeySchema());
        testingKafka.sendMessages(messages.stream(), producerConfig);

        waitUntilTableExists(topicName);
        assertCount(topicName, MESSAGE_COUNT);

        assertQuery(initialQuery, getExpectedValues(messages, getInitialSchema(), isKeyIncluded));

        List<ProducerRecord<DynamicMessage, DynamicMessage>> newMessages = createMessages(topicName, MESSAGE_COUNT, false, getEvolvedSchema(), getKeySchema());
        testingKafka.sendMessages(newMessages.stream(), producerConfig);

        List<ProducerRecord<DynamicMessage, DynamicMessage>> allMessages = ImmutableList.<ProducerRecord<DynamicMessage, DynamicMessage>>builder()
                .addAll(messages)
                .addAll(newMessages)
                .build();
        assertCount(topicName, allMessages.size());
        assertQuery(evolvedQuery, getExpectedValues(allMessages, getEvolvedSchema(), isKeyIncluded));
    }

    private static String getExpectedValues(List<ProducerRecord<DynamicMessage, DynamicMessage>> messages, Descriptor descriptor, boolean isKeyIncluded)
    {
        StringBuilder valuesBuilder = new StringBuilder("VALUES ");
        ImmutableList.Builder<String> rowsBuilder = ImmutableList.builder();
        for (ProducerRecord<DynamicMessage, DynamicMessage> message : messages) {
            ImmutableList.Builder<String> columnsBuilder = ImmutableList.builder();

            if (isKeyIncluded) {
                addExpectedColumns(message.key().getDescriptorForType(), message.key(), columnsBuilder);
            }

            addExpectedColumns(descriptor, message.value(), columnsBuilder);

            rowsBuilder.add(format("(%s)", String.join(", ", columnsBuilder.build())));
        }
        valuesBuilder.append(String.join(", ", rowsBuilder.build()));
        return valuesBuilder.toString();
    }

    private static void addExpectedColumns(Descriptor descriptor, DynamicMessage message, ImmutableList.Builder<String> columnsBuilder)
    {
        for (FieldDescriptor field : descriptor.getFields()) {
            FieldDescriptor fieldDescriptor = message.getDescriptorForType().findFieldByName(field.getName());
            if (fieldDescriptor == null) {
                columnsBuilder.add("null");
                continue;
            }
            Object value = message.getField(message.getDescriptorForType().findFieldByName(field.getName()));
            if (field.getJavaType() == STRING || field.getJavaType() == ENUM) {
                columnsBuilder.add(toSingleQuoted(value));
            }
            else {
                columnsBuilder.add(String.valueOf(value));
            }
        }
    }

    private void assertNotExists(String tableName)
    {
        if (schemaExists()) {
            assertQueryReturnsEmptyResult(format("SHOW TABLES LIKE '%s'", tableName));
        }
    }

    private void waitUntilTableExists(String tableName)
    {
        Failsafe.with(
                        new RetryPolicy<>()
                                .withMaxAttempts(10)
                                .withDelay(Duration.ofMillis(100)))
                .run(() -> assertTrue(schemaExists()));
        Failsafe.with(
                        new RetryPolicy<>()
                                .withMaxAttempts(10)
                                .withDelay(Duration.ofMillis(100)))
                .run(() -> assertTrue(tableExists(tableName)));
    }

    private boolean schemaExists()
    {
        return computeActual(format("SHOW SCHEMAS FROM %s LIKE '%s'", getSession().getCatalog().get(), getSession().getSchema().get())).getRowCount() == 1;
    }

    private boolean tableExists(String tableName)
    {
        return computeActual(format("SHOW TABLES LIKE '%s'", tableName.toLowerCase(ENGLISH))).getRowCount() == 1;
    }

    private void assertCount(String tableName, int count)
    {
        assertQuery(format("SELECT count(*) FROM %s", toDoubleQuoted(tableName)), format("VALUES (%s)", count));
    }

    private static Descriptor getInitialSchema()
            throws Exception
    {
        return getDescriptor("initial_schema.proto");
    }

    private static Descriptor getEvolvedSchema()
            throws Exception
    {
        return getDescriptor("evolved_schema.proto");
    }

    private static Descriptor getKeySchema()
            throws Exception
    {
        return getDescriptor("key_schema.proto");
    }

    public static Descriptor getDescriptor(String fileName)
            throws Exception
    {
        return getFileDescriptor(getProtoFile("protobuf/" + fileName)).findMessageTypeByName(DEFAULT_MESSAGE);
    }

    private static String toDoubleQuoted(String tableName)
    {
        return format("\"%s\"", tableName);
    }

    private static String toSingleQuoted(Object value)
    {
        requireNonNull(value, "value is null");
        return format("'%s'", value);
    }

    private static List<ProducerRecord<DynamicMessage, DynamicMessage>> createMessages(String topicName, int messageCount, boolean useInitialSchema, Descriptor descriptor, Descriptor keyDescriptor)
    {
        ImmutableList.Builder<ProducerRecord<DynamicMessage, DynamicMessage>> producerRecordBuilder = ImmutableList.builder();
        if (useInitialSchema) {
            for (long key = 0; key < messageCount; key++) {
                producerRecordBuilder.add(new ProducerRecord<>(topicName, createKeySchema(key, keyDescriptor), createRecordWithInitialSchema(key, descriptor)));
            }
        }
        else {
            for (long key = 0; key < messageCount; key++) {
                producerRecordBuilder.add(new ProducerRecord<>(topicName, createKeySchema(key, keyDescriptor), createRecordWithEvolvedSchema(key, descriptor)));
            }
        }
        return producerRecordBuilder.build();
    }

    private static DynamicMessage createKeySchema(long key, Descriptor descriptor)
    {
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("key"), key)
                .build();
    }

    private static DynamicMessage createRecordWithInitialSchema(long key, Descriptor descriptor)
    {
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("col_1"), format("string-%s", key))
                .setField(descriptor.findFieldByName("col_2"), multiplyExact(key, 100))
                .build();
    }

    private static DynamicMessage createRecordWithEvolvedSchema(long key, Descriptor descriptor)
    {
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("col_1"), format("string-%s", key))
                .setField(descriptor.findFieldByName("col_2"), multiplyExact(key, 100))
                .setField(descriptor.findFieldByName("col_3"), (key + 10.1D) / 10.0D)
                .build();
    }
}
