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
package io.trino.plugin.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Json description to parse a row on a Kafka topic. A row contains a message and an optional key. See the documentation for the exact JSON syntax.
 */
public class KafkaTopicDescription
{
    private final String tableName;
    private final String topicName;
    private final Optional<String> schemaName;
    private final Optional<KafkaTopicFieldGroup> key;
    private final Optional<KafkaTopicFieldGroup> message;
    private final Optional<KafkaColumnHandle> keyColumn;

    @JsonCreator
    public KafkaTopicDescription(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("schemaName") Optional<String> schemaName,
            @JsonProperty("topicName") String topicName,
            @JsonProperty("key") Optional<KafkaTopicFieldGroup> key,
            @JsonProperty("message") Optional<KafkaTopicFieldGroup> message,
            @JsonProperty("keyColumn") Optional<KafkaColumnHandle> keyColumn)
    {
        checkArgument(!isNullOrEmpty(tableName), "tableName is null or is empty");
        this.tableName = tableName;
        this.topicName = requireNonNull(topicName, "topicName is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.key = requireNonNull(key, "key is null");
        this.message = requireNonNull(message, "message is null");
        this.keyColumn = requireNonNull(keyColumn, "keyColumn is null");
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getTopicName()
    {
        return topicName;
    }

    @JsonProperty
    public Optional<String> getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public Optional<KafkaTopicFieldGroup> getKey()
    {
        return key;
    }

    @JsonProperty
    public Optional<KafkaTopicFieldGroup> getMessage()
    {
        return message;
    }

    @JsonProperty
    public Optional<KafkaColumnHandle> getKeyColumn()
    {
        return keyColumn;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, topicName, schemaName, key, message, keyColumn);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        KafkaTopicDescription other = (KafkaTopicDescription) obj;
        return Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.topicName, other.topicName) &&
                Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.key, other.key) &&
                Objects.equals(this.message, other.message) &&
                Objects.equals(this.keyColumn, other.keyColumn);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("topicName", topicName)
                .add("schemaName", schemaName)
                .add("key", key)
                .add("message", message)
                .add("keyColumn", keyColumn)
                .toString();
    }
}
