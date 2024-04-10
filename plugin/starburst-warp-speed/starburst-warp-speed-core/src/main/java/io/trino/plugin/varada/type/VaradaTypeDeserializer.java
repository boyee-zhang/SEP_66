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
package io.trino.plugin.varada.type;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

@Singleton
public class VaradaTypeDeserializer
        extends FromStringDeserializer<Type>
{
    private final TypeManager typeManager;

    @Inject
    public VaradaTypeDeserializer(TypeManager typeManager)
    {
        super(Type.class);
        this.typeManager = requireNonNull(typeManager);
    }

    @Override
    protected Type _deserialize(String value, DeserializationContext context)
    {
        Type type = typeManager.fromSqlType(value);
        requireNonNull(type, "Unknown type " + value);
        return type;
    }
}