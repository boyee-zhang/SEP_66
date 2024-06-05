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
package io.trino.plugin.bigquery.type;

import com.google.cloud.bigquery.StandardSQLTypeName;

import static io.trino.plugin.bigquery.type.TypeInfoUtils.getStandardSqlTypeNameFromTypeName;
import static java.util.Objects.requireNonNull;

public sealed class PrimitiveTypeInfo
        extends TypeInfo
        permits BigDecimalTypeInfo, DecimalTypeInfo
{
    private final String typeName;
    private final StandardSQLTypeName standardSqlTypeName;

    PrimitiveTypeInfo(String typeName)
    {
        this.typeName = requireNonNull(typeName, "typeName is null");
        this.standardSqlTypeName = getStandardSqlTypeNameFromTypeName(typeName);
    }

    public StandardSQLTypeName getStandardSqlTypeName()
    {
        return standardSqlTypeName;
    }

    @Override
    public String toString()
    {
        return typeName;
    }
}
